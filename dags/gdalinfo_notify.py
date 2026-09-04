"""Inspect a raster that landed in S3 and email its metadata.

Triggered by the ``trigger_dag`` Lambda, which passes the object location in
``dag_run.conf``:

    {"bucket": "example-dem", "key": "path/to.tif", "size": 123, "etag": "..."}

The GDAL work happens in a container on an EC2 worker that is normally stopped —
this DAG starts it, drives it over SSM Run Command, and stops it again.

``max_active_runs=1`` is load-bearing, not a tuning knob: with a shared worker that
is started and stopped per run, two concurrent runs would let one run's
``stop_instance`` pull the host out from under the other's ``run_gdalinfo``. A burst
of uploads therefore processes one file at a time; Airflow queues the rest.

Once the report exists the run forks: a usable raster is emailed, archived and
catalogued; an unusable one is deleted unreported. Either way ``stop_instance`` runs.

The catalogue is STAC, written to the artifacts bucket as static JSON. This DAG
writes one item per raster and nothing else — the collection and the root catalogue
are rolled up by the ``stac_publish`` DAG, which keeps a read-modify-write over a
document that grows with every raster off the per-upload path.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import tempfile
from datetime import timedelta

import boto3
import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from common import ec2_ssm, stac
from common.email_render import render, render_failure, subject

LOG = logging.getLogger(__name__)

DAG_ID = "gdalinfo_notify"
CONFIG_VARIABLE = "ice_config"
IMAGE_VARIABLE = "gdal_image_uri"
ALLOWED_SUFFIXES = (".tif", ".tiff", ".img", ".vrt", ".jp2")

# Where a raster goes once it has been reported on. Must match ARCHIVE_PREFIX in the
# trigger Lambda's environment: the archive copy lands in the bucket the pipeline
# watches, and the Lambda dropping that event is what stops it looping.
ARCHIVE_PREFIX = "sent/"

# The two branches validate_report picks between. Named here because the branch
# callable returns one of them as a string: a typo is not a NameError, it is an
# AirflowException raised mid-run with the worker already started and billing.
EMAIL_TASK = "build_and_send_email"
DELETE_TASK = "delete_file"


def get_config() -> dict:
    """Runtime configuration, resolved from Secrets Manager via the Airflow secrets backend.

    Read inside tasks rather than at module scope — a module-level ``Variable.get``
    hits the secrets backend on every DAG-file parse, which the scheduler does
    constantly.
    """
    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


# --------------------------------------------------------------------------- tasks


def parse_event(**context) -> dict:
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    bucket, key = conf.get("bucket"), conf.get("key")

    if not bucket or not key:
        raise AirflowException(f"dag_run.conf must contain 'bucket' and 'key'. Got: {json.dumps(conf)[:500]}")
    if not key.lower().endswith(ALLOWED_SUFFIXES):
        raise AirflowException(f"s3://{bucket}/{key} is not a recognised raster extension")

    LOG.info("processing s3://%s/%s (%s bytes)", bucket, key, conf.get("size", "unknown"))
    return {"bucket": bucket, "key": key, "size": conf.get("size"), "etag": conf.get("etag")}


def wait_for_object(**context) -> str:
    """Block until the raster is actually readable in S3.

    Runs before ``start_instance`` on purpose: a key that never shows up fails here
    in seconds instead of inside ``run_gdalinfo`` on a worker that has already been
    started and billed for.
    """
    config = get_config()
    event = context["ti"].xcom_pull(task_ids="parse_event")
    bucket, key = event["bucket"], event["key"]

    delay = 5
    waiter = boto3.client("s3").get_waiter("object_exists")
    waiter.wait(
        Bucket=bucket,
        Key=key,
        WaiterConfig={"Delay": delay, "MaxAttempts": max(1, int(config.get("object_timeout", 60)) // delay)},
    )

    LOG.info("s3://%s/%s is available", bucket, key)
    return key


def start_instance(**context) -> str:
    config = get_config()
    return ec2_ssm.start_and_wait(config["instance_id"], timeout=int(config.get("start_timeout", 300)))


def run_gdalinfo(**context) -> str:
    config = get_config()
    event = context["ti"].xcom_pull(task_ids="parse_event")
    image = Variable.get(IMAGE_VARIABLE)
    region = config.get("region", "eu-north-1")
    registry = image.split("/", 1)[0]

    # Every interpolated value ends up in a root shell on the worker, and object keys
    # are attacker-controlled in the sense that anyone who can write to the source
    # bucket chooses them. shlex.quote is what keeps a key with a quote or a `;` in it
    # from becoming a command.
    login = (
        f"aws ecr get-login-password --region {shlex.quote(region)}"
        f" | docker login --username AWS --password-stdin {shlex.quote(registry)}"
    )

    commands = [
        "set -euo pipefail",
        login,
        f"docker pull {shlex.quote(image)}",
        # --rm so a failed run leaves no container behind on a host that will be
        # stopped and started for months.
        " ".join(
            shlex.quote(part)
            for part in [
                "docker",
                "run",
                "--rm",
                "-e",
                f"AWS_REGION={region}",
                image,
                "--bucket",
                event["bucket"],
                "--key",
                event["key"],
                "--out-bucket",
                config["artifacts_bucket"],
                "--out-prefix",
                config.get("report_prefix", "reports"),
            ]
        ),
    ]

    ec2_ssm.run_shell(
        config["instance_id"],
        commands,
        comment=f"gdalinfo {event['key']}",
        timeout=int(config.get("gdal_timeout", 1800)),
        log_group=config.get("ssm_log_group"),
    )

    return f"{config.get('report_prefix', 'reports')}/{event['key']}"


def validate_report(**context) -> str:
    """Route the run: report a usable raster, delete an unusable one.

    The distinction this task draws is between broken and useless. A report that
    cannot be read is *broken* — it raises, the failure email goes out, and nothing
    is deleted. A report that reads fine but describes a raster nobody can use is
    *useless*, which is a routing decision rather than an error, so the run stays
    green and takes the delete branch.

    Returns a task_id, which is what makes this a branch: the task not named here is
    the one Airflow marks skipped.
    """
    config = get_config()
    event = context["ti"].xcom_pull(task_ids="parse_event")
    prefix = context["ti"].xcom_pull(task_ids="run_gdalinfo")

    s3 = boto3.client("s3")
    summary = json.loads(
        s3.get_object(Bucket=config["artifacts_bucket"], Key=f"{prefix}/summary.json")["Body"].read()
    )

    bands = summary.get("bands") or []
    reasons = []
    if not summary.get("band_count"):
        reasons.append("no raster bands")
    # Worth knowing before this fires in anger: describe_crs in gdal_report.py leaves
    # epsg None for a valid *custom* projection too, because AutoIdentifyEPSG only
    # sets an authority when it is confident. Testing crs["name"] instead is the
    # looser reading — "no CRS at all" rather than "no EPSG code".
    if (summary.get("crs") or {}).get("epsg") is None:
        reasons.append("no EPSG code on the CRS")
    if bands and all("stats_error" in band for band in bands):
        reasons.append(f"all {len(bands)} bands failed statistics")

    if reasons:
        LOG.warning(
            "s3://%s/%s is unusable (%s) — deleting it unreported",
            event["bucket"],
            event["key"],
            "; ".join(reasons),
        )
        return DELETE_TASK

    LOG.info("report for %s passed validation", event["key"])
    return EMAIL_TASK


def build_and_send_email(**context) -> None:
    """Fetch the report from S3, render it, and send it with the raw JSON attached.

    Downloading and sending live in one task on purpose. ``EmailOperator``'s
    ``files=`` argument reads from local disk, so splitting this across two tasks
    would depend on both of them landing on a worker with the same filesystem.
    """
    from airflow.providers.smtp.hooks.smtp import SmtpHook

    config = get_config()
    event = context["ti"].xcom_pull(task_ids="parse_event")
    prefix = context["ti"].xcom_pull(task_ids="run_gdalinfo")
    bucket = config["artifacts_bucket"]

    s3 = boto3.client("s3")
    summary = json.loads(s3.get_object(Bucket=bucket, Key=f"{prefix}/summary.json")["Body"].read())

    # The worker container is long-lived and processes every run, so the attachment
    # has to be cleaned up rather than left in /tmp until the container is recycled.
    with tempfile.TemporaryDirectory(prefix="gdalinfo-") as workdir:
        attachment = os.path.join(workdir, f"{os.path.basename(event['key'])}.gdalinfo.json")
        s3.download_file(bucket, f"{prefix}/gdalinfo.json", attachment)

        # SmtpHook only opens its connection in __enter__, and send_email_smtp
        # refuses to run without it ("The 'smtp_client' should be initialized
        # before!"). Constructing the hook and calling straight through raises every
        # time, so the context manager is required rather than stylistic.
        with SmtpHook() as smtp:
            smtp.send_email_smtp(
                to=config["email_to"],
                subject=subject(summary),
                html_content=render(summary, report_location=f"s3://{bucket}/{prefix}/"),
                files=[attachment],
            )
    LOG.info("emailed report for %s to %s", event["key"], config["email_to"])


def move_file(**context) -> str:
    """Move the source raster under ``ARCHIVE_PREFIX`` once it has been reported on.

    It stays in the source bucket rather than moving to the artifacts one: an
    archived raster is still an input, and the artifacts bucket holds outputs.

    The copy fires a second ``ObjectCreated`` event on a bucket whose notification
    filter is suffix-only, so the trigger Lambda is what drops it. This guard only
    catches a run triggered by hand against an already-archived key.
    """
    event = context["ti"].xcom_pull(task_ids="parse_event")
    bucket, key = event["bucket"], event["key"]

    if key.startswith(ARCHIVE_PREFIX):
        LOG.info("s3://%s/%s is already archived, nothing to move", bucket, key)
        return key

    dest_key = f"{ARCHIVE_PREFIX}{key}"

    s3 = boto3.client("s3")
    s3.copy({"Bucket": bucket, "Key": key}, bucket, dest_key)

    # Delete only after the copy has returned: a failed copy leaves the source in
    # place, which is the recoverable direction to fail in.
    s3.delete_object(Bucket=bucket, Key=key)

    LOG.info("moved s3://%s/%s to s3://%s/%s", bucket, key, bucket, dest_key)
    return dest_key


def publish_stac(**context) -> str:
    """Write one STAC item describing the raster this run reported on.

    Downstream of ``move_file`` rather than beside it, and that is the whole reason
    this task is where it is: the item's data asset points at the object, and until
    the move has returned the object is still under the watched prefix. An item
    written any earlier would reference a key that no longer exists by the time
    anyone reads it.

    Only the item. The collection that lists it is rebuilt by the ``stac_publish``
    DAG, so nothing on this path reads a document whose size grows with the number of
    rasters ever processed.

    Idempotent: the id is derived from the key, so a re-run overwrites the item it
    wrote last time rather than adding a second one.
    """
    config = get_config()
    event = context["ti"].xcom_pull(task_ids="parse_event")
    report_key = context["ti"].xcom_pull(task_ids="run_gdalinfo")
    raster_key = context["ti"].xcom_pull(task_ids="move_file")
    bucket = config["artifacts_bucket"]

    s3 = boto3.client("s3")
    reports = {}
    for name in ("gdalinfo.json", "summary.json"):
        reports[name] = json.loads(s3.get_object(Bucket=bucket, Key=f"{report_key}/{name}")["Body"].read())

    item = stac.build_item(
        reports["gdalinfo.json"],
        reports["summary.json"],
        source_bucket=event["bucket"],
        raster_key=raster_key,
        artifacts_bucket=bucket,
        report_key=report_key,
        archive_prefix=ARCHIVE_PREFIX,
    )

    key = stac.item_key(item["id"])
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(item, indent=2, default=str).encode(),
        ContentType="application/geo+json",
    )

    LOG.info("catalogued %s as s3://%s/%s", event["key"], bucket, key)
    return key


def delete_file(**context) -> str:
    """Delete a raster that failed validation, instead of archiving it.

    Irreversible: the source bucket is adopted rather than managed by this stack and
    has no versioning, so this destroys the only copy. What survives is the report —
    gdalinfo.json and summary.json are written before this runs, live in the
    artifacts bucket, and are the record of why the object went away.

    The delete fires ``ObjectRemoved``, which the bucket notification does not
    subscribe to, so unlike the archive copy this cannot re-trigger the DAG.
    """
    event = context["ti"].xcom_pull(task_ids="parse_event")
    bucket, key = event["bucket"], event["key"]

    # An object under the archive prefix passed validation on an earlier run and was
    # reported on. A hand-triggered re-run that now judges it unusable must not be
    # what destroys the archive: fail loudly and let a human decide.
    if key.startswith(ARCHIVE_PREFIX):
        raise AirflowException(f"refusing to delete an already-archived raster: s3://{bucket}/{key}")

    boto3.client("s3").delete_object(Bucket=bucket, Key=key)

    LOG.warning("deleted unusable raster s3://%s/%s", bucket, key)
    return key


def stop_instance(**context) -> str:
    config = get_config()
    return ec2_ssm.stop(config["instance_id"])


def notify_failure(context) -> None:
    """Email the failure so a broken run is never silent.

    Wrapped in a broad except: a callback that raises is swallowed by Airflow and
    would leave no trace at all of why the notification never arrived.
    """
    try:
        from airflow.providers.smtp.hooks.smtp import SmtpHook

        config = get_config()
        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
        task = context.get("task_instance")

        details = {
            "Object": f"s3://{conf.get('bucket', '?')}/{conf.get('key', '?')}",
            "DAG run": getattr(context.get("dag_run"), "run_id", "?"),
            "Failed task": getattr(task, "task_id", "?"),
        }
        error = str(context.get("exception") or "See the task logs for details.")

        # Context manager for the same reason as in build_and_send_email: the hook
        # has no usable connection until __enter__ runs.
        with SmtpHook() as smtp:
            smtp.send_email_smtp(
                to=config["email_to"],
                subject=f"[GDAL] FAILED — {os.path.basename(conf.get('key', 'unknown'))}",
                html_content=render_failure(details, error),
            )
    except Exception:  # noqa: BLE001
        LOG.exception("failure notification could not be sent")


# ----------------------------------------------------------------------------- dag

with DAG(
    dag_id=DAG_ID,
    description="gdalinfo a raster landing in S3 and email the metadata",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    on_failure_callback=notify_failure,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["gdal", "s3", "elevation"],
) as dag:
    t_parse = PythonOperator(task_id="parse_event", python_callable=parse_event)
    t_wait = PythonOperator(task_id="wait_for_object", python_callable=wait_for_object)
    t_start = PythonOperator(task_id="start_instance", python_callable=start_instance)
    t_gdal = PythonOperator(task_id="run_gdalinfo", python_callable=run_gdalinfo)
    t_validate = BranchPythonOperator(task_id="validate_report", python_callable=validate_report)
    t_email = PythonOperator(task_id=EMAIL_TASK, python_callable=build_and_send_email)

    # Default trigger rule on purpose. On the delete branch build_and_send_email is
    # skipped, all_success propagates that skip here, and move_file skips too — which
    # is right: the raster it would archive no longer exists.
    t_move = PythonOperator(task_id="move_file", python_callable=move_file)
    t_delete = PythonOperator(task_id=DELETE_TASK, python_callable=delete_file)

    # After move_file, never beside it: publish_stac pulls that task's return value as
    # the key its data asset points at. Skipping with the rest of the email branch is
    # the intended behaviour — a raster that was deleted for being unusable should not
    # appear in the catalogue.
    t_publish = PythonOperator(task_id="publish_stac", python_callable=publish_stac)

    # all_done, and retried harder than anything else: this task is what stands
    # between a failed run and an instance billing quietly for a month. It is also
    # the join: whichever branch was skipped counts as done, so this still runs.
    t_stop = PythonOperator(
        task_id="stop_instance",
        python_callable=stop_instance,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=4,
        retry_delay=timedelta(seconds=20),
    )

    # A branch, not a short circuit: BranchPythonOperator skips only the direct
    # downstream it did not pick, leaving everything past it to ordinary trigger
    # rules. ShortCircuitOperator's default would stamp SKIPPED on every descendant
    # instead — stop_instance included, and the worker left running.
    t_parse >> t_wait >> t_start >> t_gdal >> t_validate >> t_email >> t_move >> t_publish >> t_stop
    t_validate >> t_delete >> t_stop
