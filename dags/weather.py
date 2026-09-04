"""Fetch the day's GFS rainfall forecast, render it as a map, and email it.

Runs at 13:00 America/New_York against the 12Z GFS cycle at forecast hour 24 — the
rain expected between 08:00 today and 08:00 tomorrow, Eastern. GFS runs four times a
day (00/06/12/18Z) and the 12Z products land around 11:45 UTC + a few hours, so a 13:00
Eastern start is comfortably clear of publication with a DST-proof local wall time.

The GDAL work happens in a container on the same normally-stopped EC2 worker the
elevation pipeline uses, driven over SSM Run Command. ``max_active_runs=1`` is
load-bearing for the same reason it is in ``gdalinfo_notify``: two runs sharing one
start/stop worker means one run's ``stop_instance`` can pull the host out from under
the other's work. The two DAGs can still collide with each other — see the note on
``stop_instance`` below.

The run is a straight line, with no branch. There is no equivalent here of "the raster
is unusable": either NOMADS served a forecast or it did not, and not-served is a
failure rather than a routing decision.

Deliberately not catalogued in STAC. The existing collection describes elevation
rasters with a static footprint; a daily forecast is a different collection with a real
temporal dimension, and bolting it onto this one would leave the collection's extent
meaning nothing.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import tempfile
import time
from datetime import timedelta

import boto3
import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from common import ec2_ssm, gfs
from common.email_render import build_message, recipients, render_failure, render_weather, weather_subject

# The prefix gdalinfo_notify archives to, imported rather than redeclared for the
# reason given at its own import in publish.py: the DAG, the trigger Lambda and the
# IAM policy already have to agree on this string, and a fourth copy would be a fourth
# thing to drift. Importing the module binds the constant, not its DAG object.
from gdalinfo_notify import ARCHIVE_PREFIX

LOG = logging.getLogger(__name__)

DAG_ID = "gdal_weather"
CONFIG_VARIABLE = "ice_config"

# The same image the elevation pipeline runs, with the entrypoint overridden. One
# build, one ECR repository, one secret for CI to keep pinned.
IMAGE_VARIABLE = "gdal_image_uri"
WEATHER_SCRIPT = "/app/gfs_rain.py"

# 12Z, forecast hour 24, and therefore the 24-hour accumulation band. The three are
# not independent: f024 out of the 12Z cycle is what makes "the next 24 hours" true,
# and ACCUMULATION_HOURS is what picks the 0-24h record out of a file that also
# contains the 18-24h one. Change one and the email's own description of itself, which
# is derived from the GRIB rather than from these constants, will disagree with it.
CYCLE = "12"
FORECAST_HOUR = 24
ACCUMULATION_HOURS = 24

# Basename of the products inside the run's prefix. The archived GRIB gets the cycle
# date appended; everything else is namespaced by the prefix already.
PRODUCT_NAME = "usa_rain"

# Where the archived GRIB lands, under the prefix the trigger Lambda already ignores.
# Its own subdirectory because the rest of sent/ mirrors the source bucket's key
# layout, and these files were never in the source bucket to begin with.
ARCHIVE_SUBDIR = "gfs/"


def get_config() -> dict:
    """Runtime configuration, resolved from Secrets Manager via the Airflow secrets backend.

    Read inside tasks rather than at module scope — a module-level ``Variable.get``
    hits the secrets backend on every DAG-file parse, which the scheduler does
    constantly.

    Every key this DAG adds is read with a default, so it runs against the ice_config
    that is already deployed without a terraform apply first.
    """
    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def _cycle_date(context) -> str:
    """The UTC date of the GFS cycle this run should fetch, as YYYYMMDD.

    Taken from ``data_interval_end`` rather than ``logical_date``. For a daily schedule
    Airflow's logical date is the *start* of the interval, so the run that fires at
    13:00 on the 19th carries a logical date of the 18th — and fetching gfs.20260818
    from a run on the 19th is exactly the off-by-one-day this avoids. The interval end
    is the wall-clock moment the run was scheduled for, which is the day whose 12Z
    cycle is wanted.

    A manual trigger gets an interval ending at roughly now, which is also right. Pass
    ``{"date": "20260818"}`` in the run conf to override it for a backfill — NOMADS
    keeps about ten days.
    """
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    if conf.get("date"):
        LOG.info("cycle date overridden from dag_run.conf: %s", conf["date"])
        return str(conf["date"])

    scheduled = context.get("data_interval_end") or context["logical_date"]
    return scheduled.in_timezone("UTC").format("YYYYMMDD")


# --------------------------------------------------------------------------- tasks


def resolve_cycle(**context) -> dict:
    """Work out which forecast this run is for, and where its products will live.

    Pure computation on purpose — no AWS, no network. Everything downstream reads the
    URL and the prefix from here, so the URL that gets probed is character-for-character
    the URL that gets fetched and there is no second construction of it to drift.
    """
    config = get_config()
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}

    date = _cycle_date(context)
    cycle = str(conf.get("cycle", CYCLE))
    fhour = int(conf.get("fhour", FORECAST_HOUR))
    hours = int(conf.get("accumulation", ACCUMULATION_HOURS))

    url = gfs.build_url(date, cycle=cycle, fhour=fhour)
    valid_from, valid_to = gfs.window(date, cycle=cycle, fhour=fhour, accumulation=hours)

    # Keyed by the cycle date rather than by the run, so a re-run overwrites the
    # products it wrote last time instead of accumulating a second copy of them.
    iso_date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    prefix = f"{config.get('weather_prefix', 'reports/weather').strip('/')}/{iso_date}/{cycle}z"

    plan = {
        "date": date,
        "iso_date": iso_date,
        "cycle": cycle,
        "fhour": fhour,
        "accumulation": hours,
        "url": url,
        "prefix": prefix,
        "valid_from": valid_from.isoformat(),
        "valid_to": valid_to.isoformat(),
    }
    LOG.info("gfs.%s/%s f%03d, valid %s → %s", date, cycle, fhour, valid_from, valid_to)
    LOG.info("products will be written to %s", prefix)
    return plan


def wait_for_cycle(**context) -> str:
    """Block until NOMADS is actually serving this cycle's f024 product.

    Before ``start_instance``, and that ordering is the whole point: a cycle that is
    late, or a date NOMADS has already aged out, fails here in seconds instead of
    inside ``run_weather_report`` on a worker that has been started and billed for.

    Polls rather than failing on the first miss because "published" is a state NOMADS
    arrives at a few minutes either side of the same time each day, and the run is
    scheduled hours after that — so a miss here almost always means a genuinely late
    cycle rather than a wrong URL.
    """
    config = get_config()
    plan = context["ti"].xcom_pull(task_ids="resolve_cycle")

    delay = 30
    deadline = int(config.get("nomads_timeout", 900))
    for attempt in range(1, max(deadline // delay, 1) + 1):
        if gfs.is_published(plan["url"], timeout=int(config.get("nomads_probe_timeout", 30))):
            LOG.info("gfs.%s/%s f%03d is published", plan["date"], plan["cycle"], plan["fhour"])
            return plan["url"]
        LOG.info("attempt %d: cycle not published yet, waiting %ds", attempt, delay)
        time.sleep(delay)

    raise AirflowException(
        f"NOMADS is not serving GRIB2 for gfs.{plan['date']}/{plan['cycle']} f{plan['fhour']:03d} "
        f"after {deadline}s. Check https://www.nco.ncep.noaa.gov/status/prodstat/ for the "
        f"cycle's production status, or whether the date has aged off NOMADS' ~10 day window.\n"
        f"URL: {plan['url']}"
    )


def start_instance(**context) -> str:
    config = get_config()
    return ec2_ssm.start_and_wait(config["instance_id"], timeout=int(config.get("start_timeout", 300)))


def run_weather_report(**context) -> str:
    """Download, render and publish the forecast, in a container on the worker.

    The image is the elevation pipeline's, with ``--entrypoint python3`` overriding the
    ENTRYPOINT so the weather script runs instead of the raster one. That is the whole
    cost of sharing an image between the two.
    """
    config = get_config()
    plan = context["ti"].xcom_pull(task_ids="resolve_cycle")
    image = Variable.get(IMAGE_VARIABLE)
    region = config.get("region", "eu-north-1")
    registry = image.split("/", 1)[0]

    # Same reasoning as run_gdalinfo: everything interpolated here lands in a root
    # shell on the worker. The URL is built by this DAG rather than by a caller, but
    # the run conf can override the cycle date, so it is quoted like anything else.
    login = (
        f"aws ecr get-login-password --region {shlex.quote(region)}"
        f" | docker login --username AWS --password-stdin {shlex.quote(registry)}"
    )

    commands = [
        "set -euo pipefail",
        login,
        f"docker pull {shlex.quote(image)}",
        " ".join(
            shlex.quote(part)
            for part in [
                "docker",
                "run",
                "--rm",
                "--entrypoint",
                "python3",
                "-e",
                f"AWS_REGION={region}",
                image,
                WEATHER_SCRIPT,
                "--url",
                plan["url"],
                "--accumulation",
                str(plan["accumulation"]),
                "--name",
                PRODUCT_NAME,
                "--out-bucket",
                config["artifacts_bucket"],
                "--out-prefix",
                plan["prefix"],
            ]
        ),
    ]

    ec2_ssm.run_shell(
        config["instance_id"],
        commands,
        comment=f"gfs rain {plan['date']} {plan['cycle']}z",
        timeout=int(config.get("weather_timeout", 1200)),
        log_group=config.get("ssm_log_group"),
    )

    return plan["prefix"]


def build_and_send_email(**context) -> None:
    """Send the map inline, with the raw gdalinfo attached.

    Downloading and sending live in one task for the same reason they do in
    ``gdalinfo_notify``: the message body references a PNG on local disk, so splitting
    this in two would depend on both halves landing on a worker with the same
    filesystem.

    ``SmtpHook.send_email_smtp`` is not used — it has no way to give an attachment a
    Content-ID, which is what the inline map needs. The message is assembled in
    common.email_render and handed to the hook's own client, so the connection, the
    credentials and the From address still come from the smtp_default connection.
    """
    from airflow.providers.smtp.hooks.smtp import SmtpHook

    config = get_config()
    plan = context["ti"].xcom_pull(task_ids="resolve_cycle")
    bucket = config["artifacts_bucket"]
    prefix = plan["prefix"]

    s3 = boto3.client("s3")
    summary = json.loads(s3.get_object(Bucket=bucket, Key=f"{prefix}/summary.json")["Body"].read())

    # The worker container is long-lived and processes every run, so the map and the
    # attachment have to be cleaned up rather than left in /tmp indefinitely.
    with tempfile.TemporaryDirectory(prefix="gfs-rain-") as workdir:
        png = os.path.join(workdir, f"{PRODUCT_NAME}.png")
        report = os.path.join(workdir, f"{PRODUCT_NAME}-{plan['iso_date']}.gdalinfo.json")
        s3.download_file(bucket, f"{prefix}/{PRODUCT_NAME}.png", png)
        s3.download_file(bucket, f"{prefix}/gdalinfo.json", report)

        cid = f"rainmap.{plan['date']}"
        to = recipients(config["email_to"])

        # Context manager rather than a bare constructor: SmtpHook only opens its
        # connection in __enter__, and smtp_client is None until it has.
        with SmtpHook() as smtp:
            message = build_message(
                mail_from=smtp.from_email,
                to=to,
                subject=weather_subject(summary),
                html_content=render_weather(summary, cid, report_location=f"s3://{bucket}/{prefix}/"),
                inline_images={cid: png},
                files=[report],
            )
            smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())

    LOG.info("emailed the %s forecast to %s", plan["iso_date"], config["email_to"])


def archive_grib(**context) -> str:
    """Keep the GRIB NOMADS served, under sent/ in the source bucket.

    The scheduler does this copy rather than the worker because the worker's role can
    write to ``artifacts/reports/*`` and nowhere else, while the scheduler already has
    PutObject scoped to ``sent/*`` for the elevation pipeline's archive step. Doing it
    here needs no IAM change; doing it on the worker would widen its write surface into
    the bucket that drives the pipeline.

    Only the GRIB. The GeoTIFF and the PNG are renderings — reproducible from these
    bytes and this DAG — and the GRIB is the thing that cannot be fetched again once
    NOMADS ages the cycle out.

    After ``build_and_send_email``, so a failed copy cannot withhold a map that has
    already been rendered.
    """
    config = get_config()
    plan = context["ti"].xcom_pull(task_ids="resolve_cycle")

    artifacts = config["artifacts_bucket"]
    source_key = f"{plan['prefix']}/{PRODUCT_NAME}.grib2"
    dest_key = f"{ARCHIVE_PREFIX}{ARCHIVE_SUBDIR}{PRODUCT_NAME}_{plan['iso_date']}_{plan['cycle']}z.grib2"

    # Cross-bucket, and safe to land in the watched bucket: the trigger Lambda drops
    # every key under sent/, and .grib2 is not in the notification's suffix filters
    # either way. Idempotent — a re-run overwrites the same key.
    boto3.client("s3").copy({"Bucket": artifacts, "Key": source_key}, config["source_bucket"], dest_key)

    LOG.info("archived s3://%s/%s to s3://%s/%s", artifacts, source_key, config["source_bucket"], dest_key)
    return dest_key


def stop_instance(**context) -> str:
    config = get_config()
    return ec2_ssm.stop(config["instance_id"])


def notify_failure(context) -> None:
    """Email the failure so a broken run is never silent.

    Wrapped in a broad except: a callback that raises is swallowed by Airflow and would
    leave no trace at all of why the notification never arrived.
    """
    try:
        from airflow.providers.smtp.hooks.smtp import SmtpHook

        config = get_config()
        task = context.get("task_instance")
        plan = task.xcom_pull(task_ids="resolve_cycle") if task else None

        details = {
            "Forecast": f"gfs.{plan['date']}/{plan['cycle']} f{plan['fhour']:03d}" if plan else "unresolved",
            "URL": plan["url"] if plan else "—",
            "DAG run": getattr(context.get("dag_run"), "run_id", "?"),
            "Failed task": getattr(task, "task_id", "?"),
        }
        error = str(context.get("exception") or "See the task logs for details.")

        with SmtpHook() as smtp:
            smtp.send_email_smtp(
                to=config["email_to"],
                subject=f"[GFS] FAILED — rainfall forecast {plan['iso_date'] if plan else 'unknown'}",
                html_content=render_failure(details, error),
            )
    except Exception:  # noqa: BLE001
        LOG.exception("failure notification could not be sent")


# ----------------------------------------------------------------------------- dag

with DAG(
    dag_id=DAG_ID,
    description="Map the GFS 24-hour US rainfall forecast and email it",
    # 13:00 in the DAG's own timezone, so this stays 13:00 Eastern across a DST
    # boundary rather than drifting to noon or two o'clock the way a UTC cron would.
    schedule="0 13 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="America/New_York"),
    # A forecast is only interesting before it is verified. Catching up would email a
    # week of maps of weather that has already happened.
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    on_failure_callback=notify_failure,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["gdal", "weather", "gfs"],
) as dag:
    t_resolve = PythonOperator(task_id="resolve_cycle", python_callable=resolve_cycle)
    t_wait = PythonOperator(task_id="wait_for_cycle", python_callable=wait_for_cycle)
    t_start = PythonOperator(task_id="start_instance", python_callable=start_instance)
    t_run = PythonOperator(task_id="run_weather_report", python_callable=run_weather_report)
    t_email = PythonOperator(task_id="build_and_send_email", python_callable=build_and_send_email)
    t_archive = PythonOperator(task_id="archive_grib", python_callable=archive_grib)

    # all_done and retried harder than anything else, for the same reason as in
    # gdalinfo_notify: this task is what stands between a failed run and an instance
    # billing quietly for a month.
    #
    # Worth knowing: max_active_runs only serialises *this* DAG. An upload arriving
    # while this run holds the worker gives gdalinfo_notify a run of its own, and
    # whichever finishes first stops the shared instance under the other. The idle
    # alarm in infra/ec2.tf is the backstop for the resulting orphan, and the loser
    # fails its SSM command and retries. Once a day at 13:00 against an event-driven
    # pipeline, that overlap is rare enough to accept rather than build a lock for.
    t_stop = PythonOperator(
        task_id="stop_instance",
        python_callable=stop_instance,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=4,
        retry_delay=timedelta(seconds=20),
    )

    t_resolve >> t_wait >> t_start >> t_run >> t_email >> t_archive >> t_stop
