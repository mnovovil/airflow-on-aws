"""Turn an S3 ObjectCreated event into an Airflow DAG run.

S3 cannot call Airflow directly — its only notification targets are SNS, SQS,
Lambda and EventBridge — so this sits in between. It POSTs to Airflow's own REST
API over the VPC with basic auth.

EventBridge API Destinations support basic auth natively and could replace this
function outright, but they call from AWS's own network and so need Airflow on a
public HTTPS endpoint — a domain and a certificate. That is a worse trade than this
file for a pipeline of this size.

Nothing here needs a third-party package, or even boto3, which is why this deploys
as a bare source file with nothing vendored.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request

LOG = logging.getLogger()
LOG.setLevel(logging.INFO)

DAG_ID = os.environ.get("DAG_ID", "gdalinfo_notify")
ALLOWED_SUFFIXES = tuple(
    s.strip().lower()
    for s in os.environ.get("ALLOWED_SUFFIXES", ".tif,.tiff,.img,.vrt,.jp2").split(",")
    if s.strip()
)

# The DAG's move_file copies each raster under this prefix once its report has been
# emailed, and that copy fires an ObjectCreated event of its own. S3's notification
# filters cannot express "not this prefix" — they are prefix/suffix matches, not
# exclusions — so dropping it has to happen here. Without this, every archived file
# costs a second worker start and a duplicate email before the DAG's own guard
# recognises the key as already archived. Must match ARCHIVE_PREFIX in the DAG.
ARCHIVE_PREFIX = os.environ.get("ARCHIVE_PREFIX", "sent/")

# Read at import so a misconfigured function fails on its first cold start with a
# clear KeyError, rather than once per upload from inside a task.
AIRFLOW_API_URL = os.environ["AIRFLOW_API_URL"].rstrip("/")
AIRFLOW_USERNAME = os.environ["AIRFLOW_USERNAME"]
AIRFLOW_PASSWORD = os.environ["AIRFLOW_PASSWORD"]


def _run_id(record: dict, key: str) -> str:
    """Deterministic run id so S3's at-least-once delivery cannot double-send email.

    The ETag changes whenever the object's content changes, so re-uploading a
    modified raster still produces a fresh run, while a redelivered notification
    for the same bytes collides with the existing run and is rejected.
    """
    etag = record["s3"]["object"].get("eTag", "noetag").strip('"')
    version = record["s3"]["object"].get("versionId", "null")
    digest = f"{key}-{etag}-{version}"
    # Airflow run ids are free-form but a bare key can contain characters that make
    # the CLI and URLs awkward, so keep it to a safe alphabet.
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in digest)
    return f"s3-{safe}"[:250]


def _conf(record: dict, bucket: str, key: str) -> dict:
    obj = record["s3"]["object"]
    return {
        "bucket": bucket,
        "key": key,
        "size": obj.get("size"),
        "etag": obj.get("eTag", "").strip('"'),
        "event_time": record.get("eventTime"),
    }


def _trigger(conf: dict, run_id: str) -> None:
    """POST a DAG run to Airflow's REST API."""
    credentials = base64.b64encode(f"{AIRFLOW_USERNAME}:{AIRFLOW_PASSWORD}".encode()).decode()

    request = urllib.request.Request(
        f"{AIRFLOW_API_URL}/api/v1/dags/{DAG_ID}/dagRuns",
        data=json.dumps({"dag_run_id": run_id, "conf": conf}).encode(),
        method="POST",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            state = json.loads(response.read()).get("state")
    except urllib.error.HTTPError as exc:
        # Airflow answers a repeated dag_run_id with 409, which is the deterministic
        # run id doing its job against S3's at-least-once delivery — not a failure.
        if exc.code == 409:
            LOG.info("run %s already exists — duplicate S3 delivery, nothing to do", run_id)
            return
        body = exc.read().decode(errors="replace")
        raise RuntimeError(f"Airflow API returned {exc.code}: {body[:500]}") from exc

    LOG.info("triggered %s as %s (%s)", DAG_ID, run_id, state)


def handler(event: dict, _context) -> dict:
    triggered, skipped = [], []

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        # S3 URL-encodes keys in notifications and turns spaces into '+'.
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        # Guarded on ARCHIVE_PREFIX being set: an empty value would make this skip
        # every key in the bucket and the pipeline would go silent.
        if ARCHIVE_PREFIX and key.startswith(ARCHIVE_PREFIX):
            LOG.info("skipping %s — already archived, this is move_file's own copy", key)
            skipped.append(key)
            continue

        if not key.lower().endswith(ALLOWED_SUFFIXES):
            # The bucket notification filter should already have excluded this;
            # the second check keeps the DAG honest if the filter is ever widened.
            LOG.info("skipping %s — not a recognised raster extension", key)
            skipped.append(key)
            continue

        run_id = _run_id(record, key)
        conf = _conf(record, bucket, key)
        LOG.info("triggering %s for s3://%s/%s", DAG_ID, bucket, key)

        _trigger(conf, run_id)
        triggered.append(key)

    return {"triggered": triggered, "skipped": skipped}
