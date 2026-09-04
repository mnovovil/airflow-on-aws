"""Roll the per-raster STAC items up into a collection, and backfill the ones missing.

``gdalinfo_notify`` writes one item per raster as it processes it, and stops there.
This DAG owns everything that has to consider the catalogue as a whole:

* backfilling items for rasters that were processed before there was a catalogue, or
  whose ``publish_stac`` task failed,
* rewriting ``collection.json`` — whose extent is the union of every item's, and
  whose link list names every one of them,
* rewriting the root ``catalog.json``.

Split out of the event-driven DAG rather than bolted onto it because the collection
is a document that grows with every raster ever processed. Rewriting it on the upload
path would put a read-modify-write of unbounded size between an upload and its email,
and would make ``max_active_runs=1`` load-bearing for a third reason.

The cost of the split is staleness: an item written a minute ago is on S3 but not yet
listed in the collection. ``@daily`` closes that on its own, and a manual trigger
closes it now.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta

import boto3
import pendulum
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError

from common import stac

# The same string move_file archives to. Imported rather than redeclared: the DAG,
# the IAM policy and the trigger Lambda already have to agree on it and
# tests/test_archive_wiring.py enforces that, so a fourth copy would be a fourth
# thing to drift. Importing the module binds the constant, not its DAG object, so
# this does not register gdalinfo_notify twice.
from gdalinfo_notify import ARCHIVE_PREFIX

LOG = logging.getLogger(__name__)

DAG_ID = "stac_publish"
CONFIG_VARIABLE = "ice_config"


def get_config() -> dict:
    """See the note on the same function in gdalinfo_notify: read inside tasks only."""
    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def _iter_keys(s3, bucket: str, prefix: str, suffix: str = ""):
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for entry in page.get("Contents", []):
            if entry["Key"].endswith(suffix):
                yield entry["Key"]


def _read_json(s3, bucket: str, key: str) -> dict:
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def _locate_raster(s3, bucket: str, key: str) -> str | None:
    """Where the raster named by a report lives now, or None if it is gone.

    Reports outlive their rasters on purpose: ``delete_file`` destroys an unusable
    raster *after* its report has been written, and that report is the record of why.
    So a report with no object behind it is a normal thing to find here, and it must
    not become a catalogue entry pointing at nothing.

    Probed with head_object rather than a list because the scheduler's policy grants
    ``s3:GetObject`` over the source bucket but no ``s3:ListBucket``. That also
    decides the error handling: without ListBucket, S3 answers a missing key with 403
    rather than 404, so both mean the same thing here.
    """
    for candidate in (f"{ARCHIVE_PREFIX}{key}", key):
        try:
            s3.head_object(Bucket=bucket, Key=candidate)
            return candidate
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "403", "NoSuchKey", "AccessDenied"):
                raise
    return None


# --------------------------------------------------------------------------- tasks


def backfill_items(**context) -> int:
    """Write an item for every report that does not have one yet.

    Existing items are left untouched rather than regenerated. That keeps a rerun
    cheap, and means this task cannot rewrite history if the mapping in common/stac.py
    changes — deliberate: to republish everything, delete the items prefix first and
    let this rebuild it.
    """
    config = get_config()
    artifacts, source = config["artifacts_bucket"], config["source_bucket"]
    report_prefix = config.get("report_prefix", "reports")

    s3 = boto3.client("s3")

    existing = {
        key.rsplit("/", 1)[-1].removesuffix(".json")
        for key in _iter_keys(s3, artifacts, f"{stac.CATALOG_PREFIX}/{stac.COLLECTION_ID}/items/", ".json")
    }
    LOG.info("catalogue holds %d item(s)", len(existing))

    written = 0
    for summary_key in _iter_keys(s3, artifacts, f"{report_prefix.rstrip('/')}/", "/summary.json"):
        report_key = summary_key.removesuffix("/summary.json")
        raster_key = report_key[len(report_prefix.rstrip("/")) + 1 :]

        if stac.item_id(raster_key, ARCHIVE_PREFIX) in existing:
            continue

        located = _locate_raster(s3, source, raster_key)
        if located is None:
            LOG.info("no raster behind %s — it was deleted as unusable; skipping", report_key)
            continue

        try:
            item = stac.build_item(
                _read_json(s3, artifacts, f"{report_key}/gdalinfo.json"),
                _read_json(s3, artifacts, summary_key),
                source_bucket=source,
                raster_key=located,
                artifacts_bucket=artifacts,
                report_key=report_key,
                archive_prefix=ARCHIVE_PREFIX,
            )
        except stac.StacError as exc:
            # One unmappable report must not stop the backfill: the other few hundred
            # are still publishable, and this one is visible in the log.
            LOG.warning("skipping %s — %s", report_key, exc)
            continue

        s3.put_object(
            Bucket=artifacts,
            Key=stac.item_key(item["id"]),
            Body=json.dumps(item, indent=2, default=str).encode(),
            ContentType="application/geo+json",
        )
        written += 1

    LOG.info("backfilled %d item(s)", written)
    return written


def write_collection(**context) -> str:
    """Rewrite collection.json and catalog.json from whatever items are on S3.

    Reads every item rather than tracking the extent incrementally. That is N GETs per
    run, which is the wrong shape at a hundred thousand items and entirely fine at the
    scale this pipeline produces — and it means the collection is derived from the
    items rather than from a running total that can drift away from them.
    """
    config = get_config()
    artifacts = config["artifacts_bucket"]

    s3 = boto3.client("s3")
    prefix = f"{stac.CATALOG_PREFIX}/{stac.COLLECTION_ID}/items/"
    items = [_read_json(s3, artifacts, key) for key in _iter_keys(s3, artifacts, prefix, ".json")]

    for key, document, content_type in (
        (stac.collection_key(), stac.build_collection(items), "application/json"),
        (stac.catalog_key(), stac.build_catalog(), "application/json"),
    ):
        s3.put_object(
            Bucket=artifacts,
            Key=key,
            Body=json.dumps(document, indent=2, default=str).encode(),
            ContentType=content_type,
        )

    LOG.info("collection rebuilt over %d item(s): s3://%s/%s", len(items), artifacts, stac.catalog_key())
    return f"s3://{artifacts}/{stac.catalog_key()}"


# ----------------------------------------------------------------------------- dag

with DAG(
    dag_id=DAG_ID,
    description="Rebuild the STAC collection over the items gdalinfo_notify writes",
    # Daily, and cheap when there is nothing to do: the backfill is a list of two
    # prefixes and no writes. Trigger it by hand after an upload if you want the
    # collection to list it before tomorrow.
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # collection.json is one object rewritten from a full read of the items prefix.
    # Two concurrent runs would interleave that read-modify-write, and the loser's
    # items would be missing from the links list until the next run.
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["stac", "s3", "elevation"],
) as dag:
    t_backfill = PythonOperator(task_id="backfill_items", python_callable=backfill_items)
    t_collection = PythonOperator(task_id="write_collection", python_callable=write_collection)

    t_backfill >> t_collection
