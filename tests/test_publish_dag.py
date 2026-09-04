"""The rebuild DAG's two tasks, against a fake S3.

The interesting one is the backfill's decision about what *not* to publish. Reports
outlive their rasters by design — ``delete_file`` destroys an unusable raster after its
report has been written, and that report is the record of why — so a report with no
object behind it is a normal thing to find, and turning it into a catalogue entry
would point the catalogue at nothing.
"""

from __future__ import annotations

import io
import json

import pytest
from botocore.exceptions import ClientError

import publish as dag_module
from conftest import GDALINFO, SUMMARY

CONFIG = {
    "artifacts_bucket": "ice-artifacts-123456789012",
    "source_bucket": "example-dem",
    "report_prefix": "reports",
}


class _S3:
    """Enough of the client for both tasks: a keyed object store plus a paginator.

    ``missing_is_forbidden`` reproduces the behaviour the real bucket has for this
    role. Without ``s3:ListBucket``, S3 answers a HEAD for a key that is not there with
    403 rather than 404 — so the backfill has to read both as "gone", and a fake that
    only ever raises 404 would let a regression through.
    """

    def __init__(self, objects: dict[str, dict | bytes], missing_is_forbidden: bool = True):
        self.objects = dict(objects)
        self.missing_is_forbidden = missing_is_forbidden
        self.puts: list[dict] = []

    def _missing(self, operation: str) -> ClientError:
        code = "403" if self.missing_is_forbidden else "404"
        return ClientError({"Error": {"Code": code, "Message": "nope"}}, operation)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        store = self.objects

        class _Paginator:
            @staticmethod
            def paginate(Bucket, Prefix):
                scoped = f"{Bucket}:{Prefix}"
                contents = [{"Key": key.split(":", 1)[1]} for key in sorted(store) if key.startswith(scoped)]
                # Two pages, so a task that only reads the first one fails here.
                yield {"Contents": contents[:1]}
                yield {"Contents": contents[1:]}

        return _Paginator()

    def get_object(self, Bucket, Key):
        try:
            payload = self.objects[f"{Bucket}:{Key}"]
        except KeyError:
            raise self._missing("GetObject") from None
        return {"Body": io.BytesIO(json.dumps(payload).encode())}

    def head_object(self, Bucket, Key):
        if f"{Bucket}:{Key}" not in self.objects:
            raise self._missing("HeadObject")
        return {"ContentLength": 1}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)
        self.objects[f"{kwargs['Bucket']}:{kwargs['Key']}"] = json.loads(kwargs["Body"])


ARTIFACTS = CONFIG["artifacts_bucket"]
SOURCE = CONFIG["source_bucket"]


def _report(key: str) -> dict[str, dict]:
    return {
        f"{ARTIFACTS}:reports/{key}/gdalinfo.json": GDALINFO,
        f"{ARTIFACTS}:reports/{key}/summary.json": SUMMARY,
    }


@pytest.fixture
def s3(monkeypatch):
    def _build(objects, **kwargs):
        client = _S3(objects, **kwargs)
        monkeypatch.setattr(dag_module, "get_config", lambda: CONFIG)
        monkeypatch.setattr(dag_module.boto3, "client", lambda *a, **k: client)
        return client

    return _build


# ------------------------------------------------------------------ backfill_items


def test_backfill_writes_an_item_for_a_report_that_has_none(s3):
    client = s3({**_report("scenes/a.tif"), f"{SOURCE}:sent/scenes/a.tif": b""})

    assert dag_module.backfill_items() == 1
    (put,) = client.puts
    assert put["Key"] == "stac/elevation/items/scenes_a.json"
    assert json.loads(put["Body"])["assets"]["data"]["href"] == f"s3://{SOURCE}/sent/scenes/a.tif"


def test_backfill_skips_a_report_whose_raster_was_deleted(s3):
    """The unusable-raster case. The report is the record of why the object went away;
    it is not a catalogue entry."""
    client = s3(_report("scenes/gone.tif"))

    assert dag_module.backfill_items() == 0
    assert client.puts == []


def test_backfill_finds_a_raster_that_was_never_archived(s3):
    """A run that failed after the report and before move_file leaves the raster at its
    original key. The item should point there rather than be skipped."""
    client = s3({**_report("scenes/a.tif"), f"{SOURCE}:scenes/a.tif": b""})

    assert dag_module.backfill_items() == 1
    assert json.loads(client.puts[0]["Body"])["assets"]["data"]["href"] == f"s3://{SOURCE}/scenes/a.tif"


def test_backfill_leaves_an_item_that_already_exists(s3):
    client = s3(
        {
            **_report("scenes/a.tif"),
            f"{SOURCE}:sent/scenes/a.tif": b"",
            f"{ARTIFACTS}:stac/elevation/items/scenes_a.json": {"id": "scenes_a"},
        }
    )

    assert dag_module.backfill_items() == 0
    assert client.puts == []


def test_backfill_is_not_stopped_by_one_unmappable_report(s3):
    """A report GDAL could not place in WGS84 cannot become an item. The other reports
    still can, and the run should end with them published rather than with a traceback."""
    unmappable = {key: value for key, value in GDALINFO.items() if key != "wgs84Extent"}
    client = s3(
        {
            **_report("scenes/a.tif"),
            f"{ARTIFACTS}:reports/scenes/bad.tif/gdalinfo.json": unmappable,
            f"{ARTIFACTS}:reports/scenes/bad.tif/summary.json": SUMMARY,
            f"{SOURCE}:sent/scenes/a.tif": b"",
            f"{SOURCE}:sent/scenes/bad.tif": b"",
        }
    )

    assert dag_module.backfill_items() == 1
    assert [put["Key"] for put in client.puts] == ["stac/elevation/items/scenes_a.json"]


@pytest.mark.parametrize("missing_is_forbidden", [True, False])
def test_backfill_reads_403_and_404_the_same_way(s3, missing_is_forbidden):
    """Without s3:ListBucket the real bucket answers a missing key with 403. Treating
    only 404 as "gone" would turn every deleted raster into a failed run."""
    client = s3(_report("scenes/gone.tif"), missing_is_forbidden=missing_is_forbidden)

    assert dag_module.backfill_items() == 0
    assert client.puts == []


# ----------------------------------------------------------------- write_collection


def test_write_collection_rebuilds_from_the_items_on_s3(s3):
    client = s3({**_report("scenes/a.tif"), f"{SOURCE}:sent/scenes/a.tif": b""})
    dag_module.backfill_items()
    client.puts.clear()

    assert dag_module.write_collection() == f"s3://{ARTIFACTS}/stac/catalog.json"

    written = {put["Key"]: json.loads(put["Body"]) for put in client.puts}
    assert set(written) == {"stac/elevation/collection.json", "stac/catalog.json"}
    items = [link for link in written["stac/elevation/collection.json"]["links"] if link["rel"] == "item"]
    assert [link["href"] for link in items] == ["./items/scenes_a.json"]


def test_write_collection_writes_an_empty_catalogue_rather_than_nothing(s3):
    """The first run against a bucket with no items. A collection that is absent is
    indistinguishable from one that failed to write; an empty one is not."""
    client = s3({})

    dag_module.write_collection()

    collection = next(
        json.loads(put["Body"]) for put in client.puts if put["Key"] == "stac/elevation/collection.json"
    )
    assert collection["extent"]["temporal"]["interval"] == [[None, None]]
