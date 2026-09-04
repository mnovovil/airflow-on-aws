"""Behaviour of the DAG's task callables, exercised without Airflow or AWS.

The one worth the most here is the SSM command construction: object keys are chosen
by whoever writes to the source bucket and end up interpolated into a shell running
as root on the worker.
"""

from __future__ import annotations

import io
import json
import shlex

import pytest
from airflow.exceptions import AirflowException

import gdalinfo_notify as dag_module
from conftest import GDALINFO, SUMMARY

CONFIG = {
    "instance_id": "i-0123456789abcdef0",
    "artifacts_bucket": "ice-artifacts-123456789012",
    "report_prefix": "reports",
    "email_to": "someone@example.com",
    "region": "eu-north-1",
    "ssm_log_group": "/aws/ssm/ice-gdal-worker",
    "object_timeout": 60,
}
IMAGE = "123456789012.dkr.ecr.eu-north-1.amazonaws.com/ice/gdal-report:sha256tag"


class _TI:
    def __init__(self, values: dict):
        self._values = values

    def xcom_pull(self, task_ids: str):
        return self._values[task_ids]


class _DagRun:
    def __init__(self, conf):
        self.conf = conf


# ------------------------------------------------------------------- parse_event


def test_parse_event_extracts_the_object():
    event = dag_module.parse_event(dag_run=_DagRun({"bucket": "example-dem", "key": "a/b.tif", "size": 12}))
    assert event == {"bucket": "example-dem", "key": "a/b.tif", "size": 12, "etag": None}


def test_parse_event_rejects_a_conf_without_an_object():
    with pytest.raises(AirflowException, match="must contain"):
        dag_module.parse_event(dag_run=_DagRun({}))


def test_parse_event_rejects_a_non_raster_key():
    with pytest.raises(AirflowException, match="not a recognised raster"):
        dag_module.parse_event(dag_run=_DagRun({"bucket": "b", "key": "notes.txt"}))


def test_parse_event_accepts_uppercase_extensions():
    event = dag_module.parse_event(dag_run=_DagRun({"bucket": "b", "key": "A.TIF"}))
    assert event["key"] == "A.TIF"


# --------------------------------------------------------------- wait_for_object


class _Waiter:
    """Stands in for the s3 object_exists waiter, recording how it was called."""

    def __init__(self):
        self.calls: list[dict] = []
        self.error: Exception | None = None

    def wait(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error


@pytest.fixture
def waiter(monkeypatch):
    stub = _Waiter()
    monkeypatch.setattr(dag_module, "get_config", lambda: CONFIG)
    monkeypatch.setattr(
        dag_module.boto3,
        "client",
        lambda *a, **k: type("_S3", (), {"get_waiter": staticmethod(lambda name: stub)})(),
    )
    return stub


def _wait(key: str, bucket: str = "example-dem"):
    return dag_module.wait_for_object(ti=_TI({"parse_event": {"bucket": bucket, "key": key}}))


def test_wait_for_object_waits_on_the_object_from_parse_event(waiter):
    assert _wait("scenes/a.tif") == "scenes/a.tif"
    assert waiter.calls[0]["Bucket"] == "example-dem"
    assert waiter.calls[0]["Key"] == "scenes/a.tif"


def test_wait_for_object_derives_its_attempts_from_the_configured_timeout(waiter):
    """The waiter takes attempts, the config is in seconds. 60s at 5s apart is 12."""
    _wait("scenes/a.tif")
    assert waiter.calls[0]["WaiterConfig"] == {"Delay": 5, "MaxAttempts": 12}


def test_wait_for_object_polls_at_least_once_on_a_sub_delay_timeout(monkeypatch, waiter):
    """A timeout shorter than the poll interval floors to one attempt.

    Integer division would otherwise give MaxAttempts=0, which botocore treats as
    'never poll' — the object would be declared missing without ever being looked
    for, and every run would fail.
    """
    monkeypatch.setattr(dag_module, "get_config", lambda: {**CONFIG, "object_timeout": 2})
    _wait("scenes/a.tif")
    assert waiter.calls[0]["WaiterConfig"]["MaxAttempts"] == 1


def test_wait_for_object_fails_the_task_when_the_object_never_appears(waiter):
    """The failure has to propagate: it is what triggers the retries and, in the
    end, the failure email. A swallowed timeout would start the worker anyway and
    fail later, with the instance already running."""
    waiter.error = RuntimeError("Waiter ObjectExists failed")
    with pytest.raises(RuntimeError, match="ObjectExists"):
        _wait("scenes/missing.tif")


# ------------------------------------------------------------------ run_gdalinfo


@pytest.fixture
def captured_commands(monkeypatch):
    """Run run_gdalinfo against stubs and hand back the shell it would have run."""
    sent: dict = {}

    monkeypatch.setattr(dag_module, "get_config", lambda: CONFIG)
    monkeypatch.setattr(dag_module.Variable, "get", staticmethod(lambda *a, **k: IMAGE))
    monkeypatch.setattr(
        dag_module.ec2_ssm,
        "run_shell",
        lambda instance_id, commands, **kwargs: sent.update(
            instance_id=instance_id, commands=commands, kwargs=kwargs
        ),
    )
    return sent


def _run(key: str, captured):
    ti = _TI({"parse_event": {"bucket": "example-dem", "key": key}})
    prefix = dag_module.run_gdalinfo(ti=ti)
    return prefix, captured["commands"]


def test_run_gdalinfo_builds_the_expected_docker_invocation(captured_commands):
    prefix, commands = _run("scenes/a.tif", captured_commands)

    assert prefix == "reports/scenes/a.tif"
    assert commands[0] == "set -euo pipefail"
    assert "docker login" in commands[1]
    assert commands[2] == f"docker pull {IMAGE}"

    run = commands[3]
    assert run.startswith("docker run --rm")
    assert "--bucket example-dem" in run
    assert "--key scenes/a.tif" in run
    assert f"--out-bucket {CONFIG['artifacts_bucket']}" in run
    assert "--out-prefix reports" in run


def test_run_gdalinfo_targets_the_configured_worker(captured_commands):
    _run("scenes/a.tif", captured_commands)
    assert captured_commands["instance_id"] == CONFIG["instance_id"]
    assert captured_commands["kwargs"]["log_group"] == CONFIG["ssm_log_group"]


@pytest.mark.parametrize(
    "key",
    [
        "scenes/a b.tif",
        "scenes/a';rm -rf /;'.tif",
        "scenes/$(touch /tmp/pwned).tif",
        'scenes/a"b.tif',
        "scenes/a`id`.tif",
    ],
)
def test_a_hostile_object_key_cannot_break_out_of_the_command(key, captured_commands):
    """Anyone who can put an object in example-dem chooses this string, and it is
    interpolated into a root shell on the worker. It must stay one argument."""
    _run(key, captured_commands)
    run = captured_commands["commands"][3]

    # shlex parses the line the way the worker's shell would: the key must come back
    # out whole, as a single argument, with no extra words introduced.
    words = shlex.split(run)
    assert words[words.index("--key") + 1] == key
    assert words[0] == "docker"
    assert "rm" not in words


# ----------------------------------------------------------------- validate_report


class _ReportS3:
    """Serves one summary.json, the way validate_report reads it."""

    def __init__(self, summary: dict):
        self._body = json.dumps(summary).encode()

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self._body)}


GOOD_SUMMARY = {
    "band_count": 1,
    "crs": {"name": "WGS 84 / UTM zone 33N", "epsg": 32633},
    "bands": [{"index": 1, "min": 0.0, "max": 2400.0}],
}


def _validate(monkeypatch, **overrides):
    summary = {**GOOD_SUMMARY, **overrides}
    monkeypatch.setattr(dag_module, "get_config", lambda: CONFIG)
    monkeypatch.setattr(dag_module.boto3, "client", lambda *a, **k: _ReportS3(summary))
    ti = _TI(
        {
            "parse_event": {"bucket": "example-dem", "key": "scenes/a.tif"},
            "run_gdalinfo": "reports/scenes/a.tif",
        }
    )
    return dag_module.validate_report(ti=ti)


def test_validate_report_sends_a_usable_raster_to_the_email(monkeypatch):
    assert _validate(monkeypatch) == dag_module.EMAIL_TASK


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"band_count": 0}, id="no bands"),
        pytest.param({"crs": {"name": None, "epsg": None}}, id="no epsg"),
        pytest.param({"bands": [{"index": 1, "stats_error": "no valid pixels"}]}, id="all bands unusable"),
    ],
)
def test_validate_report_sends_an_unusable_raster_to_the_delete(monkeypatch, overrides):
    assert _validate(monkeypatch, **overrides) == dag_module.DELETE_TASK


def test_validate_report_keeps_a_raster_where_only_some_bands_lack_stats(monkeypatch):
    """One all-nodata band among several is normal — gdal_report records the error per
    band rather than failing. Only a raster where *every* band failed is unusable."""
    bands = [{"index": 1, "stats_error": "no valid pixels"}, {"index": 2, "min": 0.0, "max": 1.0}]
    assert _validate(monkeypatch, band_count=2, bands=bands) == dag_module.EMAIL_TASK


# ---------------------------------------------------------------------- move_file


class _S3:
    """Records what move_file asked S3 to do, in the order it asked."""

    def __init__(self):
        self.calls: list[tuple] = []

    def copy(self, source, bucket, key):
        self.calls.append(("copy", source, bucket, key))

    def delete_object(self, Bucket, Key):
        self.calls.append(("delete", Bucket, Key))


@pytest.fixture
def s3(monkeypatch):
    client = _S3()
    monkeypatch.setattr(dag_module.boto3, "client", lambda *a, **k: client)
    return client


def _move(key: str, bucket: str = "example-dem"):
    return dag_module.move_file(ti=_TI({"parse_event": {"bucket": bucket, "key": key}}))


def test_move_file_copies_under_the_archive_prefix_then_deletes_the_original(s3):
    assert _move("scenes/a.tif") == "sent/scenes/a.tif"
    assert s3.calls == [
        ("copy", {"Bucket": "example-dem", "Key": "scenes/a.tif"}, "example-dem", "sent/scenes/a.tif"),
        ("delete", "example-dem", "scenes/a.tif"),
    ]


def test_move_file_deletes_only_after_the_copy_returned(s3):
    """A copy that raises must leave the source object where it is.

    Deleting first, or ignoring the copy's failure, turns a transient S3 error into
    a raster that exists nowhere — the one outcome this task must not produce.
    """

    def explode(*_a, **_k):
        raise RuntimeError("copy failed")

    s3.copy = explode
    with pytest.raises(RuntimeError, match="copy failed"):
        _move("scenes/a.tif")

    assert s3.calls == []


def test_move_file_leaves_an_already_archived_raster_alone(s3):
    """The archive copy fires another ObjectCreated event on the watched bucket.

    The trigger Lambda is what drops it; this guard is the backstop for a run
    started by hand against a key already under the prefix. Either way the object
    must not be copied to sent/sent/... and must not be deleted.
    """
    assert _move("sent/scenes/a.tif") == "sent/scenes/a.tif"
    assert s3.calls == []


def test_move_file_archives_a_key_at_the_bucket_root(s3):
    assert _move("a.tif") == "sent/a.tif"


# -------------------------------------------------------------------- delete_file


def _delete(key: str, bucket: str = "example-dem"):
    return dag_module.delete_file(ti=_TI({"parse_event": {"bucket": bucket, "key": key}}))


def test_delete_file_removes_the_unusable_raster(s3):
    assert _delete("scenes/a.tif") == "scenes/a.tif"
    assert s3.calls == [("delete", "example-dem", "scenes/a.tif")]


def test_delete_file_refuses_to_destroy_an_archived_raster(s3):
    """A key under sent/ passed validation once and was reported on. A re-run that now
    judges it unusable must not be what deletes the only copy — the source bucket has
    no versioning, so this delete is final."""
    with pytest.raises(AirflowException, match="refusing to delete"):
        _delete("sent/scenes/a.tif")

    assert s3.calls == []


# -------------------------------------------------------------------- publish_stac


class _CatalogueS3:
    """Serves the two reports and records what was written back."""

    def __init__(self, gdalinfo: dict, summary: dict):
        self._bodies = {"gdalinfo.json": gdalinfo, "summary.json": summary}
        self.puts: list[dict] = []

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(json.dumps(self._bodies[Key.rsplit("/", 1)[-1]]).encode())}

    def put_object(self, **kwargs):
        self.puts.append(kwargs)


@pytest.fixture
def catalogue(monkeypatch):
    client = _CatalogueS3(GDALINFO, SUMMARY)
    monkeypatch.setattr(dag_module, "get_config", lambda: CONFIG)
    monkeypatch.setattr(dag_module.boto3, "client", lambda *a, **k: client)
    return client


def _publish():
    return dag_module.publish_stac(
        ti=_TI(
            {
                "parse_event": {"bucket": "example-dem", "key": "scenes/n61e010.tif"},
                "run_gdalinfo": "reports/scenes/n61e010.tif",
                "move_file": "sent/scenes/n61e010.tif",
            }
        )
    )


def test_publish_stac_writes_the_item_where_the_collection_expects_it(catalogue):
    assert _publish() == "stac/elevation/items/scenes_n61e010.json"

    (put,) = catalogue.puts
    assert put["Bucket"] == CONFIG["artifacts_bucket"]
    assert put["Key"] == "stac/elevation/items/scenes_n61e010.json"
    assert put["ContentType"] == "application/geo+json"


def test_publish_stac_points_the_asset_at_the_archived_key(catalogue):
    """move_file's return value, not parse_event's key.

    The two differ by the archive prefix, and the raster is only at the second of them
    by the time this runs.
    """
    _publish()

    item = json.loads(catalogue.puts[0]["Body"])
    assert item["assets"]["data"]["href"] == "s3://example-dem/sent/scenes/n61e010.tif"
    assert item["assets"]["summary"]["href"].startswith(f"s3://{CONFIG['artifacts_bucket']}/reports/")


def test_publish_stac_is_idempotent_across_the_archive_move(catalogue):
    """A hand-triggered re-run sees an already-archived key in parse_event, so
    move_file returns it unchanged. The item id must not change with it — otherwise one
    raster becomes two items, one of them stale."""
    first = _publish()
    second = dag_module.publish_stac(
        ti=_TI(
            {
                "parse_event": {"bucket": "example-dem", "key": "sent/scenes/n61e010.tif"},
                "run_gdalinfo": "reports/sent/scenes/n61e010.tif",
                "move_file": "sent/scenes/n61e010.tif",
            }
        )
    )
    assert first == second
