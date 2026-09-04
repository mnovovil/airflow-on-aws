"""The Lambda is the only thing between an upload and silence.

Its two load-bearing behaviours are the suffix filter (S3's own filter is
case-sensitive, so the handler is what makes ``.TIF`` and ``.tif`` behave alike) and
the deterministic run id, which is what stops S3's at-least-once delivery from
emailing the same report twice.

The handler reads its configuration at import time, so it is loaded once here under
a known environment — and by path, since ``lambda`` is a keyword and the directory
is therefore not importable.
"""

from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
import urllib.error
import urllib.request

import pytest

from conftest import LAMBDA_DIR

ENV = {
    # Trailing slash on purpose — the handler has to normalise it, or every request
    # goes to a double-slashed path.
    "AIRFLOW_API_URL": "http://10.20.0.9:8080/",
    "AIRFLOW_USERNAME": "admin",
    "AIRFLOW_PASSWORD": "s3cret",
}


@pytest.fixture(scope="module")
def handler_module():
    previous = {k: os.environ.get(k) for k in ENV}
    os.environ.update(ENV)
    try:
        spec = importlib.util.spec_from_file_location("trigger_dag_handler", LAMBDA_DIR / "handler.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class _Response:
    """Minimal stand-in for what urlopen returns."""

    def __init__(self, payload: dict):
        self._payload = json.dumps(payload).encode()

    def read(self):
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


def _http_error(code: int, body: str = "") -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "http://10.20.0.9:8080/api/v1/dags/gdalinfo_notify/dagRuns",
        code,
        "error",
        {},
        io.BytesIO(body.encode()),
    )


def _record(key: str, etag: str = "abc123", bucket: str = "example-dem") -> dict:
    return {
        "eventTime": "2026-07-25T10:00:00.000Z",
        "s3": {
            "bucket": {"name": bucket},
            "object": {"key": key, "size": 4465595, "eTag": f'"{etag}"'},
        },
    }


def test_run_id_is_stable_for_the_same_object(handler_module):
    first = handler_module._run_id(_record("a/b.tif"), "a/b.tif")
    second = handler_module._run_id(_record("a/b.tif"), "a/b.tif")
    assert first == second


def test_run_id_changes_when_the_content_changes(handler_module):
    """A re-upload of modified bytes gets a new ETag and must produce a fresh run."""
    original = handler_module._run_id(_record("a/b.tif", etag="aaa"), "a/b.tif")
    modified = handler_module._run_id(_record("a/b.tif", etag="bbb"), "a/b.tif")
    assert original != modified


def test_run_id_is_url_and_cli_safe(handler_module):
    run_id = handler_module._run_id(_record("scenes/a b&c;.tif"), "scenes/a b&c;.tif")
    assert all(c.isalnum() or c in "-_." for c in run_id)
    assert len(run_id) <= 250


def test_conf_carries_what_the_dag_needs(handler_module):
    conf = handler_module._conf(_record("a/b.tif"), "example-dem", "a/b.tif")
    assert conf["bucket"] == "example-dem"
    assert conf["key"] == "a/b.tif"
    assert conf["size"] == 4465595
    assert conf["etag"] == "abc123"  # quotes stripped


def test_non_raster_keys_are_skipped_without_triggering(handler_module, monkeypatch):
    def explode(*_args, **_kwargs):
        raise AssertionError("must not trigger a DAG run for a non-raster key")

    monkeypatch.setattr(handler_module, "_trigger", explode)

    result = handler_module.handler({"Records": [_record("notes/readme.txt")]}, None)
    assert result == {"triggered": [], "skipped": ["notes/readme.txt"]}


def test_the_archive_copy_does_not_trigger_a_run(handler_module, monkeypatch):
    """move_file's copy to sent/ fires an ObjectCreated event on the watched bucket.

    It carries a raster suffix, so the bucket's filter cannot exclude it and the
    function is called. Triggering on it would start the worker and email a second
    report for a file already reported on.
    """

    def explode(*_args, **_kwargs):
        raise AssertionError("must not trigger a DAG run for an already-archived key")

    monkeypatch.setattr(handler_module, "_trigger", explode)

    key = "sent/scenes/a.TIF"
    assert handler_module.handler({"Records": [_record(key)]}, None) == {"triggered": [], "skipped": [key]}


def test_a_key_merely_containing_the_prefix_still_triggers(handler_module, monkeypatch):
    """The guard is a prefix test, not a substring one — sent/ appearing deeper in a
    key belongs to whoever uploaded it and is a raster like any other."""
    seen = []
    monkeypatch.setattr(handler_module, "_trigger", lambda conf, run_id: seen.append(conf))

    result = handler_module.handler({"Records": [_record("scenes/sent/a.tif")]}, None)
    assert result["triggered"] == ["scenes/sent/a.tif"]
    assert seen[0]["key"] == "scenes/sent/a.tif"


def test_uppercase_extensions_are_accepted(handler_module, monkeypatch):
    """S3 needs a separate .TIF filter rule; the handler must not then reject it."""
    seen = []
    monkeypatch.setattr(handler_module, "_trigger", lambda conf, run_id: seen.append(conf))

    result = handler_module.handler({"Records": [_record("scenes/A.TIF")]}, None)
    assert result["triggered"] == ["scenes/A.TIF"]
    assert seen[0]["key"] == "scenes/A.TIF"


def test_url_encoded_keys_are_decoded(handler_module, monkeypatch):
    """S3 URL-encodes keys in notifications and turns spaces into '+'. Passing the
    encoded form through would make GDAL open a key that does not exist."""
    seen = []
    monkeypatch.setattr(handler_module, "_trigger", lambda conf, run_id: seen.append(conf))

    handler_module.handler({"Records": [_record("scenes/a+b%3Ac.tif")]}, None)
    assert seen[0]["key"] == "scenes/a b:c.tif"


def test_api_request_is_shaped_correctly(handler_module, monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout=None):
        captured["url"] = request.full_url
        captured["method"] = request.method
        captured["headers"] = {k.lower(): v for k, v in request.headers.items()}
        captured["body"] = json.loads(request.data)
        return _Response({"state": "queued"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    handler_module._trigger({"bucket": "b", "key": "a.tif"}, "run-1")

    # The trailing slash in AIRFLOW_API_URL must not survive into the path.
    assert captured["url"] == "http://10.20.0.9:8080/api/v1/dags/gdalinfo_notify/dagRuns"
    assert captured["method"] == "POST"
    assert captured["headers"]["content-type"] == "application/json"
    # base64("admin:s3cret")
    assert captured["headers"]["authorization"] == "Basic YWRtaW46czNjcmV0"
    assert captured["body"] == {"dag_run_id": "run-1", "conf": {"bucket": "b", "key": "a.tif"}}


def test_409_is_treated_as_already_triggered(handler_module, monkeypatch):
    """A duplicate S3 delivery reaches a run id that already exists. Airflow says 409,
    and that is the idempotency guard working — not a failure to propagate."""

    def conflict(request, timeout=None):
        raise _http_error(409, '{"detail": "already exists"}')

    monkeypatch.setattr(urllib.request, "urlopen", conflict)
    handler_module._trigger({"bucket": "b", "key": "a.tif"}, "run-1")


@pytest.mark.parametrize("code", [401, 404, 500])
def test_other_errors_are_raised(handler_module, monkeypatch, code):
    """Anything else has to fail loudly: a silently swallowed 401 is an upload that
    produces no email and no trace of why."""

    def failure(request, timeout=None):
        raise _http_error(code, "nope")

    monkeypatch.setattr(urllib.request, "urlopen", failure)
    with pytest.raises(RuntimeError, match=str(code)):
        handler_module._trigger({"bucket": "b", "key": "a.tif"}, "run-1")
