"""The parts of gdal_weather that are wrong in ways a green run would not reveal.

Three of them, and none surfaces as an error:

* the cycle date. Airflow's logical date for a daily schedule is the *start* of the
  interval, so reading it would fetch yesterday's run — which publishes fine, renders
  fine and emails a forecast that is a day stale.
* the band. The f024 file holds a 0-24h and an 18-24h accumulation, and the six-hour
  one comes first. Picking by position produces a plausible map of the wrong period.
* the archive key. It has to land under the prefix the trigger Lambda ignores, or the
  copy starts a run of the elevation DAG.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pendulum
import pytest
import weather

CONFIG = {
    "instance_id": "i-0123456789abcdef0",
    "artifacts_bucket": "ice-artifacts-123456789012",
    "source_bucket": "example-dem",
    "email_to": "someone@example.com",
    "region": "eu-north-1",
}


class FakeTI:
    """Just enough task_instance to satisfy xcom_pull."""

    def __init__(self, values: dict):
        self.values = values

    def xcom_pull(self, task_ids: str):
        return self.values.get(task_ids)


# A scheduled 13:00 Eastern run on 2026-08-19, which is the case every assertion about
# dates below is anchored to. Its logical date is the 18th — that gap is the point.
RUN = "2026-08-19T13:00:00-04:00"

# What CI pins gdal_image_uri to: an immutable, SHA-tagged ECR reference.
IMAGE = "1234.dkr.ecr.eu-north-1.amazonaws.com/ice/gdal:abc"


def context(interval_end: str = RUN, *, conf: dict | None = None, xcoms: dict | None = None) -> dict:
    end = pendulum.parse(interval_end)
    return {
        "dag_run": SimpleNamespace(conf=conf or {}, run_id="manual__test"),
        "data_interval_end": end,
        "logical_date": end.subtract(days=1),
        "ti": FakeTI(xcoms or {}),
    }


@pytest.fixture(autouse=True)
def config(monkeypatch):
    monkeypatch.setattr(weather, "get_config", lambda: dict(CONFIG))


# ------------------------------------------------------------------- the cycle date


def test_the_run_fetches_todays_cycle_not_the_logical_dates():
    """The 13:00 run on the 19th wants gfs.20260819, whose logical date is the 18th.

    This is the bug the whole _cycle_date docstring exists for: reading logical_date
    here yields a forecast that is a full day stale and looks entirely normal.
    """
    plan = weather.resolve_cycle(**context())
    assert plan["date"] == "20260819"
    assert "dir=%2Fgfs.20260819%2F12%2Fatmos" in plan["url"]


def test_the_cycle_date_is_utc_even_when_the_schedule_is_not():
    """13:00 Eastern is 17:00 UTC the same day, so the dates agree. 21:00 Eastern would
    already be tomorrow in UTC — and NOMADS indexes by UTC date, so UTC wins."""
    plan = weather.resolve_cycle(**context("2026-08-19T21:00:00-04:00"))
    assert plan["date"] == "20260820"


def test_a_backfill_can_name_its_own_cycle():
    plan = weather.resolve_cycle(**context(conf={"date": "20260813", "cycle": "06"}))
    assert plan["date"] == "20260813"
    assert plan["cycle"] == "06"
    assert "gfs.t06z.pgrb2.0p25.f024" in plan["url"]


def test_the_products_are_keyed_by_cycle_so_a_rerun_overwrites_itself():
    plan = weather.resolve_cycle(**context())
    assert plan["prefix"] == "reports/weather/2026-08-19/12z"


def test_the_window_reaches_tomorrow():
    plan = weather.resolve_cycle(**context())
    assert plan["valid_from"].startswith("2026-08-19T12:00:00")
    assert plan["valid_to"].startswith("2026-08-20T12:00:00")


# ----------------------------------------------------------------------- the archive


def test_the_grib_is_archived_under_the_prefix_the_lambda_ignores(monkeypatch):
    """A key outside sent/ would fire ObjectCreated on the watched bucket. The Lambda's
    suffix filters do not cover .grib2 today, which makes this a latent trap rather
    than an immediate one — add .grib2 to raster_suffixes and it becomes a loop."""
    calls = {}

    class FakeS3:
        def copy(self, source, bucket, key):
            calls.update(source=source, bucket=bucket, key=key)

    monkeypatch.setattr(weather.boto3, "client", lambda name: FakeS3())

    plan = weather.resolve_cycle(**context())
    dest = weather.archive_grib(**context(xcoms={"resolve_cycle": plan}))

    assert dest.startswith(weather.ARCHIVE_PREFIX)
    assert dest == "sent/gfs/usa_rain_2026-08-19_12z.grib2"
    assert calls["bucket"] == CONFIG["source_bucket"]
    assert calls["source"] == {
        "Bucket": CONFIG["artifacts_bucket"],
        "Key": "reports/weather/2026-08-19/12z/usa_rain.grib2",
    }


def test_the_archive_copy_reads_artifacts_and_writes_the_source_bucket(monkeypatch):
    """Which bucket is which is the difference between a working copy and AccessDenied:
    the scheduler may only write under sent/ in the source bucket, and may not write to
    the artifacts bucket outside the STAC prefix at all."""
    seen = {}
    monkeypatch.setattr(
        weather.boto3,
        "client",
        lambda name: SimpleNamespace(
            copy=lambda source, bucket, key: seen.update(src=source["Bucket"], dst=bucket)
        ),
    )
    plan = weather.resolve_cycle(**context())
    weather.archive_grib(**context(xcoms={"resolve_cycle": plan}))

    assert seen == {"src": CONFIG["artifacts_bucket"], "dst": CONFIG["source_bucket"]}


# ------------------------------------------------------------------ the worker command


def test_the_container_runs_the_weather_script_not_the_raster_one(monkeypatch):
    """The image's ENTRYPOINT is gdal_report.py. Without the override this run would
    inspect a raster it was never given and fail on the missing --bucket."""
    captured = {}
    monkeypatch.setattr(weather.ec2_ssm, "run_shell", lambda *a, **k: captured.update(commands=a[1]) or "")
    monkeypatch.setattr(weather.Variable, "get", lambda name: IMAGE)

    plan = weather.resolve_cycle(**context())
    weather.run_weather_report(**context(xcoms={"resolve_cycle": plan}))

    docker_run = next(line for line in captured["commands"] if "docker run" in line)
    assert "--entrypoint python3" in docker_run
    assert weather.WEATHER_SCRIPT in docker_run
    assert "--accumulation 24" in docker_run


def test_the_url_is_quoted_into_the_shell_command(monkeypatch):
    """The URL is full of & and ? and lands in a root shell on the worker. Unquoted, the
    first & backgrounds the docker run and the rest becomes separate commands."""
    captured = {}
    monkeypatch.setattr(weather.ec2_ssm, "run_shell", lambda *a, **k: captured.update(commands=a[1]) or "")
    monkeypatch.setattr(weather.Variable, "get", lambda name: IMAGE)

    plan = weather.resolve_cycle(**context())
    weather.run_weather_report(**context(xcoms={"resolve_cycle": plan}))

    docker_run = next(line for line in captured["commands"] if "docker run" in line)
    assert f"'{plan['url']}'" in docker_run


# ------------------------------------------------------------------------- the polling


def test_an_unpublished_cycle_fails_before_the_worker_is_started(monkeypatch):
    """The ordering that keeps a late cycle from costing an EC2 start: this raises, and
    start_instance is downstream of it."""
    from airflow.exceptions import AirflowException

    monkeypatch.setattr(weather.gfs, "is_published", lambda url, timeout=30: False)
    monkeypatch.setattr(weather.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(weather, "get_config", lambda: {**CONFIG, "nomads_timeout": 60})

    plan = weather.resolve_cycle(**context())
    with pytest.raises(AirflowException, match="not serving GRIB2"):
        weather.wait_for_cycle(**context(xcoms={"resolve_cycle": plan}))


def test_the_wait_returns_the_same_url_it_probed(monkeypatch):
    monkeypatch.setattr(weather.gfs, "is_published", lambda url, timeout=30: True)
    plan = weather.resolve_cycle(**context())
    probed = weather.wait_for_cycle(**context(xcoms={"resolve_cycle": plan}))
    assert probed == plan["url"]


# --------------------------------------------------------------------------- dag shape


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    from conftest import DAGS_DIR

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False).dags["gdal_weather"]


def test_it_runs_at_one_pm_eastern(dag):
    """A UTC cron would drift an hour twice a year. The DAG's own timezone is what
    keeps this at 13:00 local across a DST boundary."""
    assert dag.schedule_interval == "0 13 * * *"
    assert str(dag.timezone) == "America/New_York"


def test_it_does_not_catch_up(dag):
    """Catchup would email a backlog of forecasts for weather that already happened."""
    assert dag.catchup is False


def test_single_active_run(dag):
    """Same constraint as gdalinfo_notify: one shared start/stop worker."""
    assert dag.max_active_runs == 1


def test_stop_instance_always_runs(dag):
    stop = dag.get_task("stop_instance")
    assert stop.trigger_rule == "all_done"
    assert stop.retries >= 2


def test_the_cycle_is_probed_before_the_worker_is_started(dag):
    assert dag.get_task("resolve_cycle").downstream_task_ids == {"wait_for_cycle"}
    assert dag.get_task("wait_for_cycle").downstream_task_ids == {"start_instance"}


def test_the_grib_is_archived_after_the_email(dag):
    """A failed copy must not withhold a map that has already been rendered."""
    assert dag.get_task("archive_grib").upstream_task_ids == {"build_and_send_email"}
    assert "stop_instance" in dag.get_task("archive_grib").get_flat_relative_ids(upstream=False)


def test_expected_tasks(dag):
    assert set(dag.task_ids) == {
        "resolve_cycle",
        "wait_for_cycle",
        "start_instance",
        "run_weather_report",
        "build_and_send_email",
        "archive_grib",
        "stop_instance",
    }


def test_it_is_not_wired_into_the_stac_catalogue(dag):
    """Deliberate, and worth asserting so it is a decision rather than an omission:
    the elevation collection describes static footprints, not daily forecasts."""
    assert not any("stac" in task_id for task_id in dag.task_ids)


# ------------------------------------------------------------------ the shared summary


def test_the_summary_shape_matches_what_the_renderer_reads():
    """gfs_rain.py writes this document on the worker and common.email_render reads it
    on the scheduler. Neither imports the other, so the field names are the contract."""
    from common.email_render import render_weather, weather_subject

    summary = json.loads(SUMMARY_JSON)
    body = render_weather(summary, "rainmap.20260819", report_location="s3://bucket/prefix/")

    assert "cid:rainmap.20260819" in body
    assert "76.4 mm" in body
    assert "APCP24" in body
    assert "2026-08-19T12:00:00+00:00" in body
    assert weather_subject(summary).startswith("[GFS] 24h US rainfall forecast to 2026-08-19")


def test_the_renderer_survives_a_summary_with_nothing_in_it():
    """Same standard the elevation email is held to: render something rather than fail
    the task after the GDAL work has already been paid for."""
    from common.email_render import render_weather, weather_subject

    assert "<html>" in render_weather({}, "cid")
    assert weather_subject({}).startswith("[GFS]")


# A trimmed copy of a real gfs_rain.py summary — the fields the email actually reads.
SUMMARY_JSON = json.dumps(
    {
        "source": "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?...",
        "file_name": "usa_rain.tif",
        "size_human": "62.2 KiB",
        "driver": "GTiff (GeoTIFF)",
        "crs": {"name": "WGS 84", "epsg": 4326, "units": "degree"},
        "width": 241,
        "height": 105,
        "pixel_size": {"x": 0.25, "y": -0.25},
        "corner_coordinates": {"upperLeft": [-125.125, 50.125], "lowerRight": [-64.875, 23.875]},
        "band_count": 1,
        "processed_at": "2026-08-19T17:05:00+00:00",
        "forecast": {
            "model": "GFS 0.25°",
            "element": "APCP24",
            "description": "24 hr Total precipitation [kg/(m^2)]",
            "cycle": "2026-08-18T12:00:00+00:00",
            "valid_from": "2026-08-18T12:00:00+00:00",
            "valid_to": "2026-08-19T12:00:00+00:00",
            "accumulation_hours": 24,
            "grib_band": 2,
            "grib_band_count": 2,
            "max_mm": 76.4375,
            "mean_mm": 1.472,
        },
    }
)


def test_utc_is_what_the_timestamps_mean():
    """Sanity check on the fixture above rather than on the code: every instant in the
    summary is UTC, and the email prints them verbatim without converting."""
    assert json.loads(SUMMARY_JSON)["forecast"]["valid_to"].endswith("+00:00")
    assert datetime.fromisoformat(json.loads(SUMMARY_JSON)["forecast"]["valid_to"]).tzinfo == UTC
