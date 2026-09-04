"""What the temperature report can get wrong without ever failing a run.

Four things, and none of them raises:

* the window. Meteostat's bounds are inclusive and its stamps are UTC unless it is
  told otherwise. ``end = day + 1`` fetches two days, and a missing timezone puts the
  hours of a New York day on the chart labelled as somebody else's.
* the round trip. The observations reach the email as JSON on XCom. A tz-aware index
  comes back out as UTC, which relabels every hour on a chart that still looks right.
* the empty day. A station with a gap in its record returns a frame with no ``temp``
  column at all — a KeyError on the fetch, not a quiet one, and the day is ordinary.
* the pairing. The series ride on XCom keyed by station ID; pair them with the
  configured stations by position instead and one station's temperatures arrive under
  another station's name.
"""

from __future__ import annotations

import base64
from email import message_from_string
from email.header import decode_header, make_header
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import temperature as tp

STATIONS = [
    {"id": "72503", "name": "LaGuardia Airport", "timezone": "America/New_York"},
    {"id": "07156", "name": "Paris-Orly", "timezone": "Europe/Paris"},
]
CONFIG = {"email_to": "someone@example.com", "temperature": STATIONS}

# The day every assertion about dates is anchored to.
DAY = "2026-08-24"


class FakeTI:
    """Just enough task_instance to satisfy xcom_pull."""

    def __init__(self, values: dict):
        self.values = values

    def xcom_pull(self, task_ids: str):
        return self.values.get(task_ids)


def context(*, params: dict | None = None, xcoms: dict | None = None) -> dict:
    """A task context with the params the trigger form would have supplied."""
    form = {"station": None, "name": "", "timezone": ""}
    return {"params": {**form, **(params or {})}, "ti": FakeTI(xcoms or {})}


def series(temps=(18.0, 21.5, 24.0, 19.5), *, day: str = DAY, tz: str | None = None) -> pd.Series:
    """A few hours of observations, indexed the way XCom hands them back: local wall
    time with no offset on it."""
    index = pd.DatetimeIndex([f"{day} {hour + 6}:00" for hour in range(len(temps))], name="time")
    if tz:
        index = index.tz_localize(tz)
    return pd.Series(list(temps), index=index, name="temp")


@pytest.fixture(autouse=True)
def config(monkeypatch):
    monkeypatch.setattr(tp, "get_config", lambda: dict(CONFIG))


# ----------------------------------------------------------------- station_settings


def test_a_scheduled_run_reports_on_every_configured_station():
    """No form, so no station: the path every scheduled and catchup run takes."""
    assert tp.station_settings(context()) == STATIONS


def test_a_config_without_the_key_falls_back_to_one_flat_station(monkeypatch):
    """DEFAULT_STATION is a list, and wrapping it again would hand every task a list
    whose single element is a list — which nothing here raises on."""
    monkeypatch.setattr(tp, "get_config", lambda: {"email_to": "a@b.c"})
    stations = tp.station_settings(context())
    assert stations == tp.DEFAULT_STATION
    assert all(isinstance(station, dict) for station in stations)


def test_an_empty_list_is_not_an_empty_report(monkeypatch):
    """``temperature = []`` in the secret is a mistake, not an instruction to email
    nobody — and a run that reports nothing looks exactly like one that worked."""
    monkeypatch.setattr(tp, "get_config", lambda: {"email_to": "a@b.c", "temperature": []})
    assert tp.station_settings(context()) == tp.DEFAULT_STATION


def test_the_form_names_the_station_under_the_key_the_tasks_read():
    """The regression this test exists for: the form field is ``station``, and every
    task downstream looks the station up by ``id``. Either half spelled the other way
    means a manual run silently reports the configured station instead."""
    settings = tp.station_settings(context(params={"station": "10637", "name": "Frankfurt"}))
    assert settings == [{"id": "10637", "name": "Frankfurt", "timezone": tp.DEFAULT_TIMEZONE}]


def test_a_station_typed_into_the_form_replaces_the_configured_ones():
    assert len(tp.station_settings(context(params={"station": "10637"}))) == 1


# -------------------------------------------------------------- get_temperature_data


class FakeHourly:
    """The recorded state of a faked ``meteostat.Hourly``."""

    calls: list[dict] = []
    frame: pd.DataFrame | None = None

    def __init__(self, loc, start, end, timezone=None):
        FakeHourly.calls.append({"loc": loc, "start": start, "end": end, "timezone": timezone})

    def fetch(self):
        if FakeHourly.frame is not None:
            return FakeHourly.frame
        # tz-aware, the way Meteostat returns it when a timezone is named.
        return pd.DataFrame({"temp": [18.0, 21.5], "rhum": [80, 60]}, index=series(temps=(0, 0)).index)


@pytest.fixture
def meteostat(monkeypatch):
    FakeHourly.calls = []
    FakeHourly.frame = None
    monkeypatch.setattr(tp.ms, "Hourly", FakeHourly)
    return FakeHourly


def test_the_window_is_the_one_day_asked_for(meteostat):
    """Meteostat's ``end`` is inclusive, so the obvious ``day + 1 day`` fetches two
    days and charts tomorrow's small hours onto today."""
    tp.get_temperature_data(DAY, **context())
    start, end = meteostat.calls[0]["start"], meteostat.calls[0]["end"]
    assert start.date().isoformat() == DAY
    assert end.date().isoformat() == DAY
    assert (start.hour, start.minute) == (0, 0)
    assert (end.hour, end.minute) == (23, 59)


def test_each_station_is_fetched_in_its_own_timezone(meteostat):
    """Without this the stamps are UTC, and a Paris day drawn in UTC starts at 02:00."""
    tp.get_temperature_data(DAY, **context())
    assert [call["timezone"] for call in meteostat.calls] == ["America/New_York", "Europe/Paris"]


def test_a_station_without_a_configured_timezone_still_gets_one(meteostat, monkeypatch):
    monkeypatch.setattr(tp, "get_config", lambda: {"email_to": "a@b.c", "temperature": [{"id": "1"}]})
    tp.get_temperature_data(DAY, **context())
    assert meteostat.calls[0]["timezone"] == tp.DEFAULT_TIMEZONE


def test_the_day_is_the_run_s_and_not_today(meteostat):
    """A backfill has to re-fetch its own day, or a cleared task reports this morning
    under a past date."""
    tp.get_temperature_data("2026-01-02", **context())
    assert meteostat.calls[0]["start"].date().isoformat() == "2026-01-02"


def test_the_observations_are_keyed_by_station(meteostat):
    payload = tp.get_temperature_data(DAY, **context())
    assert list(payload) == ["72503", "07156"]


def test_the_hours_survive_the_xcom_round_trip(meteostat):
    """The failure this guards is invisible: a tz-aware index serialises with its
    offset and comes back as UTC, so the chart is right about the numbers and four
    hours wrong about when they happened."""
    from io import StringIO

    aware = series(temps=(0, 0), tz="America/New_York").index
    FakeHourly.frame = pd.DataFrame({"temp": [18.0, 21.5]}, index=aware)
    payload = tp.get_temperature_data(DAY, **context())
    back = pd.read_json(StringIO(payload["72503"]), orient="split", typ="series")
    assert back.index.tz is None
    assert [stamp.hour for stamp in back.index] == [6, 7]


def test_a_day_the_station_did_not_report_is_an_empty_series_not_an_error(meteostat):
    """No ``temp`` column at all is what a gap in the record looks like."""
    FakeHourly.frame = pd.DataFrame()
    payload = tp.get_temperature_data(DAY, **context())
    from io import StringIO

    assert pd.read_json(StringIO(payload["72503"]), orient="split", typ="series").empty


def test_rows_with_no_reading_are_dropped(meteostat):
    """A NaN plots as a gap and drags min/max to NaN with it, which renders as an
    email reporting a swing of nan°C."""
    FakeHourly.frame = pd.DataFrame({"temp": [18.0, None, 21.5, None]}, index=series().index)
    from io import StringIO

    payload = tp.get_temperature_data(DAY, **context())
    assert len(pd.read_json(StringIO(payload["72503"]), orient="split", typ="series")) == 2


# ----------------------------------------------------------------------- create_chart


def test_the_chart_is_a_png_in_the_directory_it_was_handed(tmp_path):
    path = tp.create_chart(series(), STATIONS[0], str(tmp_path))
    assert Path(path).parent == tmp_path
    # The magic number, not the extension: build_message attaches this by content.
    assert Path(path).read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_the_file_is_named_for_the_station_and_the_day(tmp_path):
    """Several stations share one tempdir per run. Named for the day alone, the second
    email would carry the first station's chart — and nothing would raise."""
    for station in STATIONS:
        tp.create_chart(series(), station, str(tmp_path))
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"07156-{DAY}.png", f"72503-{DAY}.png"]


def test_the_day_in_the_name_comes_from_the_data(tmp_path):
    tp.create_chart(series(day="2026-01-02"), STATIONS[0], str(tmp_path))
    assert [p.name for p in tmp_path.iterdir()] == ["72503-2026-01-02.png"]


def test_a_station_id_from_the_form_cannot_escape_the_directory(tmp_path):
    """The ID reaches the filename, and a manual run is free to type anything into it."""
    hostile = {"id": "../../etc/passwd", "name": "nope"}
    path = tp.create_chart(series(), hostile, str(tmp_path))
    assert Path(path).parent == tmp_path


def test_a_day_with_no_observations_is_not_charted(tmp_path):
    assert tp.create_chart(pd.Series(dtype="float64"), STATIONS[0], str(tmp_path)) is None
    assert list(tmp_path.iterdir()) == []


def test_one_observation_is_still_a_chart(tmp_path):
    """min == max, so the fill has no height and the swing is 0.0°C. It draws."""
    assert tp.create_chart(series(temps=(18.0,)), STATIONS[0], str(tmp_path)) is not None


def test_a_chart_that_blows_up_does_not_take_the_email_with_it(monkeypatch, tmp_path):
    def explode(*args, **kwargs):
        raise RuntimeError("no font, no nothing")

    monkeypatch.setattr(tp, "create_chart", explode)
    assert tp.chart_for(series(), STATIONS[0], str(tmp_path)) is None


# ------------------------------------------------------------ build_and_send_email

# A real 1×1 PNG: MIMEImage sniffs the bytes to pick its subtype, so a placeholder
# string would be attached as application/octet-stream and nothing would say so.
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


class FakeSmtp:
    """SmtpHook is only a context manager here: it opens in __enter__ and its
    smtp_client is None until it has."""

    sent: list[dict] = []
    opened = 0

    def __enter__(self):
        FakeSmtp.opened += 1
        self.from_email = "ice@example.com"
        self.smtp_client = SimpleNamespace(sendmail=self._sendmail)
        return self

    def __exit__(self, *exc):
        return False

    @staticmethod
    def _sendmail(from_addr, to_addrs, msg):
        FakeSmtp.sent.append({"from": from_addr, "to": to_addrs, "msg": msg})


@pytest.fixture
def smtp(monkeypatch):
    FakeSmtp.sent = []
    FakeSmtp.opened = 0
    monkeypatch.setattr("airflow.providers.smtp.hooks.smtp.SmtpHook", FakeSmtp)
    return FakeSmtp


@pytest.fixture
def charts(monkeypatch):
    """Stand in for create_chart, which has its own tests above."""
    made: list[dict] = []

    def fake_chart(observations, station, directory):
        path = Path(directory) / f"{station['id']}.png"
        path.write_bytes(PNG)
        made.append({"id": station["id"], "path": path, "observations": observations})
        return str(path)

    monkeypatch.setattr(tp, "create_chart", fake_chart)
    return made


def payload_for(temps=((18.0, 24.0), (12.0, 15.0))) -> dict[str, str]:
    return {
        station["id"]: series(temps=pair).to_json(orient="split", date_format="iso")
        for station, pair in zip(STATIONS, temps, strict=False)
    }


def send(smtp_payload: dict | None = None, **kwargs) -> list[dict]:
    ctx = context(xcoms={tp.DRAW_TASK_ID: payload_for() if smtp_payload is None else smtp_payload}, **kwargs)
    tp.build_and_send_email(**ctx)
    return FakeSmtp.sent


def rendered(message: dict) -> tuple[str, str]:
    """The subject and body of a sent message, as text — both come off the wire
    encoded, so neither is searchable in the raw string."""
    parsed = message_from_string(message["msg"])
    subject = str(make_header(decode_header(parsed["Subject"])))
    html = next(part for part in parsed.walk() if part.get_content_type() == "text/html")
    return subject, html.get_payload(decode=True).decode()


def images(message: dict) -> list:
    parsed = message_from_string(message["msg"])
    return [part for part in parsed.walk() if part.get_content_type() == "image/png"]


def test_one_email_per_station(smtp, charts):
    assert len(send()) == 2


def test_the_connection_is_opened_once_for_the_whole_report(smtp, charts):
    send()
    assert smtp.opened == 1


def test_each_email_carries_its_own_station_s_temperatures(smtp, charts):
    """The end-to-end statement of the pairing: the series keyed 72503 has to reach
    the email headed LaGuardia Airport."""
    subjects = [rendered(message)[0] for message in send()]
    assert "LaGuardia Airport" in subjects[0] and "24.0" in subjects[0]
    assert "Paris-Orly" in subjects[1] and "15.0" in subjects[1]


def test_the_chart_is_drawn_from_the_series_on_xcom(smtp, charts):
    """Not from a second Meteostat call — the observations are already in hand, and a
    network call on the send path is a report that can be lost to a timeout."""
    send()
    assert [list(chart["observations"]) for chart in charts] == [[18.0, 24.0], [12.0, 15.0]]


def test_the_body_and_the_part_agree_on_the_content_id(smtp, charts):
    """The angle brackets go on the header and not on the cid: reference. Get that
    asymmetry wrong and the picture arrives as a second attachment plus a broken
    image, which no assertion about 'an image is present' would catch."""
    for message in send():
        body = rendered(message)[1]
        cid = images(message)[0]["Content-ID"]
        assert cid.startswith("<") and cid.endswith(">")
        assert f'src="cid:{cid[1:-1]}"' in body


def test_the_two_emails_do_not_share_a_content_id(smtp, charts):
    send()
    assert len({images(message)[0]["Content-ID"] for message in FakeSmtp.sent}) == 2


def test_a_day_with_no_chart_still_sends_its_email(smtp, monkeypatch):
    monkeypatch.setattr(tp, "create_chart", lambda observations, station, directory: None)
    sent = send()
    assert len(sent) == 2
    assert all(images(message) == [] for message in sent)
    assert all("cid:" not in rendered(message)[1] for message in sent)


def test_an_empty_series_is_never_charted(smtp, charts):
    empty = {station["id"]: pd.Series(dtype="float64").to_json(orient="split") for station in STATIONS}
    sent = send(empty)
    assert len(sent) == 2
    assert charts == []
    assert all("no observations" in rendered(message)[0] for message in sent)


def test_a_station_added_after_the_fetch_is_skipped_rather_than_mispaired(smtp, charts):
    """The config is read again here, so it can have grown since the fetch. The
    missing station goes unreported; the one that is there is still reported."""
    partial = {STATIONS[0]["id"]: payload_for()[STATIONS[0]["id"]]}
    assert len(send(partial)) == 1


def test_a_cleared_task_with_no_upstream_says_so(smtp):
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match=tp.DRAW_TASK_ID):
        tp.build_and_send_email(**context(xcoms={}))


def test_the_charts_do_not_outlive_the_task(smtp, charts):
    """A tempdir per run, cleaned up on the way out — the worker's disk is not a
    cache, and a PNG per station per run adds up."""
    send()
    assert charts and not any(chart["path"].exists() for chart in charts)
