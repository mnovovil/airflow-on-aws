"""What the fire report can get wrong without ever failing a run.

Five things, and only one of them raises on its own:

* the branch. ``store_csv_s3`` is a ``BranchPythonOperator`` callable, so what it
  returns is a task id rather than its result. A returned id that names no task skips
  everything downstream, and the run still finishes green with nothing emailed.
* the row-less body. FIRMS answers a country with nothing burning with a header and no
  rows — but it answers a bad MAP_KEY the same shape, with ``Invalid MAP_KEY.`` as the
  header. Read both as "no fires" and an expired key arrives as a cheerful daily email
  saying the country is clear.
* the pairing. The keys ride on XCom and the country is read back off the file name.
  Pair a frame with a country by position instead and one country's fires are reported
  under another's.
* the key. It is a credential and it is no longer in the DAG file. A config missing it
  has to say so, because FIRMS answers a missing key with a 401 delivered as an
  unparseable CSV body several layers down.
* the country code. It reaches an OGR ``WHERE`` clause and it can come from the trigger
  form.
"""

from __future__ import annotations

import base64
import io
from email import message_from_string
from email.header import decode_header, make_header
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import fire
import pandas as pd
import pytest
from airflow.exceptions import AirflowException

COUNTRIES = [
    {"country": "ESP", "timezone": "Europe/Madrid"},
    {"country": "PRT", "timezone": "Europe/Lisbon"},
]

# The shape infra/fire.tf renders into ice_config under "fire". The map key is a
# placeholder of the right length: nothing here reaches FIRMS.
FIRE = {
    "map_key": "0" * 32,
    "countries_url": "https://example.invalid/ne_110m_admin_0_countries.zip",
    "prefix": "fires",
    "source": "VIIRS_NOAA20_NRT",
    "days": 2,
    "countries": COUNTRIES,
}
CONFIG = {"artifacts_bucket": "ice-artifacts", "email_to": "someone@example.com", "fire": FIRE}

# The day every assertion about dates is anchored to.
DAY = "2026-08-26"

# Spain's bounding box, to three decimals — what Natural Earth answers with.
SPAIN = (-9.393, 35.947, 3.039, 43.748)

HEADER = (
    "latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,"
    "instrument,confidence,version,bright_ti5,frp,daynight"
)
ROW = "40.1,-3.7,330.1,0.4,0.4,{date},1200,N20,VIIRS,n,2.0NRT,290.0,{frp},{daynight}"


def detections(count: int = 2, *, date: str = DAY) -> str:
    """A FIRMS CSV body with ``count`` rows in it."""
    rows = [ROW.format(date=date, frp=12.5 * (index + 1), daynight="DN"[index % 2]) for index in range(count)]
    return "\n".join([HEADER, *rows]) + "\n"


class FakeTI:
    """Just enough task_instance for a branch callable and the task after it."""

    def __init__(self, values: dict | None = None):
        self.values = dict(values or {})

    def xcom_push(self, key: str, value):
        self.values[key] = value

    def xcom_pull(self, task_ids: str, key: str | None = None):
        return self.values.get(key)


def context(*, params: dict | None = None, xcoms: dict | None = None, ds: str = DAY) -> dict:
    """A task context with the params the trigger form would have supplied."""
    form = {"country": None, "timezone": ""}
    return {"params": {**form, **(params or {})}, "ti": FakeTI(xcoms), "ds": ds}


@pytest.fixture(autouse=True)
def config(monkeypatch):
    """A fresh copy per test, so one that edits the fire block cannot leak into the next."""
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": dict(FIRE)})


class FakeGeoFrame:
    """What ``gpd.read_file`` returns: a frame that may hold one country, or none."""

    def __init__(self, bounds):
        self.empty = bounds is None
        self.geometry = SimpleNamespace(iloc=[SimpleNamespace(bounds=bounds)])
        self.plotted: list[dict] = []

    def plot(self, **kwargs):
        self.plotted.append(kwargs)


@pytest.fixture
def natural_earth(monkeypatch):
    """Natural Earth without the download, recording what it was asked for."""
    state = SimpleNamespace(calls=[], bounds=SPAIN, frame=None)

    def read_file(url, where=None):
        state.calls.append({"url": url, "where": where})
        state.frame = FakeGeoFrame(state.bounds)
        return state.frame

    monkeypatch.setattr(fire.gpd, "read_file", read_file)
    return state


@pytest.fixture
def firms(monkeypatch):
    """The bodies the FIRMS endpoint returns, in call order, and the URLs it was asked for.

    Patched at ``read_csv`` rather than at ``get_fires_country`` so the schema check
    inside it is part of what these tests cover — a row-less error body is exactly the
    thing that check has to catch and does not.
    """
    state = SimpleNamespace(bodies=[detections()], urls=[])
    real = pd.read_csv

    def read_csv(target, *args, **kwargs):
        if isinstance(target, str) and target.startswith("https://firms."):
            body = state.bodies[min(len(state.urls), len(state.bodies) - 1)]
            state.urls.append(target)
            return real(StringIO(body))
        return real(target, *args, **kwargs)

    monkeypatch.setattr(fire.pd, "read_csv", read_csv)
    return state


@pytest.fixture
def s3(monkeypatch):
    """An in-memory bucket. Returned as the dict of objects, keyed the way S3 keys them."""
    store: dict[str, SimpleNamespace] = {}

    class FakeS3:
        def put_object(self, Bucket, Key, Body, ContentType):
            store[Key] = SimpleNamespace(bucket=Bucket, body=Body, content_type=ContentType)

        def get_object(self, Bucket, Key):
            return {"Body": io.BytesIO(store[Key].body)}

    monkeypatch.setattr(fire.boto3, "client", lambda service: FakeS3())
    return store


def store(**kwargs) -> tuple[str, FakeTI]:
    """Run the branch callable and hand back its decision and the XCom it pushed."""
    ctx = context(**kwargs)
    return fire.store_csv_s3(**ctx), ctx["ti"]


# ------------------------------------------------------------------- country_settings


def test_a_scheduled_run_reports_on_every_configured_country():
    """No form, so no country: the path every scheduled and catchup run takes."""
    assert fire.country_settings(context()) == COUNTRIES


def test_the_form_names_the_country_under_the_key_the_tasks_read():
    """The regression this exists for: the fetch looks a country up by ``country`` and
    the S3 objects are named after it. Spelled ``id``, as it once was, a manual run
    reports on nothing and says nothing about it."""
    assert fire.country_settings(context(params={"country": "FRA"})) == [{"country": "FRA"}]


def test_a_country_typed_into_the_form_replaces_the_configured_ones():
    assert len(fire.country_settings(context(params={"country": "FRA"}))) == 1


def test_a_timezone_typed_into_the_form_travels_with_the_country():
    settings = fire.country_settings(context(params={"country": "FRA", "timezone": "Europe/Paris"}))
    assert settings == [{"country": "FRA", "timezone": "Europe/Paris"}]


def test_a_blank_timezone_is_left_off_rather_than_invented():
    """There is no fallback to invent one with — every value comes from ice_config."""
    assert fire.country_settings(context(params={"country": "FRA"})) == [{"country": "FRA"}]


def test_an_entry_the_fetch_cannot_look_up_is_rejected_by_name(monkeypatch):
    """An entry keyed ``id`` is not a failed run otherwise: it is a run that reports on
    nothing, which looks exactly like a quiet day."""
    thin = {**FIRE, "countries": [{"id": "ESP"}]}
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": thin})
    with pytest.raises(AirflowException, match="no 'country' code"):
        fire.country_settings(context())


def test_a_config_with_no_countries_raises_rather_than_emailing_nobody(monkeypatch):
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": {}})
    with pytest.raises(AirflowException, match="fire.countries"):
        fire.country_settings(context())


def test_an_empty_list_is_a_mistake_and_not_an_instruction(monkeypatch):
    """``countries = []`` in the secret, and a run that reports nothing looks exactly
    like one that worked."""
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": {**FIRE, "countries": []}})
    with pytest.raises(AirflowException, match="fire.countries"):
        fire.country_settings(context())


# --------------------------------------------------------------- country_timezone


def test_a_country_carries_its_own_zone():
    assert fire.country_timezone(COUNTRIES[1]) == "Europe/Lisbon"


def test_a_country_without_one_reports_none_rather_than_somebody_else_s():
    assert fire.country_timezone({"country": "ESP"}) is None


# ------------------------------------------------------------ get_country_geometry


@pytest.mark.parametrize("code", ["ESP' OR '1'='1", "ESP; DROP", "ES", "ESPA", "", None])
def test_a_code_that_is_not_three_letters_never_reaches_the_where_clause(code, natural_earth):
    """It is interpolated into OGR's SQL and it can come from the trigger form."""
    with pytest.raises(AirflowException, match="three-letter"):
        fire.get_country_geometry(code, FIRE["countries_url"])
    assert natural_earth.calls == []


def test_the_box_comes_back_as_west_south_east_north(natural_earth):
    assert fire.get_country_geometry("ESP", FIRE["countries_url"]) == SPAIN


def test_the_sovereignty_code_is_what_is_matched_on(natural_earth):
    """SOV_A3 and not ADM0_A3: the first keeps metropolitan France and its overseas
    departments together under FRA, which is the box the report is drawn from."""
    fire.get_country_geometry("esp", FIRE["countries_url"])
    assert natural_earth.calls[0]["where"] == "SOV_A3 = 'ESP'"


def test_a_caller_that_does_not_hold_the_url_resolves_it_from_the_config(natural_earth):
    """``get_fires_country`` is that caller, on the branch where it was handed no box —
    it has only the country code and no URL to pass on."""
    fire.get_country_geometry("ESP")
    assert natural_earth.calls[0]["url"] == FIRE["countries_url"]


def test_a_config_without_the_url_says_so(monkeypatch, natural_earth):
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": {}})
    with pytest.raises(AirflowException, match="fire.countries_url"):
        fire.get_country_geometry("ESP")


def test_a_well_formed_code_that_matches_nothing_names_the_country(natural_earth):
    """``.iloc[0]`` on an empty frame raises an IndexError that says nothing about
    which country was asked for."""
    natural_earth.bounds = None
    with pytest.raises(AirflowException, match="ZZZ"):
        fire.get_country_geometry("ZZZ", FIRE["countries_url"])


# ------------------------------------------------------------------ upload_dataframe_s3


def test_the_object_is_written_as_a_csv_that_previews(s3):
    """S3 assumes application/octet-stream, which makes a console click download the
    file rather than show it."""
    frame = pd.DataFrame({"latitude": [40.1], "longitude": [-3.7]})
    key = fire.upload_dataframe_s3(frame, "ice-artifacts", "fires/x.csv")
    assert key == "fires/x.csv"
    assert s3["fires/x.csv"].content_type == "text/csv"
    assert s3["fires/x.csv"].body.decode().splitlines()[0] == "latitude,longitude"


# --------------------------------------------------------------- store_csv_s3, the branch


def test_a_country_with_detections_goes_to_the_report(natural_earth, firms, s3):
    branch, _ = store()
    assert branch == fire.EMAIL_TASK_ID


def test_the_keys_ride_on_xcom_because_the_return_value_is_the_branch(natural_earth, firms, s3):
    """A branch callable's return value is its decision, so the keys cannot also be it."""
    branch, ti = store()
    assert branch == fire.EMAIL_TASK_ID
    assert ti.values[fire.XCOM_WRITTEN] == list(s3)


def test_the_key_carries_the_source_and_the_run_s_day(natural_earth, firms, s3):
    """Two instruments over one country on one day are two answers, not one that
    overwrites the other."""
    store()
    assert f"fires/VIIRS_NOAA20_NRT/{DAY}/ESP.csv" in s3


def test_the_day_is_the_run_s_and_not_today(natural_earth, firms, s3):
    """A cleared task has to re-write its own key rather than scatter a second copy
    under whatever today happens to be."""
    store(ds="2026-01-02")
    assert all("/2026-01-02/" in key for key in s3)


def test_one_object_per_country(natural_earth, firms, s3):
    store()
    assert sorted(Path(key).stem for key in s3) == ["ESP", "PRT"]


def test_a_country_overrides_the_run_wide_source(natural_earth, firms, s3, monkeypatch):
    countries = [{"country": "ESP", "source": "MODIS_NRT", "days": 1}]
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": {**FIRE, "countries": countries}})
    store()
    assert f"fires/MODIS_NRT/{DAY}/ESP.csv" in s3
    assert firms.urls[0].endswith("/1")


def test_a_prefix_with_a_trailing_slash_does_not_make_an_empty_path_segment(
    natural_earth, firms, s3, monkeypatch
):
    """S3 accepts ``fires//VIIRS/...`` and no console shows it under the folder you
    expect."""
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": {**FIRE, "prefix": "fires/"}})
    store()
    assert all("//" not in key for key in s3)


# ---------------------------------------------------- store_csv_s3, the bodies with no rows


def test_a_header_and_no_rows_is_the_no_data_branch(natural_earth, firms, s3):
    """FIRMS' ordinary answer for a country with nothing burning."""
    firms.bodies = [HEADER + "\n"]
    branch, ti = store()
    assert branch == fire.NO_DATA_TASK_ID
    assert ti.values[fire.XCOM_WRITTEN] == []
    assert s3 == {}


def test_a_header_without_a_trailing_newline_is_the_same_thing(natural_earth, firms, s3):
    firms.bodies = [HEADER]
    assert store()[0] == fire.NO_DATA_TASK_ID


def test_a_body_with_nothing_in_it_at_all_is_the_no_data_branch(natural_earth, firms, s3):
    """``read_csv`` raises EmptyDataError on zero bytes rather than returning a frame,
    so without catching it the task dies instead of branching."""
    firms.bodies = [""]
    assert store()[0] == fire.NO_DATA_TASK_ID


def test_an_error_body_is_not_quietly_reported_as_no_fires(natural_earth, firms, s3):
    """The one worth the whole file. FIRMS answers a bad key with ``Invalid MAP_KEY.``,
    which parses to a frame with a header and no rows — the same shape as a quiet day.
    Read as no data, an expired key arrives as a daily email saying the country is
    clear, and nothing anywhere goes red."""
    firms.bodies = ["Invalid MAP_KEY."]
    with pytest.raises(AirflowException, match="non-CSV body"):
        store()


def test_an_html_error_page_is_caught_the_same_way(natural_earth, firms, s3):
    firms.bodies = ["<html><body>403 Forbidden</body></html>"]
    with pytest.raises(AirflowException, match="non-CSV body"):
        store()


def test_one_quiet_country_does_not_silence_a_burning_one(natural_earth, firms, s3):
    """The branch is taken on the run, not on a country: ESP has fires, PRT does not,
    and the report still has to go out for ESP."""
    firms.bodies = [detections(), HEADER + "\n"]
    branch, ti = store()
    assert branch == fire.EMAIL_TASK_ID
    assert [Path(key).stem for key in ti.values[fire.XCOM_WRITTEN]] == ["ESP"]


def test_the_box_is_resolved_once_per_country(natural_earth, firms, s3):
    """Natural Earth is a download, and get_fires_country used to re-derive the box the
    caller had just handed it — two downloads per country for one unchanging answer."""
    store()
    assert [call["where"] for call in natural_earth.calls] == ["SOV_A3 = 'ESP'", "SOV_A3 = 'PRT'"]


def test_the_box_the_caller_holds_is_the_box_that_is_asked_for(natural_earth, firms):
    """The bounds argument reaching the URL is what makes resolving it once safe: were
    it still ignored, the saving would be real and the box would be re-derived anyway."""
    fire.get_fires_country("0" * 32, (1.0, 2.0, 3.0, 4.0), "ESP")
    assert "/1.0,2.0,3.0,4.0/" in firms.urls[0]
    assert natural_earth.calls == []


def test_a_caller_with_no_box_still_gets_one(natural_earth, firms):
    """``bounds`` is a shortcut past the download, not a requirement to call at all."""
    fire.get_fires_country("0" * 32, None, "ESP")
    assert f"/{SPAIN[0]}," in firms.urls[0]
    assert natural_earth.calls[0]["where"] == "SOV_A3 = 'ESP'"


# ------------------------------------------------------- store_csv_s3, the configuration


@pytest.mark.parametrize("key", ["map_key", "countries_url", "prefix", "source", "days"])
def test_a_missing_config_key_is_named(key, natural_earth, firms, s3, monkeypatch):
    """A missing map_key otherwise surfaces as FIRMS' 401, delivered as an unparseable
    CSV body several layers down."""
    monkeypatch.setattr(
        fire, "get_config", lambda: {**CONFIG, "fire": {k: v for k, v in FIRE.items() if k != key}}
    )
    with pytest.raises(AirflowException, match=key):
        store()


def test_every_missing_key_is_named_at_once(natural_earth, firms, s3, monkeypatch):
    """One failed run should name the whole gap, not one key per attempt."""
    thin = {k: v for k, v in FIRE.items() if k not in ("prefix", "days")}
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": thin})
    with pytest.raises(AirflowException, match=r"\['prefix', 'days'\]"):
        store()


def test_the_config_error_points_at_the_terraform_that_owns_the_value(natural_earth, firms, s3, monkeypatch):
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": {}})
    with pytest.raises(AirflowException, match="infra/fire.tf"):
        store()


def test_nothing_is_fetched_before_the_config_is_checked(natural_earth, firms, s3, monkeypatch):
    """A run that cannot finish should not spend a FIRMS request finding out."""
    monkeypatch.setattr(fire, "get_config", lambda: {**CONFIG, "fire": {}})
    with pytest.raises(AirflowException):
        store()
    assert firms.urls == []


# ----------------------------------------------------------------------- create_chart

# A real 1×1 PNG: MIMEImage sniffs the bytes to pick its subtype, so a placeholder
# string would be attached as application/octet-stream and nothing would say so.
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


def frame_of(count: int = 3) -> pd.DataFrame:
    return pd.read_csv(StringIO(detections(count)))


def test_a_country_with_nothing_detected_is_never_charted(tmp_path, natural_earth):
    assert fire.create_chart(pd.DataFrame(), "ESP", DAY, str(tmp_path), FIRE["countries_url"]) is None


def test_the_chart_is_named_for_its_country_and_day(tmp_path, natural_earth):
    """Several countries land in one directory per run, and a shared name is one
    overwriting the other."""
    path = fire.create_chart(frame_of(), "ESP", DAY, str(tmp_path), FIRE["countries_url"])
    assert Path(path).name == f"ESP-{DAY}.png"
    assert Path(path).stat().st_size > 0


def test_a_country_code_from_the_form_cannot_escape_the_filename(tmp_path, natural_earth):
    path = fire.create_chart(frame_of(), "../ESP", DAY, str(tmp_path), FIRE["countries_url"])
    assert Path(path).parent == tmp_path


def test_the_outline_is_drawn_from_the_url_it_was_handed(tmp_path, natural_earth):
    """Not from a constant: the file is configurable so a swap to Natural Earth's finer
    50m polygons does not need a redeploy."""
    fire.create_chart(frame_of(), "ESP", DAY, str(tmp_path), "https://example.invalid/50m.zip")
    assert natural_earth.calls[0]["url"] == "https://example.invalid/50m.zip"


def test_an_outline_that_will_not_download_still_leaves_a_chart(tmp_path, monkeypatch):
    """The outline is context; the detections are the data. A third-party download on
    the send path should cost the coastline and nothing else."""

    def boom(*args, **kwargs):
        raise OSError("naciscdn is unreachable")

    monkeypatch.setattr(fire.gpd, "read_file", boom)
    path = fire.create_chart(frame_of(), "ESP", DAY, str(tmp_path), FIRE["countries_url"])
    assert path and Path(path).exists()


def test_a_detection_with_no_frp_is_still_on_the_map(tmp_path, natural_earth):
    """FIRMS leaves the field blank on a detection it could not quantify, and a NaN
    passed to scatter drops that point silently."""
    frame = frame_of()
    frame.loc[0, "frp"] = None
    assert fire.create_chart(frame, "ESP", DAY, str(tmp_path), FIRE["countries_url"])


def test_a_frame_where_nothing_has_an_frp_still_draws(tmp_path, natural_earth):
    """Every point at 0 MW makes the size ramp a division by zero."""
    frame = frame_of()
    frame["frp"] = None
    assert fire.create_chart(frame, "ESP", DAY, str(tmp_path), FIRE["countries_url"])


# ------------------------------------------------------------------ build_and_send_email


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

    def fake_chart(frame, country, day, directory, countries_url):
        path = Path(directory) / f"{country}.png"
        path.write_bytes(PNG)
        made.append({"country": country, "path": path, "detections": len(frame)})
        return str(path)

    monkeypatch.setattr(fire, "create_chart", fake_chart)
    return made


@pytest.fixture
def written(natural_earth, firms, s3):
    """Two countries' objects in the bucket, and the XCom the branch pushed for them."""
    _, ti = store()
    return ti.values[fire.XCOM_WRITTEN]


def report(keys=None, **kwargs) -> list[dict]:
    ctx = context(xcoms={fire.XCOM_WRITTEN: keys}, **kwargs)
    fire.build_and_send_email(**ctx)
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


def test_one_email_per_country_that_had_fires(smtp, charts, s3, written):
    assert len(report(written)) == 2


def test_the_connection_is_opened_once_for_the_whole_report(smtp, charts, s3, written):
    report(written)
    assert smtp.opened == 1


def test_each_email_carries_its_own_country_s_fires(smtp, charts, s3, written):
    """The end-to-end statement of the pairing: the object named ESP.csv has to reach
    the email headed ESP. Paired by position instead and both are plausible."""
    subjects = [rendered(message)[0] for message in report(written)]
    assert "ESP" in subjects[0] and "PRT" in subjects[1]


def test_the_country_is_read_back_off_the_key(smtp, charts, s3, written):
    report(written)
    assert [chart["country"] for chart in charts] == ["ESP", "PRT"]


def test_the_report_never_sends_an_email_with_no_detections_in_it(smtp, charts, s3):
    """The empty email is gone from this task: a run with nothing to report takes the
    other branch instead. An object with only a header is the shape that used to reach
    here, and it is skipped rather than mailed as a blank report."""
    s3["fires/VIIRS_NOAA20_NRT/2026-08-26/ZZZ.csv"] = SimpleNamespace(
        bucket="ice-artifacts", body=(HEADER + "\n").encode(), content_type="text/csv"
    )
    assert report(list(s3)) == []
    assert charts == []


def test_the_body_and_the_part_agree_on_the_content_id(smtp, charts, s3, written):
    """The angle brackets go on the header and not on the cid: reference. Get that
    asymmetry wrong and the map arrives as a second attachment plus a broken image,
    which no assertion about 'an image is present' would catch."""
    for message in report(written):
        body = rendered(message)[1]
        cid = images(message)[0]["Content-ID"]
        assert cid.startswith("<") and cid.endswith(">")
        assert f'src="cid:{cid[1:-1]}"' in body


def test_the_two_emails_do_not_share_a_content_id(smtp, charts, s3, written):
    """A shared cid lets a threaded reply resolve one country's map against another's."""
    report(written)
    assert len({images(message)[0]["Content-ID"] for message in FakeSmtp.sent}) == 2


def test_a_map_that_would_not_draw_still_sends_its_email(smtp, s3, written, monkeypatch):
    """The numbers are already in hand by this point and are worth more than the
    picture."""

    def boom(*args, **kwargs):
        raise RuntimeError("no renderer")

    monkeypatch.setattr(fire, "create_chart", boom)
    sent = report(written)
    assert len(sent) == 2
    assert all(images(message) == [] for message in sent)
    assert all("cid:" not in rendered(message)[1] for message in sent)


def test_the_subject_carries_the_count_and_the_peak(smtp, charts, s3, written):
    subject = rendered(report(written)[0])[0]
    assert "ESP" in subject and DAY in subject and "detection(s)" in subject


def test_the_body_names_the_object_the_numbers_came_from(smtp, charts, s3, written):
    body = rendered(report(written)[0])[1]
    assert f"s3://ice-artifacts/{written[0]}" in body


def test_a_cleared_task_with_no_upstream_says_so(smtp):
    with pytest.raises(AirflowException, match=fire.STORE_TASK_ID):
        report(None)


def test_the_charts_do_not_outlive_the_task(smtp, charts, s3, written):
    """A tempdir per run, cleaned up on the way out — the worker's disk is not a cache."""
    report(written)
    assert charts and not any(chart["path"].exists() for chart in charts)


# ----------------------------------------------------------- build_and_send_no_data_email


def notice(**kwargs) -> list[dict]:
    fire.build_and_send_no_data_email(**context(**kwargs))
    return FakeSmtp.sent


def test_a_run_that_found_nothing_still_gets_one_email(smtp):
    assert len(notice()) == 1


def test_the_notice_names_the_countries_and_the_day(smtp):
    subject, body = rendered(notice()[0])
    assert "ESP, PRT" in subject and DAY in subject
    assert "no detections" in body


def test_the_notice_carries_no_attachment(smtp):
    """There is nothing to draw, so there is no chart and no part to reference."""
    assert images(notice()[0]) == []
    assert "cid:" not in rendered(FakeSmtp.sent[0])[1]


def test_the_notice_goes_to_the_configured_recipients(smtp):
    assert notice()[0]["to"] == ["someone@example.com"]


# ------------------------------------------------------------------------------ the DAG


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    from conftest import DAGS_DIR

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False).dags[fire.DAG_ID]


def test_the_dag_is_a_branch_into_two_emails(dag):
    assert set(dag.task_ids) == {fire.STORE_TASK_ID, fire.EMAIL_TASK_ID, fire.NO_DATA_TASK_ID}
    assert dag.get_task(fire.STORE_TASK_ID).downstream_task_ids == {
        fire.EMAIL_TASK_ID,
        fire.NO_DATA_TASK_ID,
    }


def test_the_store_task_is_the_branch_operator(dag):
    """A plain PythonOperator here returns the task id into XCom and runs both emails."""
    from airflow.operators.python import BranchPythonOperator

    assert isinstance(dag.get_task(fire.STORE_TASK_ID), BranchPythonOperator)


def test_every_branch_the_store_task_can_return_names_a_task_that_exists(dag, natural_earth, firms, s3):
    """The failure this guards is silent: a branch that names no task skips everything
    downstream, and the run finishes green having emailed nobody. Both decisions are
    taken for real here rather than asserted against the constants."""
    assert store()[0] in dag.task_ids

    firms.bodies = [HEADER + "\n"]
    assert store()[0] in dag.task_ids


def test_the_two_emails_are_leaves(dag):
    """Anything downstream of both would need an explicit trigger rule, because the
    branch stamps SKIPPED on the one it did not pick."""
    for task_id in (fire.EMAIL_TASK_ID, fire.NO_DATA_TASK_ID):
        assert dag.get_task(task_id).downstream_task_ids == set()


def test_the_form_defaults_are_empty_so_a_scheduled_run_uses_the_config(dag):
    """A default of ESP here would pin every scheduled run to one country and make the
    config unreachable."""
    assert dag.params["country"] in (None, "")
    assert dag.params["timezone"] == ""


def test_the_dag_does_not_fetch_the_same_country_twice(dag):
    """store_csv_s3 already downloads through get_fires_country, so a separate fetch
    task upstream would pull every country from FIRMS a second time."""
    assert dag.get_task(fire.STORE_TASK_ID).upstream_task_ids == set()
