"""What the multi-stock report can get wrong without ever failing a run.

Four things, and none of them raises:

* the shape. ``stock_settings`` returns a list now, and the constant behind it is a
  list too. One ``[...]`` too many anywhere on that path and the tasks iterate over a
  list of lists — asking yfinance for the price of a dict, which it answers with an
  empty frame and a warning nobody reads.
* the pairing. The bars ride on XCom keyed by ticker; pair them with the configured
  stocks by position instead and a config edited between the two tasks renders one
  company's prices under another company's name.
* the day. The bar a run fetches has to be a function of the run's interval, not of
  today, or a backfill reports this morning's session under a past date.
* the fan-out. One stock that errors, or one email that fails to send, is a whole
  report lost — these tasks loop, so the loop is the unit of failure.
"""

from __future__ import annotations

import base64
from email import message_from_string
from email.header import decode_header, make_header
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import sgo

STOCKS = [
    {"ticker": "SGO.PA", "name": "Saint-Gobain", "currency": "€"},
    {"ticker": "AI.PA", "name": "Air Liquide", "currency": "€"},
    {"ticker": "MC.PA", "name": "LVMH", "currency": "€"},
]
CONFIG = {"email_to": "someone@example.com", "stocks": STOCKS}

# The session every assertion about dates is anchored to, and the interval end that
# renders to it: a 13:00 Eastern run on the 19th is 17:00 UTC the same day.
SESSION = "2026-08-19"

OHLCV = ["Open", "High", "Low", "Close", "Volume"]


class FakeTI:
    """Just enough task_instance to satisfy xcom_pull."""

    def __init__(self, values: dict):
        self.values = values

    def xcom_pull(self, task_ids: str):
        return self.values.get(task_ids)


def bar(close: float = 42.0, *, ticker: str | None = None, empty: bool = False) -> pd.DataFrame:
    """One session of OHLCV, in the shape ``yf.download`` returns it.

    ``ticker`` reproduces the MultiIndex (field, ticker) columns yfinance uses even
    for a single symbol — the level the DAG drops before serialising.
    """
    index = pd.DatetimeIndex([] if empty else [pd.Timestamp(SESSION)], name="Date")
    rows = [] if empty else [[close - 1, close + 1, close - 2, close, 1_000_000]]
    frame = pd.DataFrame(rows, index=index, columns=OHLCV)
    if ticker:
        frame.columns = pd.MultiIndex.from_product([OHLCV, [ticker]], names=["Price", "Ticker"])
    return frame


def context(*, params: dict | None = None, xcoms: dict | None = None) -> dict:
    """A task context with the params the trigger form would have supplied."""
    form = {"ticker": None, "name": "", "currency": "", "dry_run": False}
    return {"params": {**form, **(params or {})}, "ti": FakeTI(xcoms or {})}


@pytest.fixture(autouse=True)
def config(monkeypatch):
    monkeypatch.setattr(sgo, "get_config", lambda: dict(CONFIG))


# ------------------------------------------------------------------- stock_settings


def test_a_scheduled_run_reports_on_every_configured_stock():
    """No form, so no ticker: this is the path every scheduled and catchup run takes."""
    assert sgo.stock_settings(context()) == STOCKS


def test_the_configured_order_is_preserved():
    """The emails arrive in list order, so the list is the running order of the report
    and reordering the secret is how you reorder them — not an implementation detail."""
    assert [s["ticker"] for s in sgo.stock_settings(context())] == ["SGO.PA", "AI.PA", "MC.PA"]


def test_a_config_without_the_key_falls_back_to_one_flat_stock(monkeypatch):
    """The regression this file exists for.

    DEFAULT_STOCK is a list, and wrapping it again would hand every task a list whose
    single element is a list. Nothing raises: yfinance takes the unhashable-looking
    argument, returns an empty frame, and the email says there was no session data.
    """
    monkeypatch.setattr(sgo, "get_config", lambda: {"email_to": "someone@example.com"})
    stocks = sgo.stock_settings(context())
    assert stocks == sgo.DEFAULT_STOCK
    assert all(isinstance(stock, dict) for stock in stocks)


def test_a_config_still_carrying_the_old_singular_key_reports_the_default(monkeypatch):
    """``stock`` was the key before this DAG grew a list, and a stale ice_config can
    still be out there. It reports the one default stock rather than nothing at all,
    which is what lets the DAG deploy and the terraform apply land in either order."""
    old = {"email_to": "someone@example.com", "stock": {"ticker": "SGO.PA", "name": "", "currency": "€"}}
    monkeypatch.setattr(sgo, "get_config", lambda: old)
    assert sgo.stock_settings(context()) == sgo.DEFAULT_STOCK


def test_an_empty_list_is_not_an_empty_report(monkeypatch):
    """``stocks = []`` in the secret is a mistake, not an instruction to email nobody —
    and a run that reports nothing looks exactly like a run that worked."""
    monkeypatch.setattr(sgo, "get_config", lambda: {"email_to": "a@b.c", "stocks": []})
    assert sgo.stock_settings(context()) == sgo.DEFAULT_STOCK


def test_a_typed_ticker_replaces_the_configured_stocks_rather_than_joining_them():
    """The form is taken whole: a manual run about one symbol is about that symbol,
    not about it plus the three the schedule already covers."""
    stocks = sgo.stock_settings(context(params={"ticker": "AAPL", "name": "Apple", "currency": "$"}))
    assert stocks == [{"ticker": "AAPL", "name": "Apple", "currency": "$"}]


def test_a_blank_ticker_means_the_config_not_a_blank_stock():
    """Every scheduled and every catchup run arrives with the empty defaults, so the
    empty string has to read as 'unset' rather than as a symbol."""
    assert sgo.stock_settings(context(params={"ticker": ""})) == STOCKS


# -------------------------------------------------------------------- get_stock_data


@pytest.fixture
def downloads(monkeypatch):
    """Record what yfinance was asked for, and hand back a bar per symbol."""
    calls: list[dict] = []

    def fake_download(ticker, start=None, end=None, **kwargs):
        calls.append({"ticker": ticker, "start": start, "end": end})
        return bar(close=100.0 + len(calls), ticker=ticker)

    monkeypatch.setattr(sgo.yf, "download", fake_download)
    return calls


def test_every_configured_stock_is_fetched_once(downloads):
    bars = sgo.get_stock_data(day=SESSION, **context())
    assert [call["ticker"] for call in downloads] == ["SGO.PA", "AI.PA", "MC.PA"]
    assert set(bars) == {"SGO.PA", "AI.PA", "MC.PA"}


def test_each_bar_is_keyed_by_its_own_ticker(downloads):
    """The pairing the email depends on. Keys, not positions: a frame under the wrong
    key is a price printed under the wrong company, which no assertion downstream of
    the render would catch."""
    bars = sgo.get_stock_data(day=SESSION, **context())
    for ticker, payload in bars.items():
        frame = pd.read_json(sgo.StringIO(payload), orient="split")
        assert not frame.empty
        # Distinct closes per call, so a swapped pair would show up here.
        assert frame["Close"].iloc[-1] == 100.0 + 1 + list(bars).index(ticker)


def test_the_window_is_the_run_s_own_session_not_today(downloads):
    """``day`` comes from data_interval_end, and yfinance's end is exclusive, so one
    session is [day, day + 1). A backfill of a past interval has to refetch that past
    session — this is what makes clearing the task idempotent."""
    sgo.get_stock_data(day="2026-08-13", **context())
    assert downloads[0]["start"].isoformat() == "2026-08-13"
    assert downloads[0]["end"].isoformat() == "2026-08-14"


def test_the_ticker_level_is_dropped_before_serialising(downloads):
    """``to_json`` cannot represent the MultiIndex columns yfinance returns even for a
    single symbol, so the level has to come off first — for every stock, not just the
    first one through the loop."""
    bars = sgo.get_stock_data(day=SESSION, **context())
    for payload in bars.values():
        frame = pd.read_json(sgo.StringIO(payload), orient="split")
        assert list(frame.columns) == OHLCV


def test_a_holiday_is_an_empty_frame_rather_than_a_failure(monkeypatch):
    """A weekend reached by a backfill returns nothing for every symbol. That is a
    normal outcome the renderer has words for, not a reason to fail the task and
    retry it twice."""
    monkeypatch.setattr(sgo.yf, "download", lambda ticker, **kwargs: bar(ticker=ticker, empty=True))
    bars = sgo.get_stock_data(day="2026-08-15", **context())
    assert set(bars) == {"SGO.PA", "AI.PA", "MC.PA"}
    assert all(pd.read_json(sgo.StringIO(p), orient="split").empty for p in bars.values())


def test_a_manual_run_fetches_only_the_typed_symbol(downloads):
    sgo.get_stock_data(day=SESSION, **context(params={"ticker": "AAPL"}))
    assert [call["ticker"] for call in downloads] == ["AAPL"]


# -------------------------------------------------------------- build_and_send_email


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


def payload_for(stocks=STOCKS, closes=(10.0, 20.0, 30.0)) -> dict[str, str]:
    return {
        stock["ticker"]: bar(close=close).to_json(orient="split", date_format="iso")
        for stock, close in zip(stocks, closes, strict=False)
    }


def send(smtp_payload: dict | None = None, **kwargs) -> list[dict]:
    ctx = context(xcoms={sgo.DRAW_TASK_ID: payload_for() if smtp_payload is None else smtp_payload}, **kwargs)
    sgo.build_and_send_email(**ctx)
    return FakeSmtp.sent


def rendered(message: dict) -> tuple[str, str]:
    """The subject and body of a sent message, as text.

    Both come off the wire encoded — the subject RFC 2047 because of the euro sign,
    the body base64 because MIMEText with a utf-8 charset always is — so neither is
    searchable in the raw string.
    """
    parsed = message_from_string(message["msg"])
    subject = str(make_header(decode_header(parsed["Subject"])))
    html = next(part for part in parsed.walk() if part.get_content_type() == "text/html")
    return subject, html.get_payload(decode=True).decode()


def test_one_email_per_stock(smtp):
    assert len(send()) == 3


def test_the_connection_is_opened_once_for_the_whole_report(smtp):
    """Three sends, one handshake. Opening the hook inside the loop would be three
    SMTP logins per run for no reason."""
    send()
    assert smtp.opened == 1


def test_each_email_carries_its_own_stock_s_close(smtp):
    """The end-to-end statement of the pairing: the frame keyed SGO.PA has to reach
    the subject line that names SGO.PA, and its own close has to be in it."""
    subjects = [rendered(message)[0] for message in send()]
    by_ticker = {t: next(s for s in subjects if t in s) for t in ("SGO.PA", "AI.PA", "MC.PA")}
    assert "10.00" in by_ticker["SGO.PA"]
    assert "20.00" in by_ticker["AI.PA"]
    assert "30.00" in by_ticker["MC.PA"]


def test_each_email_is_headed_by_the_stock_s_name(smtp):
    """The name is per-stock config, so three emails must not all say Saint-Gobain."""
    bodies = "".join(rendered(message)[1] for message in send())
    for name in ("Saint-Gobain", "Air Liquide", "LVMH"):
        assert name in bodies


def test_a_stock_missing_from_the_payload_is_reported_rather_than_mispaired(smtp):
    """A ticker added to ice_config between the fetch and the email has no bar on
    XCom. Failing here is the honest outcome — the alternative is a KeyError-free
    render of somebody else's prices under the new name."""
    partial = payload_for(stocks=STOCKS[:2])
    with pytest.raises(KeyError):
        send(partial)


def test_a_dry_run_sends_nothing_at_all(smtp):
    """Not 'sends one fewer' — a layout check must not mail three people three times."""
    assert send(params={"dry_run": True}) == []


def test_a_dry_run_does_not_need_email_to_configured(smtp, monkeypatch):
    """The recipients are resolved lazily so the render path can be exercised against
    a config that has no email_to — the reason ``to`` starts as None."""
    monkeypatch.setattr(sgo, "get_config", lambda: {"stocks": STOCKS})
    assert send(params={"dry_run": True}) == []


def test_a_cleared_email_task_without_its_upstream_says_so(smtp):
    """xcom_pull returns None, and read_json's error for that names neither the task
    nor the cause."""
    from airflow.exceptions import AirflowException

    with pytest.raises(AirflowException, match="no data on XCom"):
        sgo.build_and_send_email(**context(xcoms={}))


def test_every_email_goes_to_the_configured_recipients(smtp):
    for message in send():
        assert message["to"] == ["someone@example.com"]
        assert message["from"] == "ice@example.com"


# ------------------------------------------------------------------------- the wiring


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    from conftest import DAGS_DIR

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False).dags[sgo.DAG_ID]


def test_the_dag_is_two_tasks_in_order(dag):
    assert set(dag.task_ids) == {sgo.DRAW_TASK_ID, "build_and_send_email"}
    assert dag.get_task(sgo.DRAW_TASK_ID).downstream_task_ids == {"build_and_send_email"}


def test_the_day_is_templated_from_the_interval_end(dag):
    """``ds`` is the logical date — the interval's *start*, which for this cron is the
    previous session. Every stock in the loop would be a day stale, and the email
    would look entirely normal."""
    assert dag.get_task(sgo.DRAW_TASK_ID).op_kwargs == {"day": "{{ data_interval_end | ds }}"}


def test_the_form_defaults_are_empty_so_a_scheduled_run_uses_the_config(dag):
    """A default of SGO.PA here would pin every scheduled run to one stock and the
    ``stocks`` list would never be read."""
    assert dag.params["ticker"] in (None, "")
    assert dag.params["name"] == ""
    assert dag.params["dry_run"] is False


# ----------------------------------------------------------------------- create_chart


def hourly(closes=(81.0, 81.3, 81.2), *, day: str = SESSION, tz: str = "Europe/Paris", columns=None):
    """A session of 60-minute bars in the shape ``Ticker.history`` returns them.

    Flat columns and a tz-aware index in the exchange's timezone — the two things that
    differ from ``yf.download``, and the two the chart depends on.
    """
    index = pd.DatetimeIndex(
        [pd.Timestamp(f"{day} {9 + hour}:00", tz=tz) for hour in range(len(closes))], name="Datetime"
    )
    frame = pd.DataFrame(
        [[close - 0.1, close + 0.1, close - 0.2, close, 1_000] for close in closes],
        index=index,
        columns=OHLCV,
    )
    return frame[columns] if columns is not None else frame


class Intraday:
    """The recorded state of a faked ``yf.Ticker(...).history(...)``."""

    def __init__(self):
        self.calls: list[dict] = []
        self.frame = hourly()


@pytest.fixture
def intraday(monkeypatch):
    box = Intraday()

    class FakeTicker:
        def __init__(self, ticker):
            self.ticker = ticker

        def history(self, start=None, end=None, interval=None):
            box.calls.append({"ticker": self.ticker, "start": start, "end": end, "interval": interval})
            return box.frame

    def no_download(*args, **kwargs):
        raise AssertionError("the chart must not go through yf.download — it normalises to UTC")

    monkeypatch.setattr(sgo.yf, "Ticker", FakeTicker)
    monkeypatch.setattr(sgo.yf, "download", no_download)
    return box


def test_the_chart_is_a_png_in_the_directory_it_was_handed(intraday, tmp_path):
    path = sgo.create_chart(SESSION, STOCKS[0], str(tmp_path))
    assert Path(path).parent == tmp_path
    # The magic number, not the extension: build_message attaches this by content.
    assert Path(path).read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"


def test_the_file_is_named_for_the_symbol_and_the_session(intraday, tmp_path):
    """Several stocks share one tempdir per run. Named for the ticker alone, the third
    email would carry the first company's chart — and nothing would raise."""
    for stock in STOCKS:
        sgo.create_chart(SESSION, stock, str(tmp_path))
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        f"AI.PA-{SESSION}.png",
        f"MC.PA-{SESSION}.png",
        f"SGO.PA-{SESSION}.png",
    ]


def test_the_window_is_the_run_s_own_session_at_hourly_resolution(intraday, tmp_path):
    """Same window as the bar the email is built around, so a cleared or backfilled
    task redraws its own session rather than this morning's."""
    sgo.create_chart("2026-08-13", STOCKS[0], str(tmp_path))
    call = intraday.calls[0]
    assert call["start"].isoformat() == "2026-08-13"
    assert call["end"].isoformat() == "2026-08-14"
    assert call["interval"] == sgo.CHART_INTERVAL


def test_the_clock_on_the_axis_is_the_exchange_s_and_not_utc(intraday, tmp_path, monkeypatch):
    """The regression that makes a correct chart look wrong: matplotlib renders
    tz-aware stamps in rcParams["timezone"] — UTC — unless the formatter is told
    otherwise, which would label the 09:00 Paris open as 07:00."""
    seen = {}
    formatter = sgo.mdates.DateFormatter

    def spy(fmt, tz=None):
        seen["tz"] = tz
        return formatter(fmt, tz=tz)

    monkeypatch.setattr(sgo.mdates, "DateFormatter", spy)
    sgo.create_chart(SESSION, STOCKS[0], str(tmp_path))
    assert str(seen["tz"]) == "Europe/Paris"


def test_a_holiday_draws_nothing_and_leaves_no_file(intraday, tmp_path):
    """A weekend a backfill walked into, or a run before the open. The email says so
    in words; a chart of nothing would be an empty pair of axes."""
    intraday.frame = hourly(closes=())
    assert sgo.create_chart("2026-08-15", STOCKS[0], str(tmp_path)) is None
    assert list(tmp_path.iterdir()) == []


def test_a_frame_that_came_back_without_closes_is_not_a_chart(intraday, tmp_path):
    """yfinance has changed its columns between minor versions before. A KeyError here
    would fail the email task twice over on retries."""
    intraday.frame = hourly(columns=["Open", "High", "Low", "Volume"])
    assert sgo.create_chart(SESSION, STOCKS[0], str(tmp_path)) is None


def test_a_ticker_typed_into_the_form_cannot_write_outside_the_directory(intraday, tmp_path):
    """``ticker`` is free text on the trigger form and it lands in a filename."""
    hostile = {"ticker": "../../etc/passwd", "name": "", "currency": ""}
    path = sgo.create_chart(SESSION, hostile, str(tmp_path))
    assert Path(path).parent == tmp_path
    assert list(tmp_path.iterdir()) == [Path(path)]


# ------------------------------------------------------- the chart inside the email

# A real 1×1 PNG: MIMEImage sniffs the bytes to pick its subtype, so a placeholder
# string would be attached as application/octet-stream and nothing would say so.
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


@pytest.fixture
def charts(monkeypatch):
    """Stand in for create_chart, which has its own tests above and a network call."""
    made: list[dict] = []

    def fake_chart(day, stock, directory):
        path = Path(directory) / f"{stock['ticker']}-{day}.png"
        path.write_bytes(PNG)
        made.append({"day": day, "ticker": stock["ticker"], "path": path})
        return str(path)

    monkeypatch.setattr(sgo, "create_chart", fake_chart)
    return made


def images(message: dict) -> list:
    parsed = message_from_string(message["msg"])
    return [part for part in parsed.walk() if part.get_content_type() == "image/png"]


def test_every_email_carries_exactly_one_chart(smtp, charts):
    assert [len(images(message)) for message in send()] == [1, 1, 1]


def test_the_body_and_the_part_agree_on_the_content_id(smtp, charts):
    """The angle brackets go on the header and not on the cid: reference. Get that
    asymmetry wrong and the picture arrives as a second attachment plus a broken
    image, which no assertion about 'an image is present' would catch."""
    for message in send():
        body = rendered(message)[1]
        cid = images(message)[0]["Content-ID"]
        assert cid.startswith("<") and cid.endswith(">")
        assert f'src="cid:{cid[1:-1]}"' in body


def test_each_stock_gets_its_own_chart_rather_than_the_first_one_three_times(smtp, charts):
    """The same pairing the prices have, for the same reason: three emails from one
    run share a tempdir and a Content-ID namespace."""
    send()
    assert [chart["ticker"] for chart in charts] == ["SGO.PA", "AI.PA", "MC.PA"]
    cids = [images(message)[0]["Content-ID"] for message in FakeSmtp.sent]
    assert len(set(cids)) == 3


def test_the_chart_is_of_the_session_on_xcom_not_of_today(smtp, charts):
    """A backfill renders a past session's prices; a chart drawn for today beside them
    would be wrong in a way that looks entirely normal."""
    send()
    assert {chart["day"] for chart in charts} == {SESSION}


def test_a_session_with_no_chart_still_sends_its_email(smtp, monkeypatch):
    """No intraday bars is the ordinary weekend outcome. The prices are the report."""
    monkeypatch.setattr(sgo, "create_chart", lambda day, stock, directory: None)
    sent = send()
    assert len(sent) == 3
    assert all(images(message) == [] for message in sent)
    assert all("cid:" not in rendered(message)[1] for message in sent)


def test_a_chart_that_blows_up_does_not_take_the_email_with_it(smtp, monkeypatch):
    """It is a second Yahoo call on the send path, after the prices are already in
    hand — losing the whole report to it would be a bad trade."""

    def explode(day, stock, directory):
        raise RuntimeError("yahoo said no")

    monkeypatch.setattr(sgo, "create_chart", explode)
    sent = send()
    assert len(sent) == 3
    assert all(images(message) == [] for message in sent)


def test_an_empty_frame_is_never_charted(smtp, charts):
    """A holiday reaches this task as an empty frame for every symbol. Nothing should
    call Yahoo again to draw it."""
    empty = {stock["ticker"]: bar(empty=True).to_json(orient="split") for stock in STOCKS}
    assert len(send(empty)) == 3
    assert charts == []


def test_a_dry_run_still_draws_the_chart_it_is_not_sending(smtp, charts):
    """The point of a dry run is checking the layout, and the chart is the layout."""
    assert send(params={"dry_run": True}) == []
    assert len(charts) == 3


def test_the_charts_do_not_outlive_the_task(smtp, charts):
    """A tempdir per run, cleaned up on the way out — the worker's disk is not a
    cache, and a PNG per stock per run adds up."""
    send()
    assert charts and not any(chart["path"].exists() for chart in charts)
