"""Send out a report of a stock to the configured recipients"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from datetime import date, timedelta
from io import StringIO

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import pendulum
import yfinance as yf
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from common.email_render import build_message, recipients, render_stock, stock_subject

matplotlib.use("Agg")  # the worker has no display; must be set before a figure is made

LOG = logging.getLogger(__name__)

DAG_ID = "stock"
CONFIG_VARIABLE = "ice_config"

DRAW_TASK_ID = "draw_stock"

# yfinance spells the hourly bar "60m". "1h" is accepted as well, but 60m is the
# documented value and the one its own error message lists.
CHART_INTERVAL = "60m"

# What a run reports on when nothing else says otherwise: the fallback for an
# ice_config deployed before the "stocks" key existed, so this DAG runs against the
# configuration that is already out there without a terraform apply first. Same shape
# as the config value — a list, even at one entry — so the two are interchangeable.
DEFAULT_STOCK = [{"ticker": "SGO.PA", "name": "Saint-Gobain", "currency": "€"}]


def get_config() -> dict:
    """Runtime configuration, resolved from Secrets Manager via the Airflow secrets backend.

    Read inside tasks rather than at module scope — a module-level ``Variable.get``
    hits the secrets backend on every DAG-file parse, which the scheduler does
    constantly.
    """
    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def stock_settings(context) -> list[dict]:
    """Which stocks this run reports on — always a list, sometimes of one.

    Two layers, and a run takes one of them whole rather than mixing them. A ticker
    typed into the trigger form means the form is the answer, for that one stock —
    including a blank name, which the renderer turns into the symbol itself. No
    ticker, which is every scheduled and every catchup run since neither is given a
    form, means the stocks in ice_config.

    The config is read here rather than as the params' defaults because those are
    evaluated when the DAG file is parsed, and the scheduler parses it constantly — a
    Variable.get there would be a Secrets Manager call every few seconds, which is the
    thing get_config above exists to avoid.
    """

    params = context["params"]
    if params.get("ticker"):
        return [
            {
                "ticker": params["ticker"],
                "name": params.get("name") or "",
                "currency": params.get("currency") or "",
            }
        ]

    # Read whole, and nothing fancy on top of it: the list in the secret is the list
    # the run reports on. DEFAULT_STOCK covers a config that predates the key — one
    # deployed with the old singular ``stock`` reports that one stock rather than
    # nothing, so this DAG does not need a terraform apply to go first.
    return get_config().get("stocks") or DEFAULT_STOCK


# ---------------------------------------------------------------------------- tasks


def get_stock_data(day: str, **context) -> dict[str, str]:
    """Download each stock's OHLCV bar for ``day`` and return them keyed by ticker.

    The stocks come from ``stock_settings`` rather than from a templated op_kwarg,
    because resolving them needs the config and a template cannot reach that.

    ``day`` is an ISO date rendered from the run's data interval, not ``date.today()``:
    the bars a run fetches have to be a function of the run, so that clearing a task or
    backfilling a past interval re-fetches the same session rather than whatever today
    happens to be.

    yfinance treats ``end`` as exclusive, so [day, day + 1 day) covers exactly one
    session. An empty frame is a normal result — an exchange holiday, a weekend
    reached by a backfill, or a run that landed before the close — and the renderer
    says so rather than failing.

    The frames are serialised rather than returned as objects because XCom cannot
    carry a DataFrame: the default backend is JSON and there is no encoder for one.
    Keyed by ticker rather than a list so the email pairs a frame with its stock by
    name: a symbol that vanishes from the config between the two tasks then just
    goes unreported, instead of shifting every frame after it onto the wrong stock.
    """
    session = date.fromisoformat(day)
    bars = {}

    for stock in stock_settings(context):
        frame = yf.download(stock["ticker"], start=session, end=session + timedelta(days=1))
        if frame.columns.nlevels > 1:
            frame.columns = frame.columns.droplevel("Ticker")
        bars[stock["ticker"]] = frame.to_json(orient="split", date_format="iso")

    return bars


def create_chart(day: str, stock: dict, directory: str) -> str | None:
    """Draw the session's hourly closes as a PNG and return its path, or None if empty.

    Ticker.history rather than the yf.download above: it keeps the intraday index in the
    exchange's timezone, so an SGO.PA chart is labelled in Paris hours and not UTC.
    An empty frame is ordinary — a holiday, a weekend, a run before the open — and Yahoo
    only serves 60m bars for the last 730 days, so a deep backfill has none.
    """
    session = date.fromisoformat(day)
    frame = yf.Ticker(stock["ticker"]).history(
        start=session, end=session + timedelta(days=1), interval=CHART_INTERVAL
    )
    if frame.empty or "Close" not in frame.columns:
        LOG.info("no %s bars for %s on %s — no chart", CHART_INTERVAL, stock["ticker"], day)
        return None

    closes = frame["Close"]
    # The green and red of the email's change badge, so the two never disagree.
    colour = "#157f3d" if closes.iloc[-1] >= closes.iloc[0] else "#c0392b"

    figure, axes = plt.subplots(figsize=(7.23, 2.6), dpi=140)
    axes.plot(closes.index, closes.to_numpy(), color=colour, linewidth=1.6)
    axes.axhline(closes.iloc[0], color="#999999", linewidth=0.8, linestyle=(0, (4, 4)))
    # tz explicitly: matplotlib renders tz-aware stamps in rcParams["timezone"] (UTC)
    # otherwise, which would put the Paris open at 07:00.
    axes.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=closes.index.tz))
    axes.grid(axis="y", color="#eeeeee", linewidth=0.8)
    axes.set_axisbelow(True)
    axes.tick_params(colors="#666666", labelsize=8, length=0)
    for side in ("top", "right", "left"):
        axes.spines[side].set_visible(False)
    axes.spines["bottom"].set_color("#dddddd")
    axes.set_ylabel(f"Close {stock.get('currency') or ''}".strip(), fontsize=9)
    axes.set_title(f"{stock.get('name') or stock['ticker']} — {day}", fontsize=11, loc="left")

    # The symbol is in the name so several stocks in one directory do not overwrite each
    # other, and scrubbed because a ticker can come from the trigger form.
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", stock["ticker"])
    path = os.path.join(directory, f"{stem}-{day}.png")
    figure.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(figure)  # pyplot holds every figure until closed, and workers are long-lived
    return path


def chart_for(frame: pd.DataFrame, stock: dict, directory: str) -> str | None:
    """``create_chart`` for the session already on XCom, or None if there is no chart.

    The day comes out of the frame rather than out of the run's interval, so the
    picture and the numbers under it are always the same session — the frame is what
    the email is actually about. An empty frame has no session to draw, and the body
    for one is the short "no price data" note anyway.

    A chart is the one part of this email that is allowed to go missing: it is a second
    Yahoo call on the send path, and losing the whole report to it would be a bad
    trade. The prices are already in hand by this point.
    """
    if frame.empty:
        return None
    try:
        return create_chart(str(frame.index[-1])[:10], stock, directory)
    except Exception:  # noqa: BLE001 - the email is worth more than the picture
        LOG.warning("could not chart %s — sending without it", stock["ticker"], exc_info=True)
        return None


def build_and_send_email(**context) -> None:
    """Render the session's bar and mail it.

    One day of OHLCV is a few hundred bytes of JSON, so it rides on XCom
    comfortably. A longer history would belong in S3 instead, the way
    ``gdalinfo_notify`` hands its report between tasks.

    The intraday chart is drawn here rather than upstream for the same reason: a PNG
    is far too big for XCom, and the tempdir it lives in only has to outlast the
    ``sendmail`` call that reads it.
    """
    from airflow.providers.smtp.hooks.smtp import SmtpHook

    payload = context["ti"].xcom_pull(task_ids=DRAW_TASK_ID)
    if payload is None:
        # Reachable by clearing this task on its own without the one upstream. Saying
        # so here beats the error read_json would raise on None.
        raise AirflowException(f"no data on XCom from {DRAW_TASK_ID} — was it cleared or skipped?")

    to = None  # resolved lazily: a dry run should not need email_to to be configured

    # The stocks are resolved a second time rather than handed over on XCom. It is one
    # more Secrets Manager read per run, and it keeps this task runnable on its own —
    # clearing it to re-send an email does not depend on what the fetch decided. The
    # bars are looked up by ticker, so a stock added to the config in between is simply
    # missing from the payload rather than paired with someone else's prices.
    #
    # Context manager rather than a bare constructor: SmtpHook only opens its
    # connection in __enter__, and smtp_client is None until it has.
    with SmtpHook() as smtp, tempfile.TemporaryDirectory(prefix="stock-chart-") as workdir:
        for stock in stock_settings(context):
            # StringIO rather than the bare string: pandas 2.1 deprecated passing JSON
            # literals to read_json and 3.0 removes it.
            frame = pd.read_json(StringIO(payload[stock["ticker"]]), orient="split")
            currency = stock.get("currency", "")
            subject = stock_subject(frame, stock["ticker"], currency)

            chart = chart_for(frame, stock, workdir)
            # The ticker and the session are both in the Content-ID so three emails from
            # one run cannot collide on it, and so a threaded reply cannot resolve
            # yesterday's cid against today's picture.
            cid = f"chart.{stock['ticker']}.{str(frame.index[-1])[:10]}" if chart else None
            body = render_stock(
                frame,
                stock["ticker"],
                currency=currency,
                name=stock.get("name"),
                chart_cid=cid,
            )

            if context["params"]["dry_run"]:
                # Everything up to the send still ran, which is the point: a layout
                # change can be checked from one manual run without mailing anyone. The
                # body goes to the log rather than nowhere, so there is something to
                # look at afterwards.
                LOG.info("dry run — not sending. subject: %s", subject)
                LOG.info("dry run — chart: %s", chart or "none")
                LOG.info("dry run — body follows:\n%s", body)
                continue

            if to is None:
                to = recipients(get_config()["email_to"])
            message = build_message(
                mail_from=smtp.from_email,
                to=to,
                subject=subject,
                html_content=body,
                inline_images={cid: chart} if chart else None,
            )
            smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())
            LOG.info("emailed %d bar(s) of %s to %s", len(frame), stock["ticker"], ", ".join(to))


with DAG(
    dag_id=DAG_ID,
    description="Email the latest stock price when the market closes (ca40c)",
    schedule="0 13 * * MON-FRI",
    start_date=pendulum.datetime(2026, 8, 17, tz="America/New_York"),
    # Temporary, to watch the scheduler materialise one run per past weekday and to
    # exercise wait_for_downstream below. The start_date is kept about a week back on
    # purpose: every backfilled run sends a real email, so the window bounds how many.
    catchup=True,
    max_active_runs=1,
    # A run that has not sent its emails in ten minutes is not slow, it is stuck, so
    # fail it rather than let it hold the max_active_runs=1 slot.
    dagrun_timeout=timedelta(minutes=10),  # kills the whole run
    # "Trigger DAG w/ config" renders these as a form, and a filled-in ticker overrides
    # the configured stocks for that one run — one stock, which is what a one-off
    # manual run wants. The defaults are deliberately empty rather than SGO.PA: empty is
    # what says "use ice_config", and it is what every scheduled and every catchup run
    # gets, since neither is given a form to fill in.
    params={
        "ticker": Param(None, type=["string", "null"], description="Blank uses the configured stocks"),
        "name": Param("", type="string", description="Heading — blank uses the symbol"),
        "currency": Param("", type="string", description="Shown after every price"),
        "dry_run": Param(False, type="boolean", description="Render and log the email, send nothing"),
    },
    default_args={
        # Per task, not per run: a single task still hung after a minute is stuck on
        # the network, and killing it leaves the retries below room inside the
        # ten-minute dagrun_timeout above.
        "execution_timeout": timedelta(minutes=1),  # this creates a task timeout after 1 minute
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["stock", "email"],
) as dag:
    t_stock = PythonOperator(
        task_id=DRAW_TASK_ID,
        # Also temporary. A day's bar does not build on the day before it, so this is
        # not a dependency the report actually has — it is here to make the coupling
        # visible during a backfill: fail one run's draw_stock and every later run
        # stops. Airflow implies depends_on_past from this, so that is not set twice.
        # On the operator rather than in default_args so t_email is left alone.
        wait_for_downstream=True,
        python_callable=get_stock_data,
        # data_interval_end, not ds: with a cron schedule ds is the logical date, which
        # is the interval's *start* — for 0 13 * * MON-FRI that is the previous session,
        # so ds would fetch the wrong bar. The end of the interval is the close this run
        # fires at. It is a UTC timestamp (13:00 New York is 17:00 or 18:00 UTC), so the
        # calendar day it renders to is still the session's own.
        op_kwargs={"day": "{{ data_interval_end | ds }}"},
    )
    t_email = PythonOperator(task_id="build_and_send_email", python_callable=build_and_send_email)

    t_stock >> t_email
