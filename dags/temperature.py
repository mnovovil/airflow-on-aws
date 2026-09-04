"""Send out a temperature report. Daily at 9am"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from datetime import date, datetime, time, timedelta
from io import StringIO

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import meteostat as ms
import pandas as pd
import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from common.email_render import build_message, recipients, render_temperature, temperature_subject

matplotlib.use("Agg")  # the worker has no display; must be set before a figure is made

LOG = logging.getLogger(__name__)

# Aptos and Arial are on the desktop this chart was designed on, not on the worker,
# which falls through to the DejaVu Sans that ships with matplotlib. Its resolver logs
# a warning per missing family per piece of text while doing it — thirty-odd lines of
# noise around every chart — and the fallback is deliberate, so it is not news.
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

DAG_ID = "temperature"
CONFIG_VARIABLE = "ice_config"

DRAW_TASK_ID = "draw_temperature"

# Meteostat stamps its data in UTC unless it is told otherwise, and a "day" of
# temperatures that runs 20:00 to 20:00 local is nobody's day. The station's own zone
# is what the chart's hours and the report's date both mean, so it travels with the
# station rather than being applied at the end.
DEFAULT_TIMEZONE = "America/New_York"

# What a run reports on when nothing else says otherwise: the fallback for an
# ice_config deployed before the "temperature" key existed, so this DAG runs without a
# terraform apply first. Same shape as the config value — a list, even at one entry —
# so the two are interchangeable.
DEFAULT_STATION = [{"id": "72503", "name": "LaGuardia Airport", "timezone": DEFAULT_TIMEZONE}]


def get_config() -> dict:
    """Runtime configuration, resolved from Secrets Manager via the Airflow secrets backend.

    Read inside tasks rather than at module scope — a module-level ``Variable.get``
    hits the secrets backend on every DAG-file parse, which the scheduler does
    constantly.
    """
    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def station_settings(context) -> list[dict]:
    """Which weather stations this run reports on — always a list, sometimes of one.

    Two layers, and a run takes one of them whole rather than mixing them. A station ID
    typed into the trigger form means the form is the answer, for that one station. No
    ID, which is every scheduled and every catchup run since neither is given a form,
    means the stations in ice_config.

    The config is read here rather than as the params' defaults because those are
    evaluated when the DAG file is parsed, and the scheduler parses it constantly — a
    Variable.get there would be a Secrets Manager call every few seconds, which is the
    thing get_config above exists to avoid.
    """
    params = context["params"]
    # ``station``, the key the form actually defines. The stations are dicts keyed by
    # ``id`` because that is what every task downstream looks them up by — an entry
    # keyed anything else goes missing without raising.
    if params.get("station"):
        return [
            {
                "id": params["station"],
                "name": params.get("name") or "",
                "timezone": params.get("timezone") or DEFAULT_TIMEZONE,
            }
        ]

    return get_config().get("temperature") or DEFAULT_STATION


def station_timezone(station: dict) -> str:
    return station.get("timezone") or DEFAULT_TIMEZONE


# ---------------------------------------------------------------------------- tasks


def get_temperature_data(day: str, **context) -> dict[str, str]:
    """Fetch each station's hourly temperatures for ``day`` and return them keyed by ID.

    ``day`` is an ISO date rendered from the run's data interval, not ``date.today()``:
    the observations a run fetches have to be a function of the run, so that clearing a
    task or backfilling a past interval re-fetches the same day rather than whatever
    today happens to be.

    Three things about Meteostat that the obvious call gets wrong. Its bounds are
    *inclusive* and want datetimes, so ``[day 00:00, day 23:59:59]`` is the one day
    asked for while ``day + 1`` would quietly fetch two. Its stamps are UTC unless a
    timezone is named, which would put the small hours of the next day on the chart and
    label them wrong. And ``fetch`` returns an ordinary empty frame for a station that
    reported nothing, which is a normal outcome rather than an error — a gap in the
    record, or a run that landed before the first observation of the day.

    The series are serialised rather than returned as objects because XCom cannot carry
    a pandas object: the default backend is JSON and there is no encoder for one. A
    day of hourly temperatures is a couple of kilobytes, so the whole series rides
    there and nothing downstream has to call Meteostat again. Keyed by station ID
    rather than a list so the email pairs a series with its station by name: a station
    that vanishes from the config between the two tasks then just goes unreported,
    instead of shifting every series after it onto the wrong station.
    """
    session = date.fromisoformat(day)
    observations = {}

    for station in station_settings(context):
        frame = ms.Hourly(
            station["id"],
            datetime.combine(session, time.min),
            datetime.combine(session, time.max),
            timezone=station_timezone(station),
        ).fetch()

        # An empty frame still has to produce an empty *series*, not a KeyError: the
        # column is only there when there was something to put in it.
        series = frame["temp"].dropna() if "temp" in frame.columns else pd.Series(dtype="float64")
        # Local wall time, with the offset dropped. The stamps mean the same thing
        # either way, but a tz-aware index survives the JSON round trip below as UTC,
        # which would relabel every hour on the chart.
        if isinstance(series.index, pd.DatetimeIndex) and series.index.tz is not None:
            series.index = series.index.tz_localize(None)

        LOG.info("%s: %d hourly observation(s) for %s", station["id"], len(series), day)
        observations[station["id"]] = series.to_json(orient="split", date_format="iso")

    return observations


def create_chart(series: pd.Series, station: dict, directory: str) -> str | None:
    """Draw the day's hourly temperatures as a PNG and return its path, or None if empty.

    Drawn from the series already on XCom rather than from a second Meteostat call —
    the observations are small enough to ride there whole, which is the difference from
    the stock DAG, where the chart needs bars the fetch task never had.

    A day with no observations has nothing to draw, and the email for one is the short
    note saying so.
    """
    if series.empty:
        LOG.info("no observations for %s — no chart", station["id"])
        return None

    name = station.get("name") or station["id"]
    # The day comes out of the series rather than out of the run's interval, so the
    # picture and the file it is written to are always the day the data is from.
    day = str(series.index[0])[:10]

    # Aptos is what this report is drawn in on a desktop; the worker has neither it nor
    # Arial and falls through to DejaVu Sans, which ships with matplotlib. Named in full
    # so the fallback is a decision rather than whatever the box happens to have.
    plt.rcParams["font.family"] = ["Aptos", "Arial", "DejaVu Sans"]
    figure, axes = plt.subplots(figsize=(11, 6), dpi=120)

    axes.fill_between(series.index, series.min() - 1, series, color="#06B0F0", alpha=0.08)
    axes.plot(series.index, series, color="#06B0F0", lw=2.6)
    axes.annotate(
        f"{series.iloc[-1]:.1f}°C",
        (series.index[-1], series.iloc[-1]),
        xytext=(8, 0),
        textcoords="offset points",
        va="center",
        fontsize=12,
        fontweight="bold",
        color="#06B0F0",
    )
    # The fill starts at min - 1, so the axis does too — otherwise matplotlib pads the
    # limits and the band floats above the baseline instead of sitting on it.
    axes.set_ylim(bottom=series.min() - 1)

    axes.set_title(
        f"Temperature swung {series.max() - series.min():.1f}°C across the day at {name}",
        loc="left",
        fontsize=17,
        fontweight="bold",
        color="#0E2841",
        pad=24,
    )
    axes.text(
        0,
        1.03,
        f"Hourly observations, {series.index[0]:%d %B %Y} (°C) — Source: Meteostat / station {station['id']}",
        transform=axes.transAxes,
        fontsize=11,
        color="#808080",
    )

    # No tz= needed the way the stock chart needs it: the stamps were localised before
    # they went onto XCom, so what is being formatted is already the station's own time.
    axes.xaxis.set_major_formatter(mdates.DateFormatter("%-I %p"))
    axes.yaxis.set_major_formatter(lambda value, _: f"{value:.0f}°C")
    axes.grid(axis="y", color="#C0C0C0", lw=0.7, alpha=0.6)
    axes.set_axisbelow(True)
    axes.spines[["top", "right", "left"]].set_visible(False)
    axes.spines["bottom"].set_color("#C0C0C0")
    axes.tick_params(length=0, colors="#808080", labelsize=11)

    # The station is in the name so several of them in one directory do not overwrite
    # each other, and scrubbed because an ID can come from the trigger form.
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", station["id"])
    path = os.path.join(directory, f"{stem}-{day}.png")
    # bbox_inches so the annotation hanging off the right edge is not cropped, and an
    # explicit white face because the default is transparent — which mail clients
    # render against whatever the reading pane happens to be, dark themes included.
    figure.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(figure)  # pyplot holds every figure until closed, and workers are long-lived
    return path


def chart_for(series: pd.Series, station: dict, directory: str) -> str | None:
    """``create_chart``, but never at the cost of the email.

    A picture that fails to draw is worth less than the numbers under it, which are
    already in hand by this point — so a broken chart is logged and the report goes out
    without it.

    The empty check is here as well as inside ``create_chart`` on purpose: this is the
    one that keeps the caller from asking an empty series for the day it covers.
    """
    if series.empty:
        return None
    try:
        return create_chart(series, station, directory)
    except Exception:  # noqa: BLE001 - the email is worth more than the picture
        LOG.warning("could not chart %s — sending without it", station["id"], exc_info=True)
        return None


def build_and_send_email(**context) -> None:
    """Render each station's day and mail it.

    The chart is drawn here rather than upstream because a PNG is far too big for XCom,
    and the tempdir it lives in only has to outlast the ``sendmail`` call that reads it.

    The stations are resolved a second time rather than handed over on XCom. It is one
    more Secrets Manager read per run, and it keeps this task runnable on its own —
    clearing it to re-send an email does not depend on what the fetch decided. The
    series are looked up by ID, so a station added to the config in between is reported
    as missing rather than paired with someone else's temperatures.
    """
    from airflow.providers.smtp.hooks.smtp import SmtpHook

    payload = context["ti"].xcom_pull(task_ids=DRAW_TASK_ID)
    if payload is None:
        # Reachable by clearing this task on its own without the one upstream. Saying
        # so here beats the error read_json would raise on None.
        raise AirflowException(f"no data on XCom from {DRAW_TASK_ID} — was it cleared or skipped?")

    to = recipients(get_config()["email_to"])

    # Context manager rather than a bare constructor: SmtpHook only opens its
    # connection in __enter__, and smtp_client is None until it has.
    with SmtpHook() as smtp, tempfile.TemporaryDirectory(prefix="temperature-chart-") as workdir:
        for station in station_settings(context):
            serialised = payload.get(station["id"])
            if serialised is None:
                LOG.warning("%s is not in the payload — configured after the fetch?", station["id"])
                continue

            # StringIO rather than the bare string: pandas 2.1 deprecated passing JSON
            # literals to read_json and 3.0 removes it. typ="series" because that is
            # what went in — the default would rebuild it as a one-column frame.
            series = pd.read_json(StringIO(serialised), orient="split", typ="series")
            name = station.get("name") or station["id"]

            chart = chart_for(series, station, workdir)
            # The station and the day are both in the Content-ID so several emails from
            # one run cannot collide on it, and so a threaded reply cannot resolve
            # yesterday's cid against today's picture.
            cid = f"chart.{station['id']}.{str(series.index[0])[:10]}" if chart else None
            body = render_temperature(series, name, station_id=station["id"], chart_cid=cid)
            subject = temperature_subject(series, name)

            message = build_message(
                mail_from=smtp.from_email,
                to=to,
                subject=subject,
                html_content=body,
                inline_images={cid: chart} if chart else None,
            )
            smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())
            LOG.info("emailed %d observation(s) for %s to %s", len(series), name, ", ".join(to))


with DAG(
    dag_id=DAG_ID,
    description="Email temperature prediction for the next day",
    schedule="0 08 * * MON-SUN",
    start_date=pendulum.datetime(2026, 8, 24, tz="America/New_York"),
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    params={
        "station": Param(None, type=["string", "null"], description="Blank uses the configured station"),
        "name": Param("", type="string", description="Heading — blank uses the station ID"),
        "timezone": Param("", type="string", description=f"Station's zone — blank is {DEFAULT_TIMEZONE}"),
    },
    default_args={
        "execution_timeout": timedelta(minutes=1),
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["temp", "email"],
) as dag:
    t_temp = PythonOperator(
        task_id=DRAW_TASK_ID,
        wait_for_downstream=True,
        python_callable=get_temperature_data,
        op_kwargs={"day": "{{ data_interval_end | ds }}"},
    )
    t_email = PythonOperator(task_id="build_and_send_email", python_callable=build_and_send_email)

    t_temp >> t_email
