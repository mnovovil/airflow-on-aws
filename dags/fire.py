"""Send out a fire report. Daily at 9am, of the current active wildfires. We define the country"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from datetime import timedelta
from io import StringIO

import boto3
import geopandas as gpd
import matplotlib
import pandas as pd
import pendulum
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator, PythonOperator

from common.email_render import build_message, recipients

matplotlib.use("Agg")  # the worker has no display; must be set before a figure is made

LOG = logging.getLogger(__name__)

# The worker has neither Aptos nor Arial and falls through to DejaVu Sans. Its resolver
# logs a warning per missing family per piece of text while doing it — thirty-odd lines
# around every chart — and the fallback is deliberate, so it is not news.
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)

DAG_ID = "fire"
CONFIG_VARIABLE = "ice_config"

STORE_TASK_ID = "store_fires"
EMAIL_TASK_ID = "build_and_send_email"
NO_DATA_TASK_ID = "email_no_data"

# store_csv_s3 is the DAG's branch callable, so its return value is the task to run next
# rather than its result. The keys it wrote ride on this XCom key instead.
XCOM_WRITTEN = "written"

# Everything this DAG runs on comes from ice_config's "fire" object, which infra/fire.tf
# owns. There are no defaults: a key missing from it is a deploy that did not happen, and
# the task raises rather than running on a guess.
#
# This one is not a fallback — nothing reads it — and is only still here because the
# timezone Param in the DAG below interpolates it into its description.
DEFAULT_TIMEZONE = "Europe/Madrid"


def get_config() -> dict:
    """The ice_config blob from Secrets Manager. This DAG's keys are under ``fire``.

    Read inside tasks, not at module scope: the scheduler re-parses this file constantly
    and a module-level Variable.get would hit the secrets backend every few seconds.
    """
    return Variable.get(CONFIG_VARIABLE, deserialize_json=True)


def country_settings(context) -> list[dict]:
    """The countries this run reports on: the trigger form if it names one, else ice_config.

    Entries are keyed ``country`` (ISO 3166-1 alpha-3) because the fetch looks them up by
    it and the S3 objects are named after it. An entry spelled otherwise is rejected here
    rather than quietly reporting nothing.
    """
    params = context["params"]
    if params.get("country"):
        entry = {"country": params["country"]}
        if params.get("timezone"):
            entry["timezone"] = params["timezone"]
        return [entry]

    countries = (get_config().get("fire") or {}).get("countries")
    if not countries:
        raise AirflowException("ice_config has no fire.countries — see infra/fire.tf")
    unnamed = [entry for entry in countries if not entry.get("country")]
    if unnamed:
        raise AirflowException(f"fire.countries entries with no 'country' code: {unnamed}")
    return countries


def country_timezone(country: dict) -> str | None:
    """The zone a country's report day is measured in; ``None`` if the entry sets none.

    Carried but not read yet — the fetch asks FIRMS for a rolling window of whole UTC
    days rather than a local date range.
    """
    return country.get("timezone")


# ---------------------------------------------------------------------------- tasks


def get_country_geometry(country: str, countries_url: str | None = None) -> tuple:
    """The country's bounding box as ``(west, south, east, north)``, which is what FIRMS takes.

    ``SOV_A3`` is the sovereignty code, so France and its overseas departments share FRA.
    The ``where`` clause is pushed down to OGR, so one row comes back rather than all 177.
    """
    # The code reaches an SQL WHERE clause and can come from the trigger form.
    if not re.fullmatch(r"[A-Za-z]{3}", country or ""):
        raise AirflowException(f"{country!r} is not a three-letter SOV_A3 country code")

    # Resolved here only for a caller that does not already hold it — get_fires_country
    # is the one, and it pays a Secrets Manager read for the convenience.
    if not countries_url:
        countries_url = (get_config().get("fire") or {}).get("countries_url")
        if not countries_url:
            raise AirflowException("ice_config has no fire.countries_url — see infra/fire.tf")

    frame = gpd.read_file(countries_url, where=f"SOV_A3 = '{country.upper()}'")
    if frame.empty:
        raise AirflowException(f"no country in Natural Earth with SOV_A3 = {country.upper()!r}")

    return frame.geometry.iloc[0].bounds


def get_fires_country(map_key, bounds, country, source="VIIRS_NOAA20_NRT", days=2):
    """The country's detections over the last ``days``, as FIRMS' CSV parsed into a frame.

    ``bounds`` is the box to ask for, as ``get_country_geometry`` returns it. Deriving it
    here instead is a second Natural Earth download per country — and a Secrets Manager
    read with it, since this function holds no URL to pass on — for a box the caller
    already has and that cannot have changed between the two calls.

    Falling back rather than requiring it: ``country`` alone is enough to answer with,
    and a caller that only has the code should not have to resolve the box to ask.
    """
    west, south, east, north = bounds or get_country_geometry(country)

    df = pd.read_csv(
        f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{map_key}/{source}"
        f"/{west},{south},{east},{north}/{days}"
    )

    if not df.empty and "latitude" not in df.columns:
        raise AirflowException(f"FIRMS returned a non-CSV body for {country}: {df.columns.tolist()[:3]}")

    return df


def upload_dataframe_s3(df: pd.DataFrame, bucket: str, key: str, s3=None) -> str:
    """Write ``df`` to ``s3://bucket/key`` as CSV and return the key.

    ``s3`` is injectable for tests, and built here rather than defaulted in the
    signature — a default is evaluated at import, on every DAG-file parse.
    """
    buffer = StringIO()
    df.to_csv(buffer, index=False)

    (s3 or boto3.client("s3")).put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode(),
        # S3 assumes octet-stream, which downloads the object rather than previewing it.
        ContentType="text/csv",
    )

    LOG.info("wrote %d detections to s3://%s/%s", len(df), bucket, key)
    return key


def store_csv_s3(**context) -> str:
    """Fetch each country's detections, write them to S3, and pick the branch to follow.

    Returns a task id rather than the keys because this is the DAG's branch callable and
    a branch callable's return value is the decision. The keys ride on the ``written``
    XCom instead, which is what the report task reads.

    A run where no country returned anything goes to the no-data email, so the report
    task only ever has detections in front of it.
    """
    config = get_config()
    fire = config.get("fire") or {}
    bucket = config["artifacts_bucket"]
    ds = context["ds"]

    # Checked together so one run names every missing key rather than one per attempt.
    # A missing map_key would otherwise surface as FIRMS' 401, delivered as an
    # unparseable CSV body several layers down.
    wanted = ("map_key", "countries_url", "prefix", "source", "days")
    missing = [name for name in wanted if not fire.get(name)]
    if missing:
        raise AirflowException(f"ice_config fire is missing {missing} — see infra/fire.tf")

    fire_key = fire["map_key"]
    countries_url = fire["countries_url"]
    prefix = fire["prefix"].strip("/")

    written = []
    for country in country_settings(context):
        code = country["country"]
        # A country's own entry overrides the run-wide fire block.
        source = country.get("source") or fire["source"]
        days = int(country.get("days") or fire["days"])

        # Resolved once, here, and handed down: this is also what validates the code
        # before a FIRMS request is spent on it, so the fetch does not repeat the work.
        bounds = get_country_geometry(code, countries_url)
        try:
            df = get_fires_country(fire_key, bounds, code, source=source, days=days)
        except pd.errors.EmptyDataError:
            # No body at all rather than a header-only one. It means the same thing, but
            # read_csv raises on it instead of returning a frame with no rows.
            LOG.warning("FIRMS returned an empty body for %s — counting it as no data", code)
            continue

        if df.empty:
            # A header and no rows is FIRMS' ordinary "nothing burning" answer, but only
            # when the header is the FIRMS one. An error body ("Invalid MAP_KEY.") parses
            # to a no-row frame as well, and get_fires_country's schema check misses it
            # because that check only looks at frames that have rows.
            if "latitude" not in df.columns:
                raise AirflowException(f"FIRMS returned a non-CSV body for {code}: {df.columns.tolist()[:3]}")
            LOG.info("no detections for %s over the last %s day(s)", code, days)
            continue

        # Source in the path: two instruments over one country on one day are two
        # answers, not one overwriting the other.
        written.append(upload_dataframe_s3(df, bucket, f"{prefix}/{source}/{ds}/{code}.csv"))

    context["ti"].xcom_push(key=XCOM_WRITTEN, value=written)
    if not written:
        LOG.info("no country returned detections for %s", ds)
        return NO_DATA_TASK_ID
    return EMAIL_TASK_ID


def create_chart(
    frame: pd.DataFrame, country: str, day: str, directory: str, countries_url: str
) -> str | None:
    """Map the day's detections onto the country's outline. ``None`` if there is nothing to draw.

    Drawn from the frame the fetch already wrote to S3, not from a second FIRMS call.
    """
    # Imported here, not at module scope: only this task draws, and the scheduler
    # re-parses this file constantly.
    import math

    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    if frame.empty:
        LOG.info("no detections for %s on %s — no chart", country, day)
        return None

    # Fire radiative power in MW, carrying both the colour and the size of a point.
    # Coerced because FIRMS blanks it on a detection it could not quantify.
    power = pd.to_numeric(frame["frp"], errors="coerce").fillna(0.0)
    hottest = power.max()

    # The worker has neither Aptos nor Arial and falls through to DejaVu Sans, which
    # ships with matplotlib. Named in full so the fallback is a decision.
    plt.rcParams["font.family"] = ["Aptos", "Arial", "DejaVu Sans"]
    figure, axes = plt.subplots(figsize=(11, 7), dpi=120)

    # The outline is context, not data: a failed download costs the coastline, not the
    # detections.
    try:
        gpd.read_file(countries_url, where=f"SOV_A3 = '{country}'").plot(
            ax=axes, facecolor="#F7F7F7", edgecolor="#C0C0C0", lw=0.8, zorder=1
        )
    except Exception:  # noqa: BLE001 - the detections are worth more than the outline
        LOG.warning("no outline for %s — mapping the detections alone", country, exc_info=True)

    # YlOrRd starts near-white, which is invisible against the country fill, so the ramp
    # is taken from a quarter of the way in.
    ylorrd = plt.get_cmap("YlOrRd")
    ramp = LinearSegmentedColormap.from_list(
        "firms", [ylorrd(0.25 + 0.75 * step / 255) for step in range(256)]
    )

    points = axes.scatter(
        frame["longitude"],
        frame["latitude"],
        c=power,
        # Area as well as colour; the floor keeps a 0 MW detection visible.
        s=14 + 90 * ((power / hottest) if hottest else 0),
        cmap=ramp,
        vmin=0,
        edgecolor="#7F2704",
        lw=0.3,
        alpha=0.85,
        zorder=3,
    )

    bar = figure.colorbar(points, ax=axes, fraction=0.035, pad=0.02)
    bar.set_label("Fire radiative power (MW)", color="#808080", fontsize=11)
    bar.outline.set_visible(False)
    bar.ax.tick_params(length=0, colors="#808080", labelsize=10)

    # A degree of longitude is cos(latitude) as wide as one of latitude, so equal aspect
    # would stretch the country east to west.
    axes.set_aspect(1 / max(math.cos(math.radians(float(frame["latitude"].mean()))), 0.1))

    axes.set_title(
        f"{len(frame):,} active fire detections across {country}",
        loc="left",
        fontsize=17,
        fontweight="bold",
        color="#0E2841",
        pad=24,
    )
    # Off the frame rather than the config, so the caption describes the rows drawn.
    instrument = " / ".join(sorted(frame["instrument"].dropna().unique())) if "instrument" in frame else ""
    axes.text(
        0,
        1.03,
        f"Hottest {hottest:,.0f} MW — Source: NASA FIRMS{' / ' + instrument if instrument else ''}, {day}",
        transform=axes.transAxes,
        fontsize=11,
        color="#808080",
    )

    axes.xaxis.set_major_formatter(lambda value, _: f"{abs(value):.0f}°{'E' if value >= 0 else 'W'}")
    axes.yaxis.set_major_formatter(lambda value, _: f"{abs(value):.0f}°{'N' if value >= 0 else 'S'}")
    axes.grid(color="#C0C0C0", lw=0.7, alpha=0.5)
    axes.set_axisbelow(True)
    axes.spines[["top", "right", "bottom", "left"]].set_visible(False)
    axes.tick_params(length=0, colors="#808080", labelsize=11)

    # Scrubbed because the code can come from the trigger form.
    stem = re.sub(r"[^A-Za-z0-9._-]", "_", country)
    path = os.path.join(directory, f"{stem}-{day}.png")
    # bbox_inches keeps the colourbar label; white face because the default is
    # transparent, which a dark mail theme renders the chart against.
    figure.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(figure)  # pyplot holds every figure until closed, and workers are long-lived
    return path


def build_and_send_email(**context) -> None:
    """Read each country's detections back out of S3 and mail one report per country.

    Only reached when store_csv_s3 wrote something, so every key here has detections
    behind it — a run that found nothing goes to the no-data email instead. XCom carries
    the keys, not the frames; both they and the chart are far too big for it.
    """
    import html

    from airflow.providers.smtp.hooks.smtp import SmtpHook

    from common.email_render import STYLE

    keys = context["ti"].xcom_pull(task_ids=STORE_TASK_ID, key=XCOM_WRITTEN)
    if not keys:
        # Reachable by clearing this task without the one upstream.
        raise AirflowException(f"no keys on XCom from {STORE_TASK_ID} — was it cleared or skipped?")

    config = get_config()
    bucket = config["artifacts_bucket"]
    to = recipients(config["email_to"])
    countries_url = (config.get("fire") or {}).get("countries_url")
    day = context["ds"]
    s3 = boto3.client("s3")

    footer = "Generated by the <code>fire</code> Airflow DAG.<br>Source: NASA FIRMS."

    # SmtpHook only opens its connection in __enter__; smtp_client is None until it has.
    with SmtpHook() as smtp, tempfile.TemporaryDirectory(prefix="fire-chart-") as workdir:
        for key in keys:
            # Keys are named ``.../{ds}/{code}.csv``, so the stem is the country.
            code = os.path.splitext(os.path.basename(key))[0]
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            frame = pd.read_csv(StringIO(body.decode()))

            if frame.empty:
                # The fetch never writes an empty frame, so this is an object truncated
                # or replaced after the fact rather than a quiet day.
                LOG.warning("%s is empty — skipping rather than mailing a blank report", key)
                continue

            power = pd.to_numeric(frame["frp"], errors="coerce")
            brightness = pd.to_numeric(frame["bright_ti4"], errors="coerce")
            nights = int((frame["daynight"] == "N").sum()) if "daynight" in frame else 0

            chart = None
            try:
                chart = create_chart(frame, code, day, workdir, countries_url)
            except Exception:  # noqa: BLE001 - the email is worth more than the picture
                LOG.warning("could not chart %s — sending without it", code, exc_info=True)

            # Country and day in the Content-ID so emails from one run cannot collide on
            # it, and a threaded reply cannot resolve the wrong day's map.
            cid = f"chart.{code}.{day}" if chart else None
            image = ""
            if cid:
                image = (
                    f'<img src="cid:{html.escape(cid)}" width="723" alt="Active fire detections"\n'
                    '       style="max-width:100%;height:auto;margin-bottom:18px">'
                )

            # Off acq_date rather than the run's day: FIRMS is asked for several days, so
            # the frame's own last date is how current the report is.
            empty = pd.Series(dtype="object")
            dates = frame["acq_date"].dropna().astype(str) if "acq_date" in frame else empty
            window = f"{dates.min()} → {dates.max()}" if not dates.empty else "—"
            sats = frame["satellite"].dropna().unique() if "satellite" in frame else []
            satellites = " / ".join(sorted(sats)) or "—"

            pairs = [
                ("Country", f"<code>{html.escape(code)}</code>"),
                ("Detections", f"{len(frame):,}"),
                ("Window", html.escape(window)),
                ("Hottest", f"{power.max():,.1f} MW"),
                ("Total power", f"{power.sum():,.0f} MW"),
                ("Mean brightness", f"{brightness.mean():,.1f} K"),
                ("Day / night", f"{len(frame) - nights:,} / {nights:,}"),
                ("Satellite", html.escape(satellites)),
                ("Data", f"<code>{html.escape(f's3://{bucket}/{key}')}</code>"),
            ]
            rows = "\n".join(
                f"<tr><th>{html.escape(label)}</th><td>{value}</td></tr>" for label, value in pairs
            )

            content = f"""<html><head><style>{STYLE}</style></head><body>
  <h2>{html.escape(f"Active fires — {code}")}</h2>
  <div class="sub">Active detections as of {html.escape(day)}.</div>
  <p style="margin:0 0 20px">
    <span style="font-size:30px;font-weight:600">{len(frame):,}</span>
    <span style="color:#666;font-size:16px;margin-left:10px">detections, hottest
      {power.max():,.0f}&nbsp;MW</span></p>
  {image}
  <table>{rows}</table>
  <p class="foot">{footer}</p>
</body></html>"""

            message = build_message(
                mail_from=smtp.from_email,
                to=to,
                subject=(
                    f"[fire] {code} {day} — {len(frame):,} detection(s), " f"hottest {power.max():,.0f} MW"
                ),
                html_content=content,
                inline_images={cid: chart} if chart else None,
            )
            smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())
            LOG.info("emailed %d detection(s) for %s to %s", len(frame), code, ", ".join(to))


def build_and_send_no_data_email(**context) -> None:
    """One email saying the run found nothing — the branch store_csv_s3 takes when no
    country returned detections.

    A task of its own rather than a special case inside the report, so that the report
    only ever renders detections it actually has.
    """
    import html

    from airflow.providers.smtp.hooks.smtp import SmtpHook

    from common.email_render import STYLE

    to = recipients(get_config()["email_to"])
    day = context["ds"]
    listed = ", ".join(country["country"] for country in country_settings(context))

    content = f"""<html><head><style>{STYLE}</style></head><body>
  <h2>{html.escape(f"No active fires — {listed}")}</h2>
  <div class="sub">Nothing detected as of {html.escape(day)}.</div>
  <p>FIRMS returned no detections for <code>{html.escape(listed)}</code> over the
     requested window. That is the ordinary outcome outside a fire season, and on any
     run that lands before the day's first overpass has been processed.</p>
  <p class="foot">Generated by the <code>fire</code> Airflow DAG.<br>Source: NASA FIRMS.</p>
</body></html>"""

    with SmtpHook() as smtp:
        message = build_message(
            mail_from=smtp.from_email,
            to=to,
            subject=f"[fire] {listed} {day} — no active detections",
            html_content=content,
        )
        smtp.smtp_client.sendmail(from_addr=smtp.from_email, to_addrs=to, msg=message.as_string())

    LOG.info("emailed the no-data notice for %s to %s", listed, ", ".join(to))


with DAG(
    dag_id=DAG_ID,
    description="Email active fires today in Spain",
    schedule="0 08 * * MON-SUN",
    start_date=pendulum.datetime(2026, 8, 24, tz="America/New_York"),
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    params={
        "country": Param(None, type=["string", "null"], description="Blank uses the configured country"),
        "timezone": Param("", type="string", description=f"Station's zone — blank is {DEFAULT_TIMEZONE}"),
    },
    default_args={
        "execution_timeout": timedelta(minutes=1),
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["fire", "email"],
) as dag:
    # Downloading, writing and choosing the branch are one task because store_csv_s3
    # already fetches through get_fires_country — a separate fetch task would pull every
    # country from FIRMS twice.
    t_store = BranchPythonOperator(
        task_id=STORE_TASK_ID,
        wait_for_downstream=True,
        python_callable=store_csv_s3,
    )
    t_email = PythonOperator(task_id=EMAIL_TASK_ID, python_callable=build_and_send_email)
    t_no_data = PythonOperator(task_id=NO_DATA_TASK_ID, python_callable=build_and_send_no_data_email)

    # Exactly one of these runs: BranchPythonOperator stamps SKIPPED on the direct
    # downstream it did not return.
    t_store >> t_email
    t_store >> t_no_data
