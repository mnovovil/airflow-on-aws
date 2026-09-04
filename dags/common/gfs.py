"""Address a GFS product on NOAA's NOMADS grib filter.

NOMADS does not serve GFS as plain files you can path your way to. ``filter_gfs_0p25.pl``
is a CGI that reads a full global GRIB2 file server-side and streams back only the
records and the geographic window you asked for — which is the difference between a
65 KB download and a 500 MB one.

The URL is built here, on the scheduler, rather than inside the container that fetches
it. Two things need it and they must agree exactly: ``wait_for_cycle`` probes the URL
before the worker is started, and ``gfs_rain.py`` downloads it afterwards. Building it
in one place and passing it through as ``--url`` means there is no second
implementation to drift — the URL that was probed is the URL that is fetched.

Deliberately free of both Airflow and GDAL imports: the scheduler has no GDAL, and
keeping it importable from a bare REPL is what makes the URL easy to check by hand.
"""

from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta

NOMADS_HOST = "nomads.ncep.noaa.gov"
FILTER_URL = f"https://{NOMADS_HOST}/cgi-bin/filter_gfs_0p25.pl"

# Longitudes are degrees east, 0–360, which is how the GFS grid is indexed — 235 is
# 125°W and 295 is 65°W. Latitudes are ordinary signed degrees. Together this is CONUS
# with a little slack on every side.
CONUS = {"leftlon": 235, "rightlon": 295, "toplat": 50, "bottomlat": 24}

# Every GRIB2 message starts with these four bytes. When a cycle has not been published
# the filter answers 200 with an HTML or plain-text apology rather than a 404, so the
# status code alone cannot tell you whether you got data. This can.
GRIB_MAGIC = b"GRIB"

CYCLES = ("00", "06", "12", "18")


def build_url(
    date: str,
    cycle: str = "12",
    fhour: int = 24,
    variable: str = "APCP",
    level: str = "surface",
    bbox: dict[str, int] | None = None,
) -> str:
    """The NOMADS filter URL for one variable, at one level, over one window.

    ``date`` is the UTC date the cycle was initialised on, as ``YYYYMMDD`` — not the
    local date, and not the date the forecast is valid for.
    """
    if cycle not in CYCLES:
        raise ValueError(f"GFS runs at {', '.join(CYCLES)}Z, not {cycle}Z")
    if not (date.isdigit() and len(date) == 8):
        raise ValueError(f"date must be YYYYMMDD, got {date!r}")

    box = bbox or CONUS
    query = [
        ("file", f"gfs.t{cycle}z.pgrb2.0p25.f{fhour:03d}"),
        (f"lev_{level}", "on"),
        (f"var_{variable}", "on"),
        # An empty flag, and its presence is what switches subsetting on at all. Drop
        # it and the four bounds below are ignored and you get the whole globe.
        ("subregion", ""),
        ("leftlon", box["leftlon"]),
        ("rightlon", box["rightlon"]),
        ("toplat", box["toplat"]),
        ("bottomlat", box["bottomlat"]),
        # /atmos is a GFSv16 addition; the sibling directories are /wave and /chem.
        ("dir", f"/gfs.{date}/{cycle}/atmos"),
    ]
    return f"{FILTER_URL}?{urllib.parse.urlencode(query)}"


def window(date: str, cycle: str = "12", fhour: int = 24, accumulation: int | None = None) -> tuple:
    """The (start, end) instants an accumulated product covers, both UTC.

    ``accumulation`` defaults to the whole forecast, which is what f024 out of the 12Z
    run means when you ask for the 0–24 h record: 12Z today through 12Z tomorrow. Pass
    it explicitly for one of the shorter buckets that share the same file.
    """
    reference = datetime.strptime(f"{date}{cycle}", "%Y%m%d%H").replace(tzinfo=UTC)
    end = reference + timedelta(hours=fhour)
    return end - timedelta(hours=accumulation if accumulation is not None else fhour), end


def is_published(url: str, timeout: int = 30) -> bool:
    """Whether the filter is currently serving GRIB2 bytes for this URL.

    Reads only the first four bytes and drops the connection. HEAD is not used because
    the CGI generates its response by running the subset, so a HEAD is neither cheaper
    nor reliably supported — and the body is the only place the answer actually is.

    A cycle that has not finished publishing is a normal, expected state here, not an
    error: it is what ``wait_for_cycle`` polls through.
    """
    request = urllib.request.Request(url, headers={"User-Agent": "ice-airflow/gdal_weather"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read(len(GRIB_MAGIC)) == GRIB_MAGIC
    except urllib.error.HTTPError:
        return False
    except (urllib.error.URLError, TimeoutError):
        # A timeout or a reset mid-poll says nothing about whether the cycle exists,
        # so it is treated the same as "not yet" and the caller tries again.
        return False
