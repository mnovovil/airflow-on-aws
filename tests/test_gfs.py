"""The NOMADS URL is the whole interface to the data, and it is easy to get subtly wrong.

Every parameter in it is load-bearing in a way that fails quietly rather than loudly:
drop ``subregion`` and you silently download the globe, get ``dir`` wrong by one day
and you email yesterday's forecast, use the wrong ``lev_`` spelling and the filter
returns an empty body with a 200.

The URL in ``test_the_url_matches_the_one_that_was_verified_by_hand`` is the exact one
that was run against NOMADS while this pipeline was being written and confirmed to
return a 241x105 two-band GRIB2. It is a fixture in the strict sense — a recorded
observation of a working request, not a restatement of build_url's own logic.
"""

from __future__ import annotations

import urllib.error
import urllib.parse

import pytest

from common import gfs

VERIFIED = (
    "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
    "?file=gfs.t12z.pgrb2.0p25.f024&lev_surface=on&var_APCP=on&subregion="
    "&leftlon=235&rightlon=295&toplat=50&bottomlat=24&dir=%2Fgfs.20260818%2F12%2Fatmos"
)


def _query(url: str) -> dict[str, list[str]]:
    return urllib.parse.parse_qs(urllib.parse.urlparse(url).query, keep_blank_values=True)


def test_the_url_matches_the_one_that_was_verified_by_hand():
    assert gfs.build_url("20260818", cycle="12", fhour=24) == VERIFIED


def test_the_subregion_flag_survives_url_encoding():
    """An empty value, and its presence alone is what switches subsetting on.

    urlencode is entitled to drop nothing, but a refactor to a dict comprehension that
    filters falsy values would — and the result is a global download that still works,
    just 500 MB of it.
    """
    assert _query(gfs.build_url("20260818"))["subregion"] == [""]


def test_the_directory_is_the_cycle_date_not_the_valid_date():
    """f024 out of the 12Z run on the 18th is valid on the 19th. The path says the 18th."""
    assert _query(gfs.build_url("20260818", cycle="12", fhour=24))["dir"] == ["/gfs.20260818/12/atmos"]


def test_the_forecast_hour_is_zero_padded_to_three_digits():
    """`f24` is a 404. The filter has no tolerance for it."""
    assert "gfs.t12z.pgrb2.0p25.f024" in gfs.build_url("20260818", fhour=24)
    assert "gfs.t00z.pgrb2.0p25.f003" in gfs.build_url("20260818", cycle="00", fhour=3)


@pytest.mark.parametrize("cycle", ["13", "6", "12Z", ""])
def test_an_impossible_cycle_is_refused_rather_than_requested(cycle):
    """GFS runs at 00/06/12/18Z. Anything else is a 404 several minutes later, on a
    worker that has already been started, instead of a ValueError here."""
    with pytest.raises(ValueError, match="GFS runs at"):
        gfs.build_url("20260818", cycle=cycle)


@pytest.mark.parametrize("date", ["2026-08-18", "260818", "20260818T12", "not-a-date"])
def test_a_malformed_date_is_refused(date):
    with pytest.raises(ValueError, match="YYYYMMDD"):
        gfs.build_url(date)


def test_the_window_is_the_accumulation_period_not_the_lead_time():
    """The point of the whole DAG: 12Z today through 12Z tomorrow."""
    start, end = gfs.window("20260818", cycle="12", fhour=24)
    assert start.isoformat() == "2026-08-18T12:00:00+00:00"
    assert end.isoformat() == "2026-08-19T12:00:00+00:00"


def test_a_shorter_accumulation_ends_at_the_same_moment():
    """The 6-hour bucket in the same file shares the f024 valid time and covers 18-24h."""
    start, end = gfs.window("20260818", cycle="12", fhour=24, accumulation=6)
    assert start.isoformat() == "2026-08-19T06:00:00+00:00"
    assert end.isoformat() == "2026-08-19T12:00:00+00:00"


def test_a_non_grib_body_is_not_mistaken_for_a_published_cycle(monkeypatch):
    """NOMADS answers an unpublished cycle with 200 and an apology, so the status code
    cannot be the check. This is what wait_for_cycle polls on."""

    class FakeResponse:
        def __init__(self, payload):
            self.payload = payload

        def read(self, n):
            return self.payload[:n]

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(gfs.urllib.request, "urlopen", lambda *a, **k: FakeResponse(b"<html>no data"))
    assert gfs.is_published("https://nomads.ncep.noaa.gov/x") is False

    monkeypatch.setattr(gfs.urllib.request, "urlopen", lambda *a, **k: FakeResponse(b"GRIB\x00\x00"))
    assert gfs.is_published("https://nomads.ncep.noaa.gov/x") is True


@pytest.mark.parametrize(
    "error",
    [
        urllib.error.HTTPError("https://x", 404, "Not Found", {}, None),
        urllib.error.URLError("connection reset"),
        TimeoutError("timed out"),
    ],
)
def test_a_network_error_reads_as_not_yet_rather_than_raising(monkeypatch, error):
    """wait_for_cycle polls through this state. An exception escaping here would end
    the run on a transient reset instead of trying again a moment later."""

    def boom(*args, **kwargs):
        raise error

    monkeypatch.setattr(gfs.urllib.request, "urlopen", boom)
    assert gfs.is_published("https://nomads.ncep.noaa.gov/x") is False
