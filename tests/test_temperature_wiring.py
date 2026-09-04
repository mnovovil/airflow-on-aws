"""The temperature report spans two files that have to agree, and nothing else checks it.

``ice_config`` is written by Terraform and read by the DAG, and neither one validates
the other. The DAG looks each station up by ``id``, so an entry that Terraform spells
``station`` or ``station_id`` — or one that gives the id as a bare number, which HCL
is happy to do — is not a failed run and not a broken apply. It is a run that quietly
reports the DAG's own fallback station instead, under the right subject line, on
schedule, forever.

The timezone has the same shape of failure. Meteostat stamps its observations UTC
unless it is told otherwise, so a station whose zone is missing or misspelled produces
a "day" that starts at the wrong hour and a chart whose hours are labelled as somebody
else's — with no gap in the data to notice.

A text scan rather than an HCL parse, for the same reason as test_env_isolation.py and
test_archive_wiring.py: these are a handful of scalar assignments, and a parser
dependency to read them is the worse trade.
"""

from __future__ import annotations

import re
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pytest
import temperature as tp

ROOT = Path(__file__).resolve().parent.parent
SECRETS_TF = ROOT / "infra" / "secrets.tf"

# One ``{ key = "value", ... }`` entry of an HCL list.
ENTRY = re.compile(r"\{([^{}]*)\}")
# ``key = "value"`` or ``key = 72503`` — the unquoted form is matched on purpose, so
# the test below can fail on it rather than skip past it.
PAIR = re.compile(r"(\w+)\s*=\s*(\"[^\"]*\"|[^\s,}]+)")


def config_block(key: str) -> str:
    """The text of one ``key = [ ... ]`` list inside the ice_config secret.

    Bracket-counted rather than matched with a regex: the list holds objects, so the
    first ``]`` after the opening bracket is not reliably the closing one.
    """
    source = SECRETS_TF.read_text()
    start = source.index(f"{key} = [")
    depth = 0
    for offset in range(start, len(source)):
        if source[offset] == "[":
            depth += 1
        elif source[offset] == "]":
            depth -= 1
            if depth == 0:
                return source[start : offset + 1]
    raise AssertionError(f"{key} in {SECRETS_TF.name} is never closed")


def stations() -> list[dict[str, str]]:
    """The configured stations, as the DAG will see them after jsonencode."""
    parsed = []
    for entry in ENTRY.findall(config_block("temperature")):
        parsed.append({key: value for key, value in PAIR.findall(entry)})
    return parsed


@pytest.fixture(scope="module")
def configured() -> list[dict[str, str]]:
    found = stations()
    assert found, "no stations in the temperature key — every run would use the fallback"
    return found


def test_the_dag_s_config_key_exists_in_terraform():
    """``get_config().get("temperature")``. Rename either half and the DAG still runs,
    still emails, and reports a station nobody configured."""
    assert "temperature = [" in SECRETS_TF.read_text()


def test_every_station_is_keyed_the_way_the_dag_looks_it_up(configured):
    """The one that motivates this file. ``station["id"]`` is the fetch, the XCom key,
    the chart's filename and the Content-ID — all four go to the fallback together."""
    for station in configured:
        assert "id" in station, f"{station} has no id — the DAG cannot look it up"


def test_the_station_id_is_a_string_and_not_a_number(configured):
    """``id = 72503`` is valid HCL and jsonencodes to a JSON number. Meteostat wants a
    string, and the chart's filename is built by running a regex over it."""
    for station in configured:
        assert station["id"].startswith('"'), f"{station['id']} is unquoted in the tfvars"


def test_every_station_carries_a_timezone(configured):
    """Left off, the day is a UTC one — which for New York starts at 20:00 the day
    before, and the chart says nothing is wrong."""
    for station in configured:
        assert "timezone" in station, f"{station} has no timezone"


def test_every_configured_timezone_is_a_real_one(configured):
    """A typo here does raise, but not until the fetch task runs against Meteostat —
    which on this DAG is a scheduled 08:00 run, not the apply that introduced it."""
    for station in configured:
        name = station["timezone"].strip('"')
        try:
            ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError):  # pragma: no cover - the failure is the point
            pytest.fail(f"{name} is not an IANA timezone")


def test_the_configured_shape_matches_the_dag_s_own_fallback(configured):
    """DEFAULT_STATION is what runs when this key is missing, so the two are meant to
    be interchangeable. Let them drift and the fallback exercises a shape the
    configured path never does — which is the path nobody tests by running it."""
    assert {key for key in configured[0]} == set(tp.DEFAULT_STATION[0])


def test_the_dag_reads_the_recipients_terraform_writes():
    """``email_to`` is the one key this DAG shares with every other report."""
    assert "email_to" in SECRETS_TF.read_text()
    assert "email_to" in (ROOT / "dags" / "temperature.py").read_text()


def test_meteostat_is_in_the_image_the_dag_runs_on():
    """The DAG imports it at module scope, and a missing import is the failure Airflow
    reports worst: the DAG does not appear in the UI at all.

    The 1.x bound is not cosmetic. meteostat 2.x needs pandas>=2.3 and pytz<2024,
    which the Airflow constraint file pinned in this same requirements.txt contradicts
    on both counts — so a 2.x here fails the image build rather than the run.
    """
    requirements = (ROOT / "docker" / "airflow" / "requirements.txt").read_text()
    assert re.search(r"^meteostat>=1\.7,<2$", requirements, re.MULTILINE), requirements
