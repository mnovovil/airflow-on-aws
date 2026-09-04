"""The catalogue prefix is written down twice, in two languages, and both must agree.

``common/stac.py`` decides where an item is written. ``infra/airflow_ec2.tf`` decides
where the scheduler is *allowed* to write. Nothing at run time reconciles them, and
the failure mode when they drift is the least helpful one available: every publish
raises AccessDenied, at the end of a run that has already started the worker, emailed
the report and archived the raster.

A text scan rather than an HCL parse, for the same reason as test_archive_wiring.py:
these are a couple of scalar assignments, and a parser dependency to read them is the
worse trade.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from common import stac

ROOT = Path(__file__).resolve().parent.parent
INFRA_DIR = ROOT / "infra"


@pytest.fixture(scope="module")
def terraform() -> dict[str, str]:
    return {name: (INFRA_DIR / name).read_text() for name in ("versions.tf", "airflow_ec2.tf", "s3.tf")}


@pytest.fixture(scope="module")
def stac_prefix(terraform: dict[str, str]) -> str:
    """The value in Terraform's locals, which is the one the deployed policy uses."""
    match = re.search(r'^\s*stac_prefix\s*=\s*"([^"]*)"', terraform["versions.tf"], re.MULTILINE)
    assert match, "local.stac_prefix is gone — the IAM policy and the lifecycle rule both read it"
    return match.group(1)


def test_the_dags_and_the_stack_agree_on_the_prefix(stac_prefix: str) -> None:
    """Terraform carries the trailing slash because it is pasted into an ARN wildcard;
    the module joins with one. Compared as prefixes so the two spellings are not
    themselves the drift this is meant to catch."""
    assert stac_prefix == f"{stac.CATALOG_PREFIX}/"


def test_the_scheduler_may_write_the_catalogue(terraform: dict[str, str]) -> None:
    """Without this every publish_stac raises AccessDenied — after the worker has been
    started, the report emailed and the raster archived."""
    resource = r'Resource\s*=\s*"\$\{aws_s3_bucket\.artifacts\.arn\}/\$\{local\.stac_prefix\}\*"'
    assert re.search(resource, terraform["airflow_ec2.tf"]), "PutObject is not granted under the STAC prefix"


def test_the_write_grant_is_scoped_to_the_prefix(terraform: dict[str, str]) -> None:
    """The artifacts bucket also holds the DAG source the scheduler syncs itself from.
    A bucket-wide PutObject would let a bug in a task overwrite the code that runs it."""
    bucket_wide = (
        r'Action\s*=\s*\["s3:PutObject"\][^}]*' r'Resource\s*=\s*"\$\{aws_s3_bucket\.artifacts\.arn\}/\*"'
    )
    assert not re.search(bucket_wide, terraform["airflow_ec2.tf"])


def test_old_catalogue_versions_are_expired(terraform: dict[str, str]) -> None:
    """collection.json is rewritten in full on every stac_publish run, scheduled daily
    and whether or not anything changed. The bucket is versioned, so without a rule
    the catalogue's history outgrows the catalogue."""
    rule = r'id\s*=\s*"expire-old-stac-versions"'
    assert re.search(rule, terraform["s3.tf"]), "nothing expires noncurrent versions under the STAC prefix"
