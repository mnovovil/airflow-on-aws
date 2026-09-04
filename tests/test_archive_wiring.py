"""The archive step spans three files that have to agree, and nothing else checks it.

``move_file`` copies a raster under ``sent/`` and deletes the original. That needs
three separate things to line up, each in a different language:

* the DAG picks the destination key (``ARCHIVE_PREFIX``),
* the scheduler's IAM policy has to allow the copy and the delete — a gap here is
  invisible until a real run, because every test of the task itself uses a fake S3
  client,
* the trigger Lambda has to ignore the ``ObjectCreated`` event that the copy fires,
  or each archived raster costs a second worker start and a duplicate email.

The failure this file is really about is the middle one: the email goes out before
the move, so a missing permission produces a run that looks half-successful — report
delivered, raster still sitting in the watched prefix.

A text scan rather than an HCL parse, for the same reason as test_env_isolation.py:
these are a handful of scalar assignments, and a parser dependency to read them is
the worse trade.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import gdalinfo_notify as dag_module

ROOT = Path(__file__).resolve().parent.parent
INFRA_DIR = ROOT / "infra"
HANDLER = ROOT / "lambda" / "trigger_dag" / "handler.py"


@pytest.fixture(scope="module")
def terraform() -> dict[str, str]:
    return {name: (INFRA_DIR / name).read_text() for name in ("versions.tf", "airflow_ec2.tf", "lambda.tf")}


@pytest.fixture(scope="module")
def archive_prefix(terraform: dict[str, str]) -> str:
    """The value in Terraform's locals, which is the one the deployed stack uses."""
    match = re.search(r'^\s*archive_prefix\s*=\s*"([^"]*)"', terraform["versions.tf"], re.MULTILINE)
    assert match, "local.archive_prefix is gone — the IAM policy and the Lambda both read it"
    return match.group(1)


def test_the_dag_and_the_stack_agree_on_the_prefix(archive_prefix: str) -> None:
    """Drift here archives to one prefix and permits another: AccessDenied on every run."""
    assert dag_module.ARCHIVE_PREFIX == archive_prefix


def test_the_lambda_default_agrees_too(archive_prefix: str) -> None:
    """The default is what a hand-invoked or half-configured function falls back to."""
    default = r'^ARCHIVE_PREFIX = os\.environ\.get\("ARCHIVE_PREFIX", "([^"]*)"\)'
    match = re.search(default, HANDLER.read_text(), re.MULTILINE)
    assert match, "the handler no longer reads ARCHIVE_PREFIX"
    assert match.group(1) == archive_prefix


def test_the_lambda_is_told_the_prefix(terraform: dict[str, str]) -> None:
    """Left unset, the function keeps working off its default — which is fine until the
    two are changed apart, and then the loop guard is reading a stale string."""
    assert re.search(r"^\s*ARCHIVE_PREFIX\s*=\s*local\.archive_prefix", terraform["lambda.tf"], re.MULTILINE)


def test_the_scheduler_may_delete_the_original(terraform: dict[str, str]) -> None:
    """Without this the copy lands and the source stays: every raster archived twice."""
    assert "s3:DeleteObject" in terraform["airflow_ec2.tf"]


def test_the_scheduler_may_write_the_archive_copy(terraform: dict[str, str]) -> None:
    """Scoped to the prefix on purpose — a PutObject grant over the whole bucket would
    let a wrong destination key write a raster back into the watched prefix."""
    resource = r'Resource\s*=\s*"\$\{local\.source_bucket_arn\}/\$\{local\.archive_prefix\}\*"'
    granted = re.search(resource, terraform["airflow_ec2.tf"])
    assert granted, "PutObject is not granted under the archive prefix"


def test_the_scheduler_may_read_what_it_copies(terraform: dict[str, str]) -> None:
    """boto3's copy() reads the source object itself; the worker's own read grant is a
    different role and does nothing for the scheduler."""
    statement = re.search(
        r'Action\s*=\s*\[([^\]]*)\]\s*\n\s*Resource\s*=\s*"\$\{local\.source_bucket_arn\}/\*"',
        terraform["airflow_ec2.tf"],
    )
    assert statement, "the scheduler has no statement over the source bucket's objects"
    assert "s3:GetObject" in statement.group(1)
