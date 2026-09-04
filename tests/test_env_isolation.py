"""The two environments must not share anything they are meant to own separately.

Terraform will not catch this. Both tfvars files are valid on their own, and a
copy-paste that leaves dev pointing at ``example-dem`` produces a stack that
applies cleanly and then quietly deletes prod's S3 notification rules on its first
run — the failure shows up as prod uploads doing nothing at all, hours later and
nowhere near the change that caused it.

Deliberately a text scan rather than an HCL parse: the assertions are about a handful
of scalar assignments, and adding a parser dependency to the test suite to read four
lines is a worse trade than a regex that fails loudly if the syntax drifts.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
INFRA_DIR = ROOT / "infra"
ENVS_DIR = ROOT / "infra" / "envs"
BACKENDS_DIR = ROOT / "infra" / "backends"
DEPLOY_WORKFLOW = ROOT / ".github" / "workflows" / "deploy.yml"

ENVIRONMENTS = ("prod", "dev")

# Values that must differ between environments, and the reason each one matters.
#
# The email addresses are deliberately NOT here: both environments send from and to
# the same pair, so that dev's reports arrive in the ordinary inbox with nothing to
# verify first. What still has to hold is that only one stack CREATES each identity,
# which is what the ses_identities tests below check.
MUST_DIFFER = {
    "source_bucket": "notification config is authoritative per bucket — sharing one means each apply deletes the other's triggers",  # noqa: E501
    "vpc_cidr": "distinct ranges keep an address self-identifying",
}


def scalar(text: str, name: str) -> str | None:
    """Value of a top-level `name = "value"` assignment, ignoring comments."""
    match = re.search(rf'^\s*{name}\s*=\s*"([^"]*)"', text, re.MULTILINE)
    return match.group(1) if match else None


def string_list(text: str, name: str) -> list[str]:
    """Values of a top-level `name = [ "a", "b" ]` assignment."""
    match = re.search(rf"^\s*{name}\s*=\s*\[(.*?)\]", text, re.MULTILINE | re.DOTALL)
    return re.findall(r'"([^"]+)"', match.group(1)) if match else []


@pytest.fixture(scope="module")
def tfvars() -> dict[str, str]:
    return {env: (ENVS_DIR / f"{env}.tfvars").read_text() for env in ENVIRONMENTS}


@pytest.mark.parametrize("env", ENVIRONMENTS)
def test_every_environment_has_its_pair_of_files(env: str) -> None:
    """A tfvars file without a backend config is a stack with nowhere to put its state."""
    assert (ENVS_DIR / f"{env}.tfvars").is_file()
    assert (BACKENDS_DIR / f"{env}.hcl").is_file()


@pytest.mark.parametrize("env", ENVIRONMENTS)
def test_env_variable_matches_the_file_name(env: str, tfvars: dict[str, str]) -> None:
    """envs/dev.tfvars setting env = "prod" would build prod under dev's state key."""
    assert scalar(tfvars[env], "env") == env


@pytest.mark.parametrize("name,why", MUST_DIFFER.items())
def test_environments_do_not_share(name: str, why: str, tfvars: dict[str, str]) -> None:
    values = {env: scalar(tfvars[env], name) for env in ENVIRONMENTS}
    assert all(values.values()), f"{name} must be set in every environment — it has no default"
    assert len(set(values.values())) == len(ENVIRONMENTS), f"{name} is shared: {values} — {why}"


def test_ses_identities_are_disjoint(tfvars: dict[str, str]) -> None:
    """Two stacks declaring one address collide on apply; a destroy un-verifies it."""
    owned = {env: set(string_list(tfvars[env], "ses_identities")) for env in ENVIRONMENTS}
    shared = owned["prod"] & owned["dev"]
    assert not shared, f"both environments create the SES identity for {shared}"


def test_every_address_in_use_is_verified_by_some_environment(tfvars: dict[str, str]) -> None:
    """An unowned address is a run-time failure: apply succeeds, then SES refuses to send.

    Environments may share addresses — dev does — but the identity has to be created
    somewhere, and an environment whose ses_identities list is empty is relying on
    another one having done it.
    """
    verified = {addr for env in ENVIRONMENTS for addr in string_list(tfvars[env], "ses_identities")}
    for env in ENVIRONMENTS:
        for field in ("email_from", "email_to"):
            addr = scalar(tfvars[env], field)
            assert addr in verified, f"{env}'s {field} ({addr}) is created by no environment"


def test_state_keys_are_distinct() -> None:
    """Two environments on one key is one environment, applied twice."""
    keys = {env: scalar((BACKENDS_DIR / f"{env}.hcl").read_text(), "key") for env in ENVIRONMENTS}
    assert all(keys.values()), f"every backend config needs a key: {keys}"
    assert len(set(keys.values())) == len(ENVIRONMENTS), f"state keys are shared: {keys}"


def test_prod_source_bucket_has_no_expiry_rule(tfvars: dict[str, str]) -> None:
    """The guard that matters now that prod's bucket is managed rather than looked up.

    dev expires everything in its source bucket after 30 days. Prod's holds the real
    rasters and the archive move_file writes under sent/, so inheriting that rule
    would delete both on a delay — an outage that starts a month after the apply that
    caused it. The variable's validation refuses it too; this is the cheaper failure.
    """
    assert re.search(r"^\s*source_bucket_expiry_days\s*=\s*null", tfvars["prod"], re.MULTILINE)


def test_prod_buckets_are_adopted_not_assumed(tfvars: dict[str, str]) -> None:
    """Prod's two buckets outlived the stack, so a prod apply must adopt them.

    They were removed from state rather than deleted in the 2026-09-02 teardown —
    force_destroy is refused for prod, and a destroy that reached them would have
    stripped their encryption and public-access-block configuration before failing on
    BucketNotEmpty. So prod's state is empty while the buckets still exist, and an
    apply without these blocks would CreateBucket on names the account still owns and
    fail with BucketAlreadyOwnedByYou.

    dev is the opposite case, and that asymmetry is why the blocks are gated rather
    than unconditional as they once were: dev's buckets were destroyed outright, and
    importing one that does not exist fails the plan with "Cannot import non-existent
    remote object". An import that covered both environments would now block every dev
    apply. for_each over an empty set is the only way to make an import conditional.
    """
    imports = (INFRA_DIR / "import.tf").read_text()
    for target in ("aws_s3_bucket.source", "aws_s3_bucket.artifacts"):
        adopted = re.search(rf"to\s*=\s*{re.escape(target)}\b", imports)
        assert adopted, f"import.tf no longer adopts {target}"
    gates = imports.count('var.env == "prod"')
    assert gates == 2, f"both imports must be gated to prod, found {gates} gate(s)"


def test_prod_buckets_cannot_be_force_destroyed(tfvars: dict[str, str]) -> None:
    """The variable's own validation enforces this too — this is the cheaper failure."""
    assert re.search(r"^\s*force_destroy_buckets\s*=\s*false", tfvars["prod"], re.MULTILINE)


@pytest.mark.parametrize("env", ENVIRONMENTS)
def test_deploy_workflow_maps_a_branch_to_every_environment(env: str) -> None:
    """An environment CI cannot reach is one that drifts from the branch that owns it."""
    # Bound to a name rather than inlined: the two ruff versions in play format an
    # inline call-plus-message assert differently, and CI pins the older one.
    mapping = rf'name={env}"?\s*>>\s*"\$GITHUB_OUTPUT"'
    workflow = DEPLOY_WORKFLOW.read_text()
    assert re.search(mapping, workflow), f"deploy.yml has no branch mapped to {env}"
