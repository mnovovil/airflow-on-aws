"""The Airflow bootstrap has to fit in EC2's user_data, and it very nearly does not.

This is not a theoretical limit. The rendered script went past 16 KiB when the image
sync was added, and the way that failed is the reason this file exists: `terraform
plan` is happy, because the length is only known once the template is rendered against
real resource attributes. The apply had already destroyed the instance by the time the
provider rejected the replacement, so dev lost its Airflow box and could not get it
back without a fix — the worst possible moment to find out.

`base64gzip` buys roughly a factor of two, which is headroom rather than a solution.
If the compressed size ever approaches the cap, the answer is not more compression: it
is that the bootstrap has outgrown user_data and belongs in the image, or in an S3
object with its hash wired into the instance.

Deliberately a text substitution rather than a call into Terraform, for the same
reason test_env_isolation scans instead of parsing: the template's inputs are a
handful of scalars, and the size is what is being asserted, not the rendering.
"""

from __future__ import annotations

import base64
import gzip
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "infra" / "airflow_user_data.sh.tftpl"
COMPOSE = ROOT / "infra" / "airflow_compose.yml"
INSTANCE = ROOT / "infra" / "airflow_ec2.tf"

# EC2's hard cap on user_data, applied to the decoded bytes.
LIMIT = 16 * 1024

# dev's names, because they are the longer ones — every resource carries the `-dev`
# infix, so a script that fits here fits in prod.
VARS = {
    "region": "eu-north-1",
    "artifacts_bucket": "ice-dev-artifacts-123456789012",
    "admin_secret_name": "ice-dev/airflow-admin",
    "admin_user": "admin",
    "airflow_version": "2.10.3",
    "compose_url": "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64",
    "secrets_prefix": "airflow-dev",
    "image_secret_name": "ice-dev/airflow-image",
    "ecr_registry": "123456789012.dkr.ecr.eu-north-1.amazonaws.com",
}


def render() -> bytes:
    """templatefile() over the two forms the bootstrap uses: ${x} and the $${x} escape."""
    text = TEMPLATE.read_text()
    values = dict(VARS, compose_b64=base64.b64encode(COMPOSE.read_bytes()).decode())

    def one(match: re.Match) -> str:
        key = match.group(1)
        assert key in values, f"template reads ${{{key}}}, which this test does not supply"
        return values[key]

    # The negative lookbehind is what keeps $${VAR} — a shell variable, escaped for
    # Terraform — from being treated as a substitution.
    rendered = re.sub(r"(?<!\$)\$\{([a-z0-9_]+)\}", one, text)
    assert "${compose_b64}" not in rendered, "the compose file was not substituted in"
    return rendered.replace("$${", "${").encode()


def test_user_data_fits_once_compressed():
    size = len(gzip.compress(render(), 9))
    assert size < LIMIT, (
        f"the compressed bootstrap is {size:,} bytes against a {LIMIT:,} byte cap. "
        "Compressing harder is not the fix — move it into the image or into S3."
    )


def test_compression_is_still_what_makes_it_fit():
    """If the raw script ever fits again, this test and the base64gzip can both go."""
    raw = len(render())
    if raw < LIMIT:
        return
    assert "base64gzip(templatefile(" in INSTANCE.read_text(), (
        f"the rendered bootstrap is {raw:,} bytes, over the {LIMIT:,} byte cap, so the "
        "instance must pass it through base64gzip. Without that the apply destroys the "
        "instance and then fails to create its replacement."
    )


def test_user_data_stays_known_at_plan_time():
    """The bootstrap must not interpolate the ECR repo or the image secret.

    Both are created in the same run as the instance that reads them, so referencing
    their attributes makes user_data unknown at plan time. On an instance carrying
    user_data_replace_on_change that is not a cosmetic problem: Terraform renders the
    plan as an in-place update, the provider works out mid-apply that it needs a
    destroy-and-create, and the run aborts with "Provider produced inconsistent final
    plan". That is how prod's deploy failed, with the ECR repository created and the
    instance left untouched.

    Narrow on purpose. The template also reads the artifacts bucket and the admin
    secret, and those are fine: they are only unknown when the whole environment is
    new, and an instance being created for the first time is not being replaced. This
    asserts the two that were the actual regression, not a rule about resource
    references in general — which is why the locals carry the explanation.
    """
    text = INSTANCE.read_text()
    start = text.index("templatefile(")
    block = text[start : text.index("}))", start)]

    for reference in ("aws_ecr_repository.airflow", "aws_secretsmanager_secret.airflow_image_uri"):
        assert reference not in block, (
            f"user_data interpolates {reference}, which is unknown at plan time on the "
            "run that creates it. Use the matching local instead; ordering comes from "
            "the instance's depends_on."
        )

    assert "local.ecr_registry" in block
    assert "local.airflow_image_secret" in block
