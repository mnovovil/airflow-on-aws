#!/usr/bin/env bash
# One-time setup. Run with admin credentials, from the repository root.
#
# Creates the Terraform state bucket, the GitHub OIDC provider and the deploy role
# — the resources the main stack cannot create for itself because it stores its
# state in one of them.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Checking credentials"
IDENTITY=$(aws sts get-caller-identity --output json)
echo "$IDENTITY"

if echo "$IDENTITY" | grep -q ':root'; then
  cat <<'WARNING'

  WARNING: these are root credentials.

  Root keys cannot be scoped or rotated cleanly, and this script bakes the caller
  into resources that outlive it. Create an IAM admin user, switch your profile to
  it, and re-run.

WARNING
  read -r -p "  Continue with root anyway? [y/N] " reply
  [[ "$reply" == "y" || "$reply" == "Y" ]] || exit 1
fi

echo
echo "==> Applying bootstrap stack"
terraform -chdir=infra/bootstrap init -input=false
terraform -chdir=infra/bootstrap apply -input=false

ROLE_ARN=$(terraform -chdir=infra/bootstrap output -raw github_actions_role_arn)
STATE_BUCKET=$(terraform -chdir=infra/bootstrap output -raw state_bucket)

cat <<NEXT

==> Bootstrap complete

  State bucket : $STATE_BUCKET
  Deploy role  : $ROLE_ARN

Both are shared by every environment: one bucket holding one state file per key,
and one role assumable from any branch of the repository.

Next:

  1. Confirm the backend bucket in infra/versions.tf matches the state bucket above.
     The key is not there — it comes from infra/backends/<env>.hcl at init time.

  2. Register the role with the repository so Actions can assume it:

       gh variable set AWS_DEPLOY_ROLE_ARN --body "$ROLE_ARN"

  3. Push a deployable branch. main builds prod and dev builds dev; the workflow
     refuses any other branch rather than guessing. Give the Airflow box a few
     minutes after the first apply — it installs Airflow at boot.

  4. Click the SES verification links sent to both addresses. Until that is done
     SES accepts nothing, because the account is still in the sandbox:

       sender@example.com   recipient@example.com

     Prod creates these identities and dev uses them without redeclaring them —
     an address can only be created by one stack. So this step is prod's only,
     and a fresh dev environment needs nothing here.

NEXT
