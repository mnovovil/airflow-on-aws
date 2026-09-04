#!/usr/bin/env bash
# End-to-end check: upload a raster and watch the pipeline react.
#
#   ./scripts/smoke_test.sh path/to/raster.tif          # dev, the default
#   ./scripts/smoke_test.sh path/to/raster.tif prod     # the live one
#
# The bucket, the worker and the log group all come from the selected environment's
# state rather than from arguments, so pointing this at prod by accident takes a
# deliberate word on the command line.
set -euo pipefail

RASTER="${1:-}"
# Defaults to dev on purpose: a smoke test is exactly the thing that should land in
# the disposable environment unless you say otherwise.
ENV="${2:-dev}"
REGION="${AWS_REGION:-eu-north-1}"

if [[ -z "$RASTER" || ! -f "$RASTER" ]]; then
  echo "usage: $0 <path-to-raster.tif> [dev|prod]" >&2
  exit 1
fi

if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "unknown environment '$ENV' — expected dev or prod" >&2
  exit 1
fi

cd "$(dirname "$0")/.."

# OpenTofu is a drop-in here; override with TF=tofu if that is what is installed.
TF="${TF:-terraform}"
tf_out() { "$TF" -chdir=infra output -raw "$1"; }

# The backend is partial, so the working directory is initialised against one
# environment at a time and `terraform output` reads whichever that was. Re-init
# rather than trust it — the alternative is uploading dev's raster into prod.
echo "==> Selecting the $ENV state"
"$TF" -chdir=infra init -input=false -reconfigure \
  -backend-config="backends/$ENV.hcl" >/dev/null

STATE_ENV=$(tf_out env)
if [[ "$STATE_ENV" != "$ENV" ]]; then
  echo "state says '$STATE_ENV' but '$ENV' was requested — refusing to continue" >&2
  exit 1
fi

BUCKET=$(tf_out source_bucket)
INSTANCE=$(tf_out worker_instance_id)
LOG_GROUP=$(tf_out trigger_lambda_log_group)
KEY="smoke/$(date +%Y%m%dT%H%M%S)-$(basename "$RASTER")"

echo "==> Worker state before upload"
aws ec2 describe-instances --instance-ids "$INSTANCE" \
  --query 'Reservations[0].Instances[0].State.Name' --output text

echo "==> Uploading s3://$BUCKET/$KEY"
aws s3 cp "$RASTER" "s3://$BUCKET/$KEY"

echo "==> Trigger Lambda logs (Ctrl-C when you have seen enough)"
echo "    Expect an invoke and a successful trigger within a few seconds."
aws logs tail "$LOG_GROUP" --follow --since 2m --region "$REGION" &
TAIL_PID=$!
sleep 45
kill "$TAIL_PID" 2>/dev/null || true

echo
echo "==> Worker state during the run (expect running)"
aws ec2 describe-instances --instance-ids "$INSTANCE" \
  --query 'Reservations[0].Instances[0].State.Name' --output text

cat <<NEXT

Now check, in order:

  Environment    $ENV
  Airflow UI     $(tf_out airflow_ui)
  Report         aws s3 ls s3://$(tf_out artifacts_bucket)/reports/$KEY/
  Email          $(tf_out email_to)
  Worker stopped aws ec2 describe-instances --instance-ids $INSTANCE \\
                   --query 'Reservations[0].Instances[0].State.Name' --output text

The last one is the one worth confirming — it is what keeps the bill down.

NEXT
