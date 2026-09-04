#!/usr/bin/env bash
# Open the deployed Airflow UI in a browser, on the graph view of the DAG.
#
#   ./scripts/airflow_ui.sh              # dev, the default
#   ./scripts/airflow_ui.sh prod         # the live one
#   ./scripts/airflow_ui.sh dev 8090     # somewhere other than this env's usual port
#
# There is no public ingress to either box, so this is a port-forward over SSM and
# it holds the terminal open — Ctrl-C ends the session. Everything it needs beyond
# the environment name comes out of that environment's Terraform state.
set -euo pipefail

# Defaults to dev for the same reason smoke_test.sh does: the UI can trigger and
# clear runs, so reaching the live one takes a deliberate word on the command line.
ENV="${1:-dev}"
REGION="${AWS_REGION:-eu-north-1}"

# The default profile on this account is an `aws login` browser session, and it
# expires; ice-admin is the long-lived IAM user everything else in the README
# assumes. Defaulting to it here rather than relying on the shell having exported
# it is the difference between this script working and it failing on credentials
# every time the session lapses. Set AWS_PROFILE yourself to override.
export AWS_PROFILE="${AWS_PROFILE:-ice-admin}"

if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "unknown environment '$ENV' — expected dev or prod" >&2
  echo "usage: $0 [dev|prod] [local-port]" >&2
  exit 1
fi

# Both boxes serve on 8080 and nothing on the page says which one you are looking
# at, so the environments get different local ports rather than the same one. If
# you have to memorise anything, memorise that 8080 is prod.
if [[ "$ENV" == "prod" ]]; then
  DEFAULT_PORT=8080
else
  DEFAULT_PORT=8081
fi
PORT="${2:-$DEFAULT_PORT}"

cd "$(dirname "$0")/.."

# OpenTofu is a drop-in here. The other scripts default to terraform and leave
# TF=tofu to the caller; this one is meant to be run often enough that having to
# remember a prefix would be the thing that stops it being run, so it looks for
# whichever is installed. TF still wins if it is set.
TF="${TF:-}"
if [[ -z "$TF" ]]; then
  for candidate in terraform tofu; do
    if command -v "$candidate" >/dev/null; then
      TF="$candidate"
      break
    fi
  done
fi
if [[ -z "$TF" ]]; then
  echo "neither terraform nor tofu is on PATH — one of them reads the state this needs" >&2
  exit 1
fi
tf_out() { "$TF" -chdir=infra output -raw "$1"; }

if ! command -v session-manager-plugin >/dev/null; then
  cat >&2 <<'MISSING'
session-manager-plugin is not installed.

The AWS CLI can open an SSM shell without it but not a port forward, and the error
it gives when the plugin is missing does not say so. Install it:

  https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
MISSING
  exit 1
fi

# Fail here rather than inside the session, where a port already in use surfaces as
# a generic plugin error several seconds later.
if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  exec 3<&- 3>&-
  echo "local port $PORT is already in use — another forward is probably still open" >&2
  echo "close it, or pass a different port:  $0 $ENV 8090" >&2
  exit 1
fi

# The backend is partial, so the working directory is initialised against one
# environment at a time and `terraform output` reads whichever that was. Re-init
# rather than trust it — the two UIs are indistinguishable once they are open.
echo "==> Selecting the $ENV state"
"$TF" -chdir=infra init -input=false -reconfigure \
  -backend-config="backends/$ENV.hcl" >/dev/null

STATE_ENV=$(tf_out env)
if [[ "$STATE_ENV" != "$ENV" ]]; then
  echo "state says '$STATE_ENV' but '$ENV' was requested — refusing to continue" >&2
  exit 1
fi

INSTANCE=$(tf_out airflow_instance_id)
SECRET=$(tf_out airflow_admin_secret)

# Airflow is the first thing switched off when the bill matters, so a stopped box
# is a normal state to find rather than a fault.
STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE" --region "$REGION" \
  --query 'Reservations[0].Instances[0].State.Name' --output text)

if [[ "$STATE" != "running" ]]; then
  echo
  echo "  The $ENV Airflow box is $STATE. It has to be running to serve the UI,"
  echo "  and it bills at roughly \$16/month for as long as it stays up."
  echo
  read -r -p "  Start it? [y/N] " reply
  if [[ "$reply" != "y" && "$reply" != "Y" ]]; then
    echo
    echo "Left alone. To start it later:"
    echo "  aws ec2 start-instances --instance-ids $INSTANCE --region $REGION"
    exit 1
  fi

  echo "==> Starting $INSTANCE"
  aws ec2 start-instances --instance-ids "$INSTANCE" --region "$REGION" >/dev/null
  aws ec2 wait instance-running --instance-ids "$INSTANCE" --region "$REGION"

  # Running is not the same as ready: the SSM agent has to register and the
  # containers have to come back up, and a forward opened before that just fails.
  echo "==> Waiting for SSM to answer"
  for _ in $(seq 1 60); do
    ONLINE=$(aws ssm describe-instance-information --region "$REGION" \
      --filters "Key=InstanceIds,Values=$INSTANCE" \
      --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)
    [[ "$ONLINE" == "Online" ]] && break
    sleep 5
  done
  echo "    SSM: ${ONLINE:-unknown}. Airflow itself may need another minute after this."
fi

echo "==> Reading the UI login from $SECRET"

# Secrets Manager is the only endpoint this script touches that publishes real IPv6
# addresses — ec2, ssm, sts and s3 all resolve v4-only from here. On a host with no
# working IPv6 route the CLI tries all three AAAA records first and only falls back
# to IPv4 once each has timed out, which at the default connect timeout is about two
# minutes of what looks exactly like a hang. Bounding the per-address connect makes
# the fallback cost seconds instead. Harmless where IPv6 does work: the first
# address answers and the timeout never comes into it.
#
# The machine-wide fix, if this ever bothers anything else, is to prefer IPv4 in
# /etc/gai.conf:  precedence ::ffff:0:0/96  100
CREDS=$(aws secretsmanager get-secret-value --secret-id "$SECRET" --region "$REGION" \
  --cli-connect-timeout 2 --query SecretString --output text)
AIRFLOW_USER=$(printf '%s' "$CREDS" | python3 -c 'import json,sys; print(json.load(sys.stdin)["username"])')
AIRFLOW_PASS=$(printf '%s' "$CREDS" | python3 -c 'import json,sys; print(json.load(sys.stdin)["password"])')

DAG_ID="gdalinfo_notify"
URL="http://localhost:$PORT/dags/$DAG_ID/graph"

cat <<BANNER

  Environment  $ENV
  Instance     $INSTANCE
  Graph view   $URL
  Login        $AIRFLOW_USER / $AIRFLOW_PASS

  Ctrl-C closes the forward. The UI is unreachable the moment it does.

BANNER

# Open the browser once the forward is actually accepting connections, rather than
# immediately onto a refused port. Backgrounded because the session below holds the
# terminal for as long as the forward lives. OPEN_BROWSER=0 to skip it.
if [[ "${OPEN_BROWSER:-1}" == "1" ]] && command -v sensible-browser >/dev/null; then
  (
    for _ in $(seq 1 60); do
      if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
        exec 3<&- 3>&-
        sensible-browser "$URL" >/dev/null 2>&1 || true
        exit 0
      fi
      sleep 1
    done
  ) &
fi

echo "==> Forwarding localhost:$PORT to the $ENV Airflow"
exec aws ssm start-session --target "$INSTANCE" --region "$REGION" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"$PORT\"]}"
