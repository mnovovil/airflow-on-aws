#!/usr/bin/env bash
# Open a shell on a box, or inside one of the containers running on it.
#
#   ./scripts/shell.sh                   # dev, a shell on the Airflow box itself
#   ./scripts/shell.sh dev scheduler     # inside dev's airflow-scheduler container
#   ./scripts/shell.sh prod postgres     # inside prod's Postgres container
#   ./scripts/shell.sh dev worker        # the GDAL worker box (started if stopped)
#
# There is no SSH here and there is not meant to be: neither box has a key pair, and
# both sit behind security groups with no ingress rules at all. Everything reaches
# them through the SSM agent's outbound connection, so this is `aws ssm start-session`
# wearing an ssh-shaped hat. What you get is the same interactive shell, without a
# port open to the internet or a private key on your laptop.
#
# Read-only as far as this script is concerned — it starts a stopped box if you say
# yes, and otherwise only opens the session. What you do inside it is your business.
set -euo pipefail

# Defaults to dev for the same reason airflow_ui.sh does: a root shell on the live
# scheduler should take a deliberate word on the command line.
ENV="${1:-dev}"
TARGET="${2:-box}"
REGION="${AWS_REGION:-eu-north-1}"

# See airflow_ui.sh: the default profile is an `aws login` browser session that
# expires, and ice-admin is the long-lived IAM user the rest of the README assumes.
export AWS_PROFILE="${AWS_PROFILE:-ice-admin}"

# The compose project name, from the `name:` at the top of infra/airflow_compose.yml.
# Containers are found by their compose labels rather than by their generated names —
# the names are an implementation detail of whichever compose version is on the box,
# the labels are the contract.
PROJECT="ice-airflow"

usage() {
  cat >&2 <<USAGE
usage: $0 [dev|prod] [box|scheduler|webserver|postgres|worker]

  box         a shell on the Airflow host (the default)
  scheduler   inside the airflow-scheduler container
  webserver   inside the airflow-webserver container
  postgres    inside the Postgres container
  worker      a shell on the GDAL worker host — normally stopped
USAGE
}

if [[ "$ENV" == "-h" || "$ENV" == "--help" ]]; then
  usage
  exit 0
fi

if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "unknown environment '$ENV' — expected dev or prod" >&2
  usage
  exit 1
fi

# Which box the target lives on, and which container on it — empty container means a
# shell on the host itself.
case "$TARGET" in
  box|host|airflow) BOX=airflow; SERVICE="" ;;
  scheduler)        BOX=airflow; SERVICE="airflow-scheduler" ;;
  webserver|web)    BOX=airflow; SERVICE="airflow-webserver" ;;
  postgres|db)      BOX=airflow; SERVICE="postgres" ;;
  worker|gdal)      BOX=worker;  SERVICE="" ;;
  *)
    echo "unknown target '$TARGET'" >&2
    usage
    exit 1
    ;;
esac

cd "$(dirname "$0")/.."

# OpenTofu is a drop-in; TF wins if it is set. Same lookup as airflow_ui.sh, and for
# the same reason: a script meant to be run often should not need a prefix.
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

The AWS CLI cannot open a session without it, and the error it gives when the plugin
is missing does not say so. Install it:

  https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
MISSING
  exit 1
fi

# The backend is partial, so the working directory is initialised against one
# environment at a time and `terraform output` reads whichever that was. Re-init
# rather than trust it — a prompt on the dev box and a prompt on the prod box look
# exactly alike.
echo "==> Selecting the $ENV state"
"$TF" -chdir=infra init -input=false -reconfigure \
  -backend-config="backends/$ENV.hcl" >/dev/null

STATE_ENV=$(tf_out env)
if [[ "$STATE_ENV" != "$ENV" ]]; then
  echo "state says '$STATE_ENV' but '$ENV' was requested — refusing to continue" >&2
  exit 1
fi

if [[ "$BOX" == "airflow" ]]; then
  INSTANCE=$(tf_out airflow_instance_id)
  BOX_LABEL="Airflow box"
  # ~$16/month while it is up, and switching it off is the first thing done when the
  # bill matters — so finding it stopped is a normal state rather than a fault.
  COST_NOTE="it bills at roughly \$16/month for as long as it stays up"
else
  INSTANCE=$(tf_out worker_instance_id)
  BOX_LABEL="GDAL worker"
  # The worker is stopped by design between DAG runs. Starting it by hand means
  # nothing will stop it again except the idle alarm in ec2.tf, 30 minutes later.
  COST_NOTE="it is meant to be stopped between runs — the idle alarm stops it again after ~30 idle minutes"
fi

STATE=$(aws ec2 describe-instances --instance-ids "$INSTANCE" --region "$REGION" \
  --query 'Reservations[0].Instances[0].State.Name' --output text)

if [[ "$STATE" != "running" ]]; then
  echo
  echo "  The $ENV $BOX_LABEL is $STATE. It has to be running to open a shell, and"
  echo "  $COST_NOTE."
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

  # Running is not the same as reachable: the SSM agent has to register before a
  # session can be opened, and one attempted before that fails with a target-not-
  # connected error that reads like a permissions problem.
  echo "==> Waiting for SSM to answer"
  for _ in $(seq 1 60); do
    ONLINE=$(aws ssm describe-instance-information --region "$REGION" \
      --filters "Key=InstanceIds,Values=$INSTANCE" \
      --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)
    [[ "$ONLINE" == "Online" ]] && break
    sleep 5
  done
  echo "    SSM: ${ONLINE:-unknown}. The containers may need another minute after this."
fi

# ------------------------------------------------------------------ a shell on the host

if [[ -z "$SERVICE" ]]; then
  cat <<BANNER

  Environment  $ENV
  Target       $BOX_LABEL ($INSTANCE)

  You land as ssm-user, which is not in the docker group — prefix docker with sudo,
  or take a root login with 'sudo -i'. The Airflow box keeps everything under
  /opt/ice: docker-compose.yml, .env, the synced dags/, and the sync scripts.

  'exit' closes the session. Closing it does not stop the instance.

BANNER
  echo "==> Opening a shell on the $ENV $BOX_LABEL"
  exec aws ssm start-session --target "$INSTANCE" --region "$REGION"
fi

# ------------------------------------------------------------- a shell in a container

# Found by compose label rather than by name, and with an explicit message when it is
# not there — the bare `docker exec` failure for a missing container is a one-line
# daemon error that does not say which of the several plausible reasons applies.
REMOTE_CMD=$(cat <<REMOTE
cid=\$(sudo docker ps -q --filter label=com.docker.compose.project=$PROJECT --filter label=com.docker.compose.service=$SERVICE | head -n1)
if [ -z "\$cid" ]; then
  echo "no running $SERVICE container on this box. What is up:" >&2
  sudo docker ps --format '  {{.Names}}\t{{.Status}}' >&2
  exit 1
fi
exec sudo docker exec -it "\$cid" bash
REMOTE
)

# Built with json.dumps rather than by hand: the command carries quotes, braces and
# newlines, and every one of them is a character that would need escaping twice over
# on the way through --parameters.
PARAMS=$(printf '%s' "$REMOTE_CMD" | python3 -c \
  'import json,sys; print(json.dumps({"command": [sys.stdin.read()]}))')

cat <<BANNER

  Environment  $ENV
  Target       $SERVICE, on $INSTANCE
  Shell        bash, as the image's own user — airflow, or postgres in the database

  Useful in there: 'airflow dags list', 'airflow tasks test <dag> <task> <date>' in
  the Airflow containers; 'psql -U airflow' in the Postgres one. If you need root
  inside the container, open the 'box' target and run the exec with -u 0 yourself.

  'exit' closes the session. Nothing you write inside the container survives it
  being recreated — /opt/ice/dags on the host is read-only in there, and the image
  sync timer replaces the containers whenever the pointer secret changes.

BANNER

echo "==> Opening a shell in $SERVICE on the $ENV $BOX_LABEL"
# AWS-StartInteractiveCommand rather than a plain session: it is the document that
# runs something on the far end with a TTY attached, which is what makes `docker exec
# -it` behave. If it is ever unavailable, the equivalent by hand is to open a plain
# session with this script's 'box' target and run the docker exec yourself.
exec aws ssm start-session --target "$INSTANCE" --region "$REGION" \
  --document-name AWS-StartInteractiveCommand \
  --parameters "$PARAMS"
