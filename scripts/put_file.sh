#!/usr/bin/env bash
# Copy a local file onto the Airflow box, into /opt/ice.
#
#   ./scripts/put_file.sh notes.txt                    # dev, lands at /opt/ice/notes.txt
#   ./scripts/put_file.sh notes.txt prod               # the live box
#   ./scripts/put_file.sh notes.txt dev /opt/ice/dags  # somewhere else on the box
#   FORCE=1 ./scripts/put_file.sh notes.txt            # replace a file already there
#
# The counterpart to shell.sh: same box, same route in. There is no SSH and no scp
# to reach for — neither box has a key pair and both sit behind security groups with
# no ingress — so the file travels the way a command does, through the SSM agent's
# outbound connection. Small files ride inside the command itself as base64; larger
# ones are staged in the artifacts bucket and pulled down from there, because
# SendCommand caps what a single command may carry.
#
# AWS-RunShellScript runs as root, which is what /opt/ice needs: everything in there
# is root-owned and ssm-user is not in a position to write it.
set -euo pipefail

SRC="${1:-}"
# Defaults to dev for the same reason shell.sh does: writing a file onto the live
# scheduler's box should take a deliberate word on the command line.
ENV="${2:-dev}"
DEST_DIR="${3:-/opt/ice}"
REGION="${AWS_REGION:-eu-north-1}"

# See airflow_ui.sh: the default profile is an `aws login` browser session that
# expires, and ice-admin is the long-lived IAM user the rest of the README assumes.
export AWS_PROFILE="${AWS_PROFILE:-ice-admin}"

# Above this, the base64 goes through S3 instead of through the command. SendCommand
# allows 100 KB of parameters in total and base64 costs a third on top of the file,
# so the cut-off sits well under the limit rather than near it — the error you get
# from overshooting is a validation failure that does not mention size.
INLINE_MAX_BYTES=${INLINE_MAX_BYTES:-48000}

usage() {
  cat >&2 <<USAGE
usage: $0 <file> [dev|prod] [destination-directory]

  file                    the local file to copy up
  dev|prod                which environment's Airflow box (default: dev)
  destination-directory   where to put it on the box (default: /opt/ice)

  FORCE=1                 overwrite a file that is already at the destination
USAGE
}

if [[ "$SRC" == "-h" || "$SRC" == "--help" ]]; then
  usage
  exit 0
fi

if [[ -z "$SRC" ]]; then
  echo "no file given" >&2
  usage
  exit 1
fi

if [[ ! -f "$SRC" ]]; then
  echo "'$SRC' is not a file" >&2
  exit 1
fi

if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "unknown environment '$ENV' — expected dev or prod" >&2
  usage
  exit 1
fi

# The name on the box is the name here. Passing a directory rather than a full path
# keeps the two ends from disagreeing about which is which.
NAME=$(basename "$SRC")
DEST_DIR="${DEST_DIR%/}"
DEST="$DEST_DIR/$NAME"

cd "$(dirname "$0")/.."

# OpenTofu is a drop-in; TF wins if it is set. Same lookup as shell.sh.
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

# The backend is partial, so the working directory is initialised against one
# environment at a time and `terraform output` reads whichever that was. Re-init
# rather than trust it — the alternative is writing dev's file onto the prod box.
echo "==> Selecting the $ENV state"
"$TF" -chdir=infra init -input=false -reconfigure \
  -backend-config="backends/$ENV.hcl" >/dev/null

STATE_ENV=$(tf_out env)
if [[ "$STATE_ENV" != "$ENV" ]]; then
  echo "state says '$STATE_ENV' but '$ENV' was requested — refusing to continue" >&2
  exit 1
fi

INSTANCE=$(tf_out airflow_instance_id)

# One check rather than two: a stopped box and a box whose agent has not registered
# yet both fail the same way further down, with a target-not-connected error that
# reads like a permissions problem.
PING=$(aws ssm describe-instance-information --region "$REGION" \
  --filters "Key=InstanceIds,Values=$INSTANCE" \
  --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || true)

if [[ "$PING" != "Online" ]]; then
  cat >&2 <<UNREACHABLE

  The $ENV Airflow box ($INSTANCE) is not answering SSM — it reports '${PING:-nothing}'.
  It is most likely stopped. Start it and give the agent a minute:

    aws ec2 start-instances --instance-ids $INSTANCE --region $REGION

  ./scripts/shell.sh $ENV will offer to do that for you.

UNREACHABLE
  exit 1
fi

SIZE=$(wc -c <"$SRC" | tr -d ' ')
# Checked on both ends afterwards. A truncated base64 decodes without complaining,
# so "the command succeeded" is not on its own evidence the file arrived whole.
SUM=$(sha256sum "$SRC" | cut -d' ' -f1)

# Refusing rather than prompting: the answer has to travel to the box either way, so
# a pre-flight existence check would cost a second round trip to ask a question the
# caller can answer up front. FORCE=1 is that answer.
FORCE_FLAG=0
[[ "${FORCE:-0}" == "1" ]] && FORCE_FLAG=1

# ------------------------------------------------------------ the command to run there

STAGED_KEY=""
if (( SIZE <= INLINE_MAX_BYTES )); then
  ROUTE="inline"
  # -w0 is GNU-only; tr keeps this working from a mac laptop as well.
  PAYLOAD=$(base64 <"$SRC" | tr -d '\n')
  FETCH="printf '%s' '$PAYLOAD' | base64 -d >\"\$tmp\""
else
  ROUTE="staged through S3"
  ARTIFACTS=$(tf_out artifacts_bucket)
  # uploads/ and nowhere else: the box syncs *from* dags/ on a one-minute timer, and
  # a file dropped in there would be on its way into the scheduler before this script
  # had finished. The instance role can read the whole bucket, so no grant is needed.
  STAGED_KEY="uploads/$$-$NAME"
  echo "==> Staging $NAME in s3://$ARTIFACTS/$STAGED_KEY ($SIZE bytes)"
  aws s3 cp "$SRC" "s3://$ARTIFACTS/$STAGED_KEY" --region "$REGION" >/dev/null
  FETCH="aws s3 cp \"s3://$ARTIFACTS/$STAGED_KEY\" \"\$tmp\" --region \"$REGION\" >/dev/null"
fi

# Written to a temporary file in the destination directory and moved into place, so a
# transfer that dies half way leaves the existing file untouched rather than half
# overwritten. Same filesystem, so the move is atomic.
REMOTE_CMD=$(cat <<REMOTE
set -eu
if [ ! -d "$DEST_DIR" ]; then
  echo "no such directory on this box: $DEST_DIR" >&2
  exit 1
fi
if [ -e "$DEST" ] && [ "$FORCE_FLAG" != "1" ]; then
  echo "$DEST already exists — re-run with FORCE=1 to replace it" >&2
  ls -l "$DEST" >&2
  exit 1
fi
umask 022
tmp=\$(mktemp "$DEST_DIR/.put_file.XXXXXX")
trap 'rm -f "\$tmp"' EXIT
$FETCH
got=\$(sha256sum "\$tmp" | cut -d' ' -f1)
if [ "\$got" != "$SUM" ]; then
  echo "checksum mismatch: expected $SUM, got \$got" >&2
  exit 1
fi
chmod 0644 "\$tmp"
mv "\$tmp" "$DEST"
trap - EXIT
ls -l "$DEST"
REMOTE
)

# Built with json.dumps rather than by hand, for the reason shell.sh gives: the
# command carries quotes, braces and newlines, and every one of them would need
# escaping twice over on the way through --parameters.
PARAMS=$(printf '%s' "$REMOTE_CMD" | python3 -c \
  'import json,sys; print(json.dumps({"commands": [sys.stdin.read()]}))')

cleanup() {
  # The staged copy is a transport detail; it has no business outliving the transfer,
  # whether or not the transfer worked.
  if [[ -n "$STAGED_KEY" ]]; then
    aws s3 rm "s3://$ARTIFACTS/$STAGED_KEY" --region "$REGION" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "==> Sending $NAME ($SIZE bytes, $ROUTE) to $DEST on the $ENV Airflow box"
COMMAND_ID=$(aws ssm send-command \
  --instance-ids "$INSTANCE" \
  --region "$REGION" \
  --document-name AWS-RunShellScript \
  --comment "put_file.sh $NAME" \
  --parameters "$PARAMS" \
  --query 'Command.CommandId' --output text)

# The waiter gives up on a failed command as well as on a slow one, so its exit
# status is not the thing to report — the invocation below says what actually
# happened, and says it in the words the box used.
aws ssm wait command-executed \
  --command-id "$COMMAND_ID" --instance-id "$INSTANCE" --region "$REGION" 2>/dev/null || true

STATUS=$(aws ssm get-command-invocation \
  --command-id "$COMMAND_ID" --instance-id "$INSTANCE" --region "$REGION" \
  --query 'Status' --output text)
STDOUT=$(aws ssm get-command-invocation \
  --command-id "$COMMAND_ID" --instance-id "$INSTANCE" --region "$REGION" \
  --query 'StandardOutputContent' --output text)
STDERR=$(aws ssm get-command-invocation \
  --command-id "$COMMAND_ID" --instance-id "$INSTANCE" --region "$REGION" \
  --query 'StandardErrorContent' --output text)

if [[ "$STATUS" != "Success" ]]; then
  echo >&2
  echo "the copy failed on the box — $STATUS" >&2
  [[ -n "$STDERR" && "$STDERR" != "None" ]] && printf '%s\n' "$STDERR" >&2
  [[ -n "$STDOUT" && "$STDOUT" != "None" ]] && printf '%s\n' "$STDOUT" >&2
  exit 1
fi

cat <<DONE

  Copied      $SRC
  To          $DEST, on the $ENV Airflow box ($INSTANCE)
  sha256      $SUM — checked on the box before it was moved into place
  Owner       root, mode 0644

$(printf '  %s\n' "$STDOUT")
  Have a look with:  ./scripts/shell.sh $ENV

DONE
