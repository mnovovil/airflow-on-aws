#!/usr/bin/env bash
# Browse the STAC catalogue in STAC Browser, on this machine.
#
#   ./scripts/stac_ui.sh                 # dev, the default
#   ./scripts/stac_ui.sh prod            # the live one
#   ./scripts/stac_ui.sh dev 8090        # somewhere other than this env's usual port
#
# The catalogue is static JSON in a bucket with public access blocked, and STAC
# Browser is a single-page app that runs in *your* browser — so it can only read the
# catalogue over HTTP, and only from somewhere it is allowed to fetch. This copies
# the tree down and serves it out of the browser image's own web root, which makes
# the app and the JSON the same origin: no CORS configuration anywhere, and nothing
# on the catalogue's side has to become reachable from the internet to look at it.
#
# Nothing here writes to S3 or to AWS at all. It is a read-only sync and a container.
set -euo pipefail

# Defaults to dev for the same reason smoke_test.sh and airflow_ui.sh do. This one
# only reads, so pointing it at prod is harmless — but the two catalogues look alike
# once they are open, and guessing which one is on screen is the failure mode worth
# designing out.
ENV="${1:-dev}"
REGION="${AWS_REGION:-eu-north-1}"

# Pinned rather than :latest. The image carries the whole UI, and a silent major
# version bump between two runs would change what the catalogue looks like without
# anything in this repo having changed. Override to try a newer one.
IMAGE="${STAC_BROWSER_IMAGE:-ghcr.io/radiantearth/stac-browser:5.0.0}"

# See airflow_ui.sh: the default profile is a browser session that expires, and
# ice-admin is the long-lived user the rest of the README assumes.
export AWS_PROFILE="${AWS_PROFILE:-ice-admin}"

if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
  echo "unknown environment '$ENV' — expected dev or prod" >&2
  echo "usage: $0 [dev|prod] [local-port]" >&2
  exit 1
fi

# Same convention as airflow_ui.sh, one range down: the lower number is prod.
if [[ "$ENV" == "prod" ]]; then
  DEFAULT_PORT=8084
else
  DEFAULT_PORT=8085
fi
PORT="${2:-$DEFAULT_PORT}"

# The port is part of the name, not just the environment. Serving the same catalogue
# twice on two ports is a thing this script invites you to do — the port-in-use error
# below says so — and a name keyed only on the environment makes the second one fail
# on a docker name conflict instead.
CONTAINER="ice-stac-browser-$ENV-$PORT"

cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null; then
  echo "docker is not on PATH — STAC Browser is distributed as an image" >&2
  exit 1
fi

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

# Fail here rather than several seconds later inside docker, where a port already in
# use surfaces as a wall of daemon error.
if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then
  exec 3<&- 3>&-
  echo "local port $PORT is already in use — another browser is probably still open" >&2
  echo "close it, or pass a different port:  $0 $ENV 8090" >&2
  exit 1
fi

# `--rm` clears the name on a normal exit, so a leftover here means the last run was
# killed in a way that skipped it. Same reasoning as the port check: catch it now,
# with the command that fixes it, rather than inside docker as a wall of daemon error.
if [[ -n "$(docker ps -aq --filter "name=^/$CONTAINER\$")" ]]; then
  echo "a container named $CONTAINER is left over from an earlier run" >&2
  echo "remove it and try again:  docker rm -f $CONTAINER" >&2
  exit 1
fi

# The backend is partial, so the working directory is initialised against one
# environment at a time and `terraform output` reads whichever that was. Re-init
# rather than trust it — the two catalogues are indistinguishable once they are open.
echo "==> Selecting the $ENV state"
"$TF" -chdir=infra init -input=false -reconfigure \
  -backend-config="backends/$ENV.hcl" >/dev/null

STATE_ENV=$(tf_out env)
if [[ "$STATE_ENV" != "$ENV" ]]; then
  echo "state says '$STATE_ENV' but '$ENV' was requested — refusing to continue" >&2
  exit 1
fi

BUCKET=$(tf_out artifacts_bucket)

# Per environment, so the two catalogues never overwrite each other on disk and a
# stale copy of one cannot be served under the other's name.
CATALOG_DIR="local/stac/$ENV"
mkdir -p "$CATALOG_DIR"

echo "==> Syncing s3://$BUCKET/stac/ into $CATALOG_DIR"
# --delete because this is a mirror, not an accumulation: an item removed upstream
# should stop appearing here, and the collection's link list would 404 on it anyway.
aws s3 sync "s3://$BUCKET/stac/" "$CATALOG_DIR/" --region "$REGION" --delete --only-show-errors

# gdalinfo_notify writes items and nothing else; catalog.json and collection.json are
# written by stac_publish. An environment where that DAG has never run therefore has
# items on S3 and no root to enter the tree by, and STAC Browser's symptom for that
# is a blank page rather than an error.
if [[ ! -f "$CATALOG_DIR/catalog.json" ]]; then
  cat >&2 <<MISSING

No catalog.json under s3://$BUCKET/stac/.

Items are written per raster by gdalinfo_notify, but the root catalogue and the
collection are written by the stac_publish DAG. Trigger it once and re-run this:

  ./scripts/airflow_ui.sh $ENV     # then trigger stac_publish

MISSING
  exit 1
fi

ITEMS=$(find "$CATALOG_DIR" -path '*/items/*.json' -type f | wc -l)
URL="http://localhost:$PORT/"

cat <<BANNER

  Environment  $ENV
  Catalogue    s3://$BUCKET/stac/  ($ITEMS item(s), copied to $CATALOG_DIR)
  Browser      $URL

  Asset hrefs are s3:// — the buckets block public access, so assets are listed
  but not fetchable from the page. Metadata, geometry and band statistics all
  render. Re-run this to pick up newly published items.

  Ctrl-C stops the container.

BANNER

# Open the browser once nginx is actually accepting connections rather than
# immediately onto a refused port. Backgrounded because the container below holds
# the terminal. OPEN_BROWSER=0 to skip it.
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

echo "==> Serving $CATALOG_DIR on $URL"
# The mount is what makes this same-origin: the catalogue is served by the same
# nginx that serves the app, out of a path under its web root, so catalogUrl is a
# relative path and no cross-origin request happens at all. Read-only because the
# container has no business writing to a directory `aws s3 sync --delete` owns.
#
# The image's nginx tries real files before falling back to index.html, which is
# both halves of this working: /stac/... resolves to the JSON on disk, while the
# app's own routes (/elevation/collection.json and friends) are not files and fall
# through to the SPA.
exec docker run --rm --name "$CONTAINER" \
  -p "$PORT:8080" \
  -v "$PWD/$CATALOG_DIR:/usr/share/nginx/html/stac:ro" \
  -e SB_catalogUrl=/stac/catalog.json \
  -e SB_catalogTitle="ice — $ENV" \
  "$IMAGE"
