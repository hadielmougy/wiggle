#!/usr/bin/env bash
#
# Open a placement epoch (a shard->cell ring) for a namespace via the coordinator -- i.e. reshard a
# namespace across cells. Wraps `wiggle open-epoch`.
#
#   scripts/open-epoch.sh <namespace> <shard=cell>...
#
# Examples:
#   scripts/open-epoch.sh ns1 0=ns1 1=cellB          # split ns1 across its original cell + cellB
#   scripts/open-epoch.sh ns1 0=ns1 1=ns1 2=cellB 3=cellB
#
# Env: WIGGLE_COORDINATOR_URL (default 127.0.0.1:8099).
set -uo pipefail
cd "$(dirname "$0")/.."

export WIGGLE_COORDINATOR_URL=${WIGGLE_COORDINATOR_URL:-127.0.0.1:8099}
[ $# -ge 2 ] || { echo "usage: scripts/open-epoch.sh <namespace> <shard=cell>..." >&2; exit 2; }
ns=$1; shift

[ -x cli/build/install/wiggle/bin/wiggle ] || { echo "== building :cli:installDist =="; ./gradlew :cli:installDist -q; }
exec cli/build/install/wiggle/bin/wiggle open-epoch -n "$ns" -c "$WIGGLE_COORDINATOR_URL" "$@"
