#!/usr/bin/env bash
# Runs N worker processes locally against the kind server cluster (or any WIGGLE_URL).
#
#   scripts/run-workers.sh <worker-count> [submit-count]
#
# Each worker is a separate JVM, so the workers' connections spread across the server
# pods behind the NodePort. Pass a second arg to also submit that many orders once the
# workers are up. Ctrl-C stops every worker.
#
#   WIGGLE_URL                 server endpoint     (default localhost:30080)
#   WIGGLE_WORKER_CONCURRENCY  slots per worker    (default 8)
set -euo pipefail
cd "$(dirname "$0")/.."

if [ $# -lt 1 ]; then
  echo "usage: scripts/run-workers.sh <worker-count> [submit-count]" >&2
  exit 1
fi
N="$1"
SUBMIT="${2:-0}"
URL="${WIGGLE_URL:-localhost:30080}"
CONCURRENCY="${WIGGLE_WORKER_CONCURRENCY:-8}"

command -v java >/dev/null 2>&1 || { echo "error: java is not installed or not on PATH" >&2; exit 1; }

echo "==> building the example distribution (./gradlew :example:installDist)"
./gradlew :example:installDist -q
CP="example/build/install/example/lib/*"

mkdir -p out
pids=()
cleanup() {
  echo
  echo "stopping workers"
  kill "${pids[@]:-}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "==> starting $N worker(s) against $URL (concurrency $CONCURRENCY)"
for i in $(seq 1 "$N"); do
  WIGGLE_URL="$URL" WIGGLE_WORKER_ID="worker-$i" WIGGLE_WORKER_CONCURRENCY="$CONCURRENCY" \
    java -cp "$CP" dev.wiggle.order.WorkerMain > "out/worker-$i.log" 2>&1 &
  pids+=("$!")
  echo "  worker-$i (pid $!) -> out/worker-$i.log"
done

if [ "$SUBMIT" -gt 0 ]; then
  sleep 3
  echo "==> submitting $SUBMIT orders"
  WIGGLE_URL="$URL" java -cp "$CP" dev.wiggle.order.SubmitOrders "$SUBMIT"
fi

echo
echo "workers running. Ctrl-C to stop. Logs in out/worker-*.log"
wait
