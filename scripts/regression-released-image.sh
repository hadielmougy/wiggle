#!/usr/bin/env bash
#
# Regression against the RELEASED Docker image. Brings up docker-compose.full.yml (the published
# hadielmougy/wiggle:<tag> server + Postgres + dashboard), runs a host-side worker, submits a batch
# of orders, verifies they all reach COMPLETED, then tears everything down (containers + volume).
#
#   scripts/regression-released-image.sh [order-count]   # default 10
#
# Requires Docker and free host ports 8080 (gRPC) + 8090 (dashboard).
set -uo pipefail
cd "$(dirname "$0")/.."

COUNT="${1:-10}"
COMPOSE="docker compose -f docker-compose.full.yml"
WORKER_LOG="$(mktemp)"; SUBMIT_LOG="$(mktemp)"
WORKER_PID=""

cleanup() {
    [ -n "$WORKER_PID" ] && kill "$WORKER_PID" >/dev/null 2>&1 || true
    pkill -f "com.wiggle.order.WorkerMain" >/dev/null 2>&1 || true
    $COMPOSE down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

TAG="$(grep -oE 'image: hadielmougy/wiggle:[^ ]+' docker-compose.full.yml | head -1 | sed 's#.*:##')"
echo "==> regression against released image hadielmougy/wiggle:${TAG}"

echo "==> pulling + starting the full stack (server + Postgres)"
$COMPOSE up -d --pull always || { echo "compose up failed"; exit 1; }

echo "==> waiting for the wiggle server to report healthy"
cid="$($COMPOSE ps -q wiggle)"
deadline=$((SECONDS + 150))
until [ "$(docker inspect -f '{{.State.Health.Status}}' "$cid" 2>/dev/null)" = "healthy" ]; do
    (( SECONDS > deadline )) && { echo "server never became healthy:"; docker logs --tail 40 "$cid"; exit 1; }
    sleep 3
done
echo "    server healthy."

echo "==> starting a host-side worker against localhost:8080"
WIGGLE_URL=localhost:8080 ./gradlew --no-daemon -q :example:runWorker >"$WORKER_LOG" 2>&1 &
WORKER_PID=$!
for _ in $(seq 1 60); do grep -q "registered" "$WORKER_LOG" 2>/dev/null && break; sleep 2; done
grep -q "registered" "$WORKER_LOG" || { echo "worker failed to register:"; tail -n 20 "$WORKER_LOG"; exit 1; }
echo "    worker registered."

echo "==> submitting ${COUNT} orders"
WIGGLE_URL=localhost:8080 ./gradlew --no-daemon -q :example:submitOrders -Pcount="$COUNT" >"$SUBMIT_LOG" 2>&1
rc=$?

echo "==> polling the dashboard API until all ${COUNT} instances reach COMPLETED"
api="http://localhost:8090/api/instances?status=COMPLETED&limit=500"
deadline=$((SECONDS + 120))
completed=0
while (( SECONDS < deadline )); do
    completed="$(curl -s -u admin:change-me "$api" 2>/dev/null | grep -o 'COMPLETED' | wc -l | tr -d ' ')"
    [ "${completed:-0}" -ge "$COUNT" ] && break
    sleep 2
done

echo
echo "=========== released-image regression summary ==========="
echo "  image:      hadielmougy/wiggle:${TAG}"
echo "  orders:     ${COUNT}"
echo "  completed:  ${completed}"
if [ "$rc" -eq 0 ] && [ "${completed:-0}" -eq "$COUNT" ]; then
    echo "  result:     PASS"
    echo "========================================================="
    exit 0
fi
echo "  result:     FAIL (submit exit=${rc})"
echo "--- submit log (tail) ---"; tail -n 20 "$SUBMIT_LOG"
echo "--- worker log (tail) ---"; tail -n 15 "$WORKER_LOG"
echo "========================================================="
exit 1
