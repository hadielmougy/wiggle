#!/usr/bin/env bash
#
# Cell-coordinator integration test against a real database.
#
# Stands up, on one Postgres instance with three isolated databases:
#   - a COORDINATOR node          (own DB: wiggle_coord)
#   - cell A: a 2-node cluster    (own DB: wiggle_cella, namespace nsA)
#   - cell B: a 2-node cluster    (own DB: wiggle_cellb, namespace nsB)
# all four cell nodes pointed at the coordinator via WIGGLE_COORDINATOR_URL.
#
# Verifies: node registration + roster, per-cluster leader election, role/schema isolation on a
# real DB, and coordinator-driven failover expiry (kill -9 a cell leader, watch the coordinator
# reap it and the cell re-elect).
#
# Requires Docker. Runs in ~1 minute (failover expiry waits out the dead-node timeout). Everything
# is torn down on exit. Exit code 0 = all checks passed.
#
#   scripts/coordinator-integration.sh
#
# Timing note: the coordinator reaps a node after missedHeartbeats * heartbeatInterval (default
# 3 * 5s = 15s), and a node heartbeats the coordinator every ~10s, so the dead timeout must stay
# above the node heartbeat interval. This script uses the defaults, which satisfy that.
set -uo pipefail   # deliberately not -e: run every check, then report
cd "$(dirname "$0")/.."

PG_CONTAINER=${PG_CONTAINER:-wiggle-coord-it-pg}
PG_PORT=${PG_PORT:-5433}
PG_IMAGE=${PG_IMAGE:-postgres:16-alpine}
COORD_PORT=${COORD_PORT:-8099}
BIN=dist/build/install/wiggle/bin/wiggle
LOGS=$(mktemp -d)
declare -a PIDS=()
fail=0

pass() { echo "  PASS: $1"; }
bad()  { echo "  FAIL: $1"; fail=1; }
psql() { docker exec "$PG_CONTAINER" psql -U postgres -tA "$@"; }
scalar() { psql "$@" | tr -d '[:space:]'; }

cleanup() {
    echo "== teardown =="
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
    pkill -f dev.wiggle.dist.Main 2>/dev/null
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
    rm -rf "$LOGS"
}
trap cleanup EXIT

command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }
[ -x "$BIN" ] || { echo "== building distribution =="; ./gradlew :dist:installDist -q; }

echo "== postgres ($PG_IMAGE) on :$PG_PORT =="
docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
docker run -d --name "$PG_CONTAINER" -e POSTGRES_PASSWORD=wiggle -p "$PG_PORT:5432" "$PG_IMAGE" >/dev/null
for _ in $(seq 1 30); do docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
for db in wiggle_coord wiggle_cella wiggle_cellb; do psql -c "CREATE DATABASE $db;" >/dev/null; done

export WIGGLE_JDBC_USER=postgres WIGGLE_JDBC_PASSWORD=wiggle
J="jdbc:postgresql://localhost:$PG_PORT"

echo "== launch coordinator + 2 cell clusters (2 nodes each) =="
WIGGLE_ROLE=coordinator WIGGLE_PORT=$COORD_PORT WIGGLE_NODE_NAME=coord \
    WIGGLE_JDBC_URL=$J/wiggle_coord "$BIN" >"$LOGS/coord.log" 2>&1 &
PIDS+=($!)
sleep 5   # let the coordinator bind before cells try to register
for p in 8081 8082; do
    WIGGLE_PORT=$p WIGGLE_NODE_NAME=cellA-$p WIGGLE_NAMESPACE=nsA \
        WIGGLE_COORDINATOR_URL=127.0.0.1:$COORD_PORT WIGGLE_JDBC_URL=$J/wiggle_cella \
        "$BIN" >"$LOGS/cellA-$p.log" 2>&1 &
    PIDS+=($!)
done
for p in 8083 8084; do
    WIGGLE_PORT=$p WIGGLE_NODE_NAME=cellB-$p WIGGLE_NAMESPACE=nsB \
        WIGGLE_COORDINATOR_URL=127.0.0.1:$COORD_PORT WIGGLE_JDBC_URL=$J/wiggle_cellb \
        "$BIN" >"$LOGS/cellB-$p.log" 2>&1 &
    PIDS+=($!)
done
disown -a 2>/dev/null || true   # kill the leader later without a noisy async job notification

echo "== waiting for all four nodes to register =="
for _ in $(seq 1 30); do
    [ "$(scalar -d wiggle_coord -c 'SELECT count(*) FROM coord_node;')" = "4" ] && break
    sleep 1
done

echo "== checks =="
[ "$(scalar -d wiggle_coord -c "SELECT count(*) FROM coord_node WHERE namespace='nsA';")" = "2" ] \
    && pass "coordinator roster nsA = 2" || bad "coordinator roster nsA"
[ "$(scalar -d wiggle_coord -c "SELECT count(*) FROM coord_node WHERE namespace='nsB';")" = "2" ] \
    && pass "coordinator roster nsB = 2" || bad "coordinator roster nsB"
[ "$(scalar -d wiggle_cella -c 'SELECT count(*) FROM wf_node WHERE leader=1;')" = "1" ] \
    && pass "cell A elected exactly one leader" || bad "cell A leader count"
[ "$(scalar -d wiggle_cellb -c 'SELECT count(*) FROM wf_node WHERE leader=1;')" = "1" ] \
    && pass "cell B elected exactly one leader" || bad "cell B leader count"
[ "$(scalar -d wiggle_coord -c "SELECT count(*) FROM pg_tables WHERE tablename='wf_token';")" = "0" ] \
    && pass "coordinator DB has no engine tables (wf_token)" || bad "coordinator DB leaked wf_token"
[ "$(scalar -d wiggle_coord -c "SELECT count(*) FROM pg_tables WHERE tablename='coord_policy';")" = "1" ] \
    && pass "coordinator DB has coord_policy" || bad "coordinator DB missing coord_policy"

echo "== failover: kill cell A leader (kill -9, no graceful deregister) =="
lname=$(scalar -d wiggle_cella -c 'SELECT name FROM wf_node WHERE leader=1;')
lport=${lname#cellA-}
echo "  cell A leader is $lname (port $lport)"
kill -9 "$(lsof -ti tcp:"$lport" | head -1)" 2>/dev/null

echo "  waiting for the coordinator to reap it (dead timeout ~15s)..."
nsA=$(scalar -d wiggle_coord -c "SELECT count(*) FROM coord_node WHERE namespace='nsA';")
for _ in $(seq 1 40); do
    nsA=$(scalar -d wiggle_coord -c "SELECT count(*) FROM coord_node WHERE namespace='nsA';")
    [ "$nsA" = "1" ] && break
    sleep 1
done
[ "$nsA" = "1" ] && pass "coordinator expired the dead node (nsA roster = 1)" || bad "dead node not expired (nsA=$nsA)"

reelected=$(scalar -d wiggle_cella -c \
    "SELECT count(*) FROM wf_node WHERE leader=1 AND (EXTRACT(EPOCH FROM now())*1000)::bigint - last_heartbeat < 15000;")
[ "${reelected:-0}" -ge 1 ] && pass "cell A has a live leader after failover" || bad "cell A did not re-elect a live leader"

echo
if [ "$fail" = "0" ]; then echo "INTEGRATION: PASS"; else echo "INTEGRATION: FAIL"; fi
exit "$fail"
