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
# real DB, epoch drain/retire (bump cell A's epoch, drain the old one, watch the reconciler retire
# it -- R21), and coordinator-driven failover expiry (kill -9 a cell leader, watch the coordinator
# reap it and the cell re-elect).
#
# Requires Docker. Runs in ~2 minutes (drain/retire and failover both wait out real timeouts).
# Everything is torn down on exit. Exit code 0 = all checks passed.
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
    pkill -f com.wiggle.dist.Main 2>/dev/null
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
    WIGGLE_PORT=$p WIGGLE_NODE_NAME=cellA-$p WIGGLE_NAMESPACE=nsA WIGGLE_CELL_ID=cellA \
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
# Election settles asynchronously and briefly tolerates 0/2 leaders during startup, so poll for
# exactly-one rather than snapshotting (otherwise the check races the election).
one_leader() {  # $1 = db
    for _ in $(seq 1 20); do
        [ "$(scalar -d "$1" -c 'SELECT count(*) FROM wf_node WHERE leader=1;')" = "1" ] && return 0
        sleep 1
    done
    return 1
}
one_leader wiggle_cella && pass "cell A elected exactly one leader" || bad "cell A leader count"
one_leader wiggle_cellb && pass "cell B elected exactly one leader" || bad "cell B leader count"
[ "$(scalar -d wiggle_coord -c "SELECT count(*) FROM pg_tables WHERE tablename='wf_token';")" = "0" ] \
    && pass "coordinator DB has no engine tables (wf_token)" || bad "coordinator DB leaked wf_token"
[ "$(scalar -d wiggle_coord -c "SELECT count(*) FROM pg_tables WHERE tablename='coord_policy';")" = "1" ] \
    && pass "coordinator DB has coord_policy" || bad "coordinator DB missing coord_policy"

echo "== drain/retire: bump cell A's epoch, drain the old one, watch it retire (R21) =="
# A small admin driver (compiled against the dist jars) drives the coordinator's OpenEpoch and the
# cell's start/cancel -- there is no CLI for these yet. It: opens epoch 0, starts 5 instances (epoch 0),
# opens epoch 1 (marking epoch 0 DRAINING), proves the minter shifts to epoch 1, then cancels the
# epoch-0 instances so their live count reaches zero. The reconciler then retires epoch 0.
# The dist bundles core/proto/server/grpc but not the client module; add its compiled classes so the
# driver can use WiggleClient + the workflow DSL (all its runtime deps are already in the dist jars).
./gradlew :client:classes -q 2>/dev/null
CP=$(printf '%s:' dist/build/install/wiggle/lib/*.jar)client/build/classes/java/main
mkdir -p "$LOGS/adminout"
cat >"$LOGS/Drain.java" <<'JAVA'
import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.IdCodec;
import com.wiggle.proto.CellCoordinatorGrpc;
import com.wiggle.proto.OpenEpochRequest;
import com.wiggle.proto.RingSlot;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Drain {
    public static void main(String[] a) throws Exception {
        String coordUrl = a[0], cellUrl = a[1], ns = a[2], cell = a[3];
        ManagedChannel ch = Grpc.newChannelBuilder(coordUrl, InsecureChannelCredentials.create()).build();
        CellCoordinatorGrpc.CellCoordinatorBlockingStub coord = CellCoordinatorGrpc.newBlockingStub(ch);
        try (WiggleClient client = new WiggleClient(cellUrl)) {
            client.register(Workflow.define("drainwf").step("a", c -> c).build());
            openEpoch(coord, ns, cell);                 // epoch 0 OPEN (generation 1)
            Thread.sleep(1000);
            List<String> epoch0 = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                String id = client.start("drainwf", Map.of());
                epoch0.add(id);
                System.out.println("EPOCH0_ID " + id);
            }
            openEpoch(coord, ns, cell);                 // epoch 1 OPEN, epoch 0 -> DRAINING (generation 2)
            System.out.println("OPENED_EPOCH1");
            boolean shifted = false;
            for (int i = 0; i < 60 && !shifted; i++) {  // wait for the node to re-fetch and shift its minter
                String id = client.start("drainwf", Map.of());
                long ep = IdCodec.parse(id).map(IdCodec.Placement::epoch).orElse(-1L);
                client.cancel(id, "probe");             // keep probes from lingering as live work
                if (ep == 1) { System.out.println("MINT_SHIFTED " + id); shifted = true; break; }
                Thread.sleep(1000);
            }
            if (!shifted) System.out.println("MINT_NOT_SHIFTED");
            for (String id : epoch0) client.cancel(id, "drain");   // epoch 0 -> zero live instances
            System.out.println("CANCELLED_EPOCH0");
        } finally {
            ch.shutdownNow();
        }
    }

    static void openEpoch(CellCoordinatorGrpc.CellCoordinatorBlockingStub coord, String ns, String cell) {
        coord.openEpoch(OpenEpochRequest.newBuilder().setNamespace(ns)
                .addRing(RingSlot.newBuilder().setShard(0).setCellId(cell).build()).build());
    }
}
JAVA
if javac -cp "$CP" -d "$LOGS/adminout" "$LOGS/Drain.java" 2>"$LOGS/admin-compile.log"; then
    java -cp "$LOGS/adminout:$CP" Drain "127.0.0.1:$COORD_PORT" "127.0.0.1:8081" nsA cellA >"$LOGS/drain.log" 2>&1
    grep -q "MINT_SHIFTED" "$LOGS/drain.log" \
        && pass "minter shifted to the new epoch after the bump" || bad "minter did not shift to epoch 1"
    grep -q "CANCELLED_EPOCH0" "$LOGS/drain.log" \
        && pass "epoch-0 instances drained (cancelled)" || bad "drain driver did not finish"

    status0() { scalar -d wiggle_coord -c "SELECT epochs::jsonb->'0'->>'status' FROM coord_policy WHERE namespace='nsA';"; }
    echo "  epoch 0 is '$(status0)' right after draining; waiting for the reconciler to retire it..."
    retired=""
    for _ in $(seq 1 40); do
        [ "$(status0)" = "RETIRED" ] && { retired=1; break; }
        sleep 1
    done
    [ -n "$retired" ] && pass "reconciler retired the drained epoch 0 (R21)" || bad "epoch 0 not retired (status=$(status0))"
    [ "$(status0)" = "RETIRED" ] \
        && [ "$(scalar -d wiggle_coord -c "SELECT epochs::jsonb->'1'->>'status' FROM coord_policy WHERE namespace='nsA';")" = "OPEN" ] \
        && pass "current epoch 1 stays OPEN while the old epoch retires" || bad "current epoch not OPEN"
else
    bad "admin driver failed to compile (see $LOGS/admin-compile.log)"
fi

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
