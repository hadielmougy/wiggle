#!/usr/bin/env bash
#
# A persistent local Wiggle cluster to play with, backed by a real database.
#
# Topology (one Postgres container, isolated databases):
#   - a COORDINATOR                    (own DB: wiggle_coord,  gRPC :8099)
#   - 3 cells, each a 2-member cluster, each with its OWN database, all registered with the coordinator:
#       ns1 -> DB wiggle_ns1  nodes :8081 :8082
#       ns2 -> DB wiggle_ns2  nodes :8083 :8084
#       ns3 -> DB wiggle_ns3  nodes :8085 :8086
#   (the two members of a cell share that cell's database -- that is what makes them one cell.)
#
# Then allocate/deallocate flows to namespaces with the `wiggle` CLI (see the printed hints).
#
# Usage:
#   scripts/playground.sh up       # build, start everything, print how to use it (persistent)
#   scripts/playground.sh status   # roster + per-namespace allocations
#   scripts/playground.sh cli ...  # run the wiggle CLI pointed at the coordinator, e.g.
#                                   #   scripts/playground.sh cli allocate scripts/playground/greet.yaml -n ns1
#   scripts/playground.sh down     # stop and remove everything
#
# Requires Docker. Nothing is torn down until you run `down`.
set -uo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)

PG_CONTAINER=${PG_CONTAINER:-wiggle-playground-pg}
PG_PORT=${PG_PORT:-5440}
PG_IMAGE=${PG_IMAGE:-postgres:16-alpine}
COORD_PORT=${COORD_PORT:-8099}
STATE="$ROOT/.playground"
LOGS="$STATE/logs"
PIDS_FILE="$STATE/pids"
SERVER_BIN="dist/build/install/wiggle/bin/wiggle"
CLI_BIN="cli/build/install/wiggle/bin/wiggle"
COORD_URL="127.0.0.1:$COORD_PORT"

# Single-cell namespaces (2 members each) -- "cell name : node ports".
CELLS=("ns1:8081 8082" "ns2:8083 8084" "ns3:8085 8086")

# A two-cell namespace 'orders': two cells (one member each), own DBs, sharing the namespace. A ring
# maps shard 0 -> orders-a, shard 1 -> orders-b, so a NamespaceWorker for 'orders' serves both cells.
ORDERS_NS="orders"
ORDERS_CELLS=("orders-a:8091" "orders-b:8092")

db_of() { echo "wiggle_$(echo "$1" | tr - _)"; }   # cell/ns name -> a valid db name

psql()   { docker exec "$PG_CONTAINER" psql -U postgres -tA "$@"; }
scalar() { psql "$@" | tr -d '[:space:]'; }

require_docker() { command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }; }

up() {
    require_docker
    mkdir -p "$LOGS"; : > "$PIDS_FILE"
    export WIGGLE_JDBC_USER=postgres WIGGLE_JDBC_PASSWORD=wiggle
    local J="jdbc:postgresql://localhost:$PG_PORT"

    echo "== building distribution + CLI =="
    ./gradlew :dist:installDist :cli:installDist -q || { echo "build failed" >&2; exit 1; }

    echo "== postgres ($PG_IMAGE) on :$PG_PORT =="
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
    docker run -d --name "$PG_CONTAINER" -e POSTGRES_PASSWORD=wiggle -p "$PG_PORT:5432" "$PG_IMAGE" >/dev/null
    for _ in $(seq 1 30); do docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break; sleep 1; done
    psql -c "CREATE DATABASE wiggle_coord;" >/dev/null
    for c in "${CELLS[@]}"; do psql -c "CREATE DATABASE wiggle_${c%%:*};" >/dev/null; done
    for c in "${ORDERS_CELLS[@]}"; do psql -c "CREATE DATABASE $(db_of "${c%%:*}");" >/dev/null; done

    echo "== coordinator (own DB) on :$COORD_PORT =="
    WIGGLE_ROLE=coordinator WIGGLE_PORT=$COORD_PORT WIGGLE_NODE_NAME=coord \
        WIGGLE_JDBC_URL="$J/wiggle_coord" nohup "$SERVER_BIN" >"$LOGS/coord.log" 2>&1 &
    echo $! >> "$PIDS_FILE"
    sleep 5   # let the coordinator bind before cells register

    echo "== 3 single-cell namespaces x 2 members (own DB each), registering with the coordinator =="
    for entry in "${CELLS[@]}"; do
        local ns=${entry%%:*}; local ports=${entry#*:}
        for p in $ports; do
            WIGGLE_PORT=$p WIGGLE_NODE_NAME="$ns-$p" WIGGLE_NAMESPACE="$ns" WIGGLE_CELL_ID="$ns" \
                WIGGLE_COORDINATOR_URL="$COORD_URL" WIGGLE_JDBC_URL="$J/wiggle_$ns" \
                nohup "$SERVER_BIN" >"$LOGS/$ns-$p.log" 2>&1 &
            echo $! >> "$PIDS_FILE"
        done
    done

    echo "== two-cell namespace '$ORDERS_NS': cells $(for c in "${ORDERS_CELLS[@]}"; do printf '%s ' "${c%%:*}"; done)(own DB each) =="
    for entry in "${ORDERS_CELLS[@]}"; do
        local cell=${entry%%:*}; local ports=${entry#*:}
        for p in $ports; do
            WIGGLE_PORT=$p WIGGLE_NODE_NAME="$cell-$p" WIGGLE_NAMESPACE="$ORDERS_NS" WIGGLE_CELL_ID="$cell" \
                WIGGLE_COORDINATOR_URL="$COORD_URL" WIGGLE_JDBC_URL="$J/$(db_of "$cell")" \
                nohup "$SERVER_BIN" >"$LOGS/$cell-$p.log" 2>&1 &
            echo $! >> "$PIDS_FILE"
        done
    done
    disown -a 2>/dev/null || true

    local want=8   # 6 single-cell nodes + 2 orders-cell nodes
    echo "== waiting for all $want cell nodes to register with the coordinator =="
    for _ in $(seq 1 40); do
        [ "$(scalar -d wiggle_coord -c 'SELECT count(*) FROM coord_node;')" = "$want" ] && break
        sleep 1
    done
    echo "  roster: $(scalar -d wiggle_coord -c 'SELECT count(*) FROM coord_node;') / $want nodes"

    echo "== configuring the '$ORDERS_NS' ring: shard 0 -> orders-a, shard 1 -> orders-b =="
    run_app ring "$COORD_URL" "$ORDERS_NS" orders-a orders-b >/dev/null && echo "  ring opened (epoch 0)"
    echo
    print_usage
}

print_usage() {
    cat <<EOF
== cluster is up (persistent -- run 'scripts/playground.sh down' to stop) ==

  coordinator      : $COORD_URL
  ns1 / ns2 / ns3  : single-cell namespaces, 2 members each  (:8081..:8086, DB wiggle_nsN)
  orders           : TWO-cell namespace  — orders-a :8091 (DB wiggle_orders_a), orders-b :8092 (DB wiggle_orders_b)
  logs             : $LOGS

Allocate / deallocate flows to namespaces (the CLI dials the coordinator by default):

  scripts/playground.sh cli allocate   scripts/playground/greet.yaml -n ns1
  scripts/playground.sh cli allocations -n ns1
  scripts/playground.sh cli deallocate -w greet -n ns1

  scripts/playground.sh status         # roster + allocations + ring across all namespaces

Run a coordinator-aware worker (NamespaceWorker) that fans out across a namespace's active cells:

  scripts/playground.sh worker ns1       # single-cell: one active cell
  scripts/playground.sh demo   ns1 5     # allocate greet + start 5 instances (the worker processes them)

Watch the worker's poll set SHIFT on the two-cell namespace:

  scripts/playground.sh worker orders             # (terminal 1) serves both cells -> active cells: [orders-a, orders-b]
  scripts/playground.sh demo   orders 5           # (terminal 2) allocate greet + start work
  scripts/playground.sh rebalance orders orders-a orders-a   # move both shards to orders-a; orders-b drains + retires
                                                  # -> terminal 1 shows active cells: [orders-a]
  scripts/playground.sh rebalance orders orders-a orders-b   # ... and back to two cells

Point the CLI at a server once and it sticks (saved in ~/.wiggle), then switch anytime:

  $CLI_BIN use coordinator $COORD_URL     # talk to the coordinator
  $CLI_BIN use cell 127.0.0.1:8081          # talk to one cell directly (e.g. for 'register')
  $CLI_BIN use                              # show the current target
  $CLI_BIN allocations -n ns1               # uses the saved target; --coordinator/--server override it

The CLI is '$CLI_BIN'. Precedence per command: explicit flag > env (WIGGLE_COORDINATOR_URL /
WIGGLE_URL) > saved target (wiggle use) > default.
EOF
}

status() {
    require_docker
    echo "== coordinator roster =="
    psql -d wiggle_coord -c \
        "SELECT namespace, cell_id, endpoint FROM coord_node ORDER BY namespace, endpoint;" 2>/dev/null \
        | sed 's/|/  /g; s/^/  /'
    echo "== allocations (coord_definition) =="
    psql -d wiggle_coord -c \
        "SELECT namespace, name, version FROM coord_definition ORDER BY namespace, name;" 2>/dev/null \
        | sed 's/|/  /g; s/^/  /'
    echo "== ring (coord_policy: namespace, current epoch, per-epoch status) =="
    psql -d wiggle_coord -c \
        "SELECT namespace, current_epoch,
                (SELECT string_agg(key || '=' || (value->>'status'), ' ' ORDER BY key)
                   FROM jsonb_each(epochs::jsonb)) AS epochs
           FROM coord_policy ORDER BY namespace;" 2>/dev/null \
        | sed 's/|/  /g; s/^/  /'
}

cli() {
    [ -x "$CLI_BIN" ] || { echo "CLI not built; run 'scripts/playground.sh up' first" >&2; exit 1; }
    WIGGLE_COORDINATOR_URL="$COORD_URL" "$CLI_BIN" "$@"
}

# ---- coordinator-aware worker (NamespaceWorker) ----
# A tiny Java app compiled against the CLI's bundled jars (which include the client + NamespaceWorker).
APP_OUT="$STATE/appout"
app_cp() { printf '%s:' cli/build/install/wiggle/lib/*.jar; }

compile_app() {
    [ -f "$APP_OUT/PlaygroundWorker.class" ] && return 0
    [ -d cli/build/install/wiggle/lib ] || { echo "run 'scripts/playground.sh up' first" >&2; exit 1; }
    mkdir -p "$APP_OUT"
    cat > "$STATE/PlaygroundWorker.java" <<'JAVA'
import dev.wiggle.client.CellResolver;
import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.worker.NamespaceWorker;
import dev.wiggle.core.Tls;
import dev.wiggle.proto.CellCoordinatorGrpc;
import dev.wiggle.proto.OpenEpochRequest;
import dev.wiggle.proto.RingSlot;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Runs a coordinator-aware NamespaceWorker, starts sample 'greet' instances, or (re)opens a ring. */
public class PlaygroundWorker {
    public static void main(String[] a) throws Exception {
        String mode = a[0], coord = a[1], ns = a[2];
        if (mode.equals("ring")) { ring(coord, ns, a); return; }
        CellResolver resolver = CellResolver.coordinator(coord, Tls.Options.DISABLED, "");
        if (mode.equals("worker")) worker(resolver, ns);
        else start(resolver, ns, a.length > 3 ? Integer.parseInt(a[3]) : 3);
    }

    /** ring <coord> <ns> <cellId-for-shard0> <cellId-for-shard1> ... -> OpenEpoch with that ring. */
    static void ring(String coord, String ns, String[] a) {
        ManagedChannel ch = Grpc.newChannelBuilder(coord, InsecureChannelCredentials.create()).build();
        try {
            OpenEpochRequest.Builder req = OpenEpochRequest.newBuilder().setNamespace(ns);
            for (int shard = 3; shard < a.length; shard++) {
                req.addRing(RingSlot.newBuilder().setShard(shard - 3).setCellId(a[shard]).build());
            }
            CellCoordinatorGrpc.newBlockingStub(ch).openEpoch(req.build());
            StringBuilder sb = new StringBuilder();
            for (int shard = 3; shard < a.length; shard++) sb.append(" s").append(shard - 3).append("->").append(a[shard]);
            System.out.println("opened epoch for '" + ns + "' ring:" + sb);
        } finally {
            ch.shutdownNow();
        }
    }

    static void worker(CellResolver resolver, String ns) throws Exception {
        // Identity handlers for the sample greet flow; the worker only needs handlers by name.
        NamespaceWorker nw = new NamespaceWorker(resolver, ns, "playground", w -> {
            w.handle("greet", "hello", c -> { System.out.println("  [worker] greet#hello"); return c; });
            w.handle("greet", "world", c -> { System.out.println("  [worker] greet#world"); return c; });
        });
        nw.reconcileEvery(Duration.ofSeconds(3)).start();
        System.out.println("worker serving namespace '" + ns + "' -- watching active cells (Ctrl-C to stop)");
        Set<String> last = Set.of();
        while (true) {
            Set<String> cur = nw.activeCells();
            if (!cur.equals(last)) { System.out.println("  active cells: " + new TreeSet<>(cur)); last = cur; }
            Thread.sleep(1000);
        }
    }

    static void start(CellResolver resolver, String ns, int n) throws Exception {
        WiggleClient client = resolver.clientForNamespace(ns);
        for (int i = 0; i < n; i++) {
            String id = client.start("greet", Map.of());
            String status = client.awaitCompletion(id, Duration.ofSeconds(15)).status();
            System.out.println(status + "  " + id);
        }
        resolver.close();
    }
}
JAVA
    javac -cp "$(app_cp)" -d "$APP_OUT" "$STATE/PlaygroundWorker.java" \
        || { echo "worker compile failed" >&2; exit 1; }
}

run_app() { compile_app; java -cp "$APP_OUT:$(app_cp)" PlaygroundWorker "$@"; }

worker() { run_app worker "$COORD_URL" "${1:?usage: scripts/playground.sh worker <namespace>}"; }

demo() {
    local ns=${1:?usage: scripts/playground.sh demo <namespace> [count]}; local n=${2:-3}
    echo "== allocate greet -> $ns, then start $n instance(s) via the coordinator =="
    cli allocate scripts/playground/greet.yaml -n "$ns" >/dev/null
    run_app start "$COORD_URL" "$ns" "$n"
}

# rebalance <ns> <cellId-for-shard0> [cellId-for-shard1 ...] -- open a new epoch with that ring.
# The previous epoch drains and, once empty, retires; a worker for <ns> then drops the dropped cell.
rebalance() {
    local ns=${1:?usage: scripts/playground.sh rebalance <ns> <cellId0> [cellId1 ...]}; shift
    [ "$#" -ge 1 ] || { echo "give at least one cell id (for shard 0)" >&2; exit 2; }
    run_app ring "$COORD_URL" "$ns" "$@"
}

down() {
    echo "== stopping cluster =="
    if [ -f "$PIDS_FILE" ]; then
        while read -r p; do [ -n "$p" ] && kill "$p" 2>/dev/null; done < "$PIDS_FILE"
    fi
    pkill -f dev.wiggle.dist.Main 2>/dev/null
    docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
    rm -rf "$STATE"
    echo "  done"
}

case "${1:-up}" in
    up)     up ;;
    status) status ;;
    down)   down ;;
    cli)       shift; cli "$@" ;;
    worker)    shift; worker "$@" ;;
    demo)      shift; demo "$@" ;;
    rebalance) shift; rebalance "$@" ;;
    *)         echo "usage: scripts/playground.sh {up|status|cli ...|worker <ns>|demo <ns> [n]|rebalance <ns> <cell0> [cell1]|down}" >&2; exit 2 ;;
esac
