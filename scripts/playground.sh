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

# cell name : node ports (space-separated)
CELLS=("ns1:8081 8082" "ns2:8083 8084" "ns3:8085 8086")

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

    echo "== coordinator (own DB) on :$COORD_PORT =="
    WIGGLE_ROLE=coordinator WIGGLE_PORT=$COORD_PORT WIGGLE_NODE_NAME=coord \
        WIGGLE_JDBC_URL="$J/wiggle_coord" nohup "$SERVER_BIN" >"$LOGS/coord.log" 2>&1 &
    echo $! >> "$PIDS_FILE"
    sleep 5   # let the coordinator bind before cells register

    echo "== 3 cells x 2 members (each cell its own DB), registering with the coordinator =="
    for entry in "${CELLS[@]}"; do
        local ns=${entry%%:*}; local ports=${entry#*:}
        for p in $ports; do
            WIGGLE_PORT=$p WIGGLE_NODE_NAME="$ns-$p" WIGGLE_NAMESPACE="$ns" WIGGLE_CELL_ID="$ns" \
                WIGGLE_COORDINATOR_URL="$COORD_URL" WIGGLE_JDBC_URL="$J/wiggle_$ns" \
                nohup "$SERVER_BIN" >"$LOGS/$ns-$p.log" 2>&1 &
            echo $! >> "$PIDS_FILE"
        done
    done
    disown -a 2>/dev/null || true

    echo "== waiting for all 6 cell nodes to register with the coordinator =="
    for _ in $(seq 1 40); do
        [ "$(scalar -d wiggle_coord -c 'SELECT count(*) FROM coord_node;')" = "6" ] && break
        sleep 1
    done
    echo "  roster: $(scalar -d wiggle_coord -c 'SELECT count(*) FROM coord_node;') / 6 nodes"
    echo
    print_usage
}

print_usage() {
    cat <<EOF
== cluster is up (persistent -- run 'scripts/playground.sh down' to stop) ==

  coordinator : $COORD_URL
  ns1         : 127.0.0.1:8081  127.0.0.1:8082   (DB wiggle_ns1)
  ns2         : 127.0.0.1:8083  127.0.0.1:8084   (DB wiggle_ns2)
  ns3         : 127.0.0.1:8085  127.0.0.1:8086   (DB wiggle_ns3)
  logs        : $LOGS

Allocate / deallocate flows to namespaces (the CLI dials the coordinator by default):

  scripts/playground.sh cli allocate   scripts/playground/greet.yaml -n ns1
  scripts/playground.sh cli allocations -n ns1
  scripts/playground.sh cli allocate   scripts/playground/greet.yaml -n ns2
  scripts/playground.sh cli deallocate -w greet -n ns1

  scripts/playground.sh status         # roster + allocations across all namespaces

The CLI is '$CLI_BIN'; it defaults --coordinator to \$WIGGLE_COORDINATOR_URL else localhost:8099,
so 'export WIGGLE_COORDINATOR_URL=$COORD_URL' lets you call it directly too.
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
}

cli() {
    [ -x "$CLI_BIN" ] || { echo "CLI not built; run 'scripts/playground.sh up' first" >&2; exit 1; }
    WIGGLE_COORDINATOR_URL="$COORD_URL" "$CLI_BIN" "$@"
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
    cli)    shift; cli "$@" ;;
    *)      echo "usage: scripts/playground.sh {up|status|cli ...|down}" >&2; exit 2 ;;
esac
