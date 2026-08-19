#!/usr/bin/env bash
# Brings up three server nodes against one Postgres, plus two workers, and submits
# a batch of orders. Demonstrates leader election and pull-based work distribution.
#
# Prerequisites: a reachable Postgres, plus grpc/protobuf/JDBC on the classpath.
#   docker compose up -d postgres
#   ./gradlew :server:installDist          # produces a lib dir with every runtime jar
set -euo pipefail
cd "$(dirname "$0")/.."

JDBC_URL="${WIGGLE_JDBC_URL:-jdbc:postgresql://localhost:5432/wiggle}"
JDBC_USER="${WIGGLE_JDBC_USER:-wiggle}"
JDBC_PASSWORD="${WIGGLE_JDBC_PASSWORD:-wiggle}"
LIBS="$(ls server/build/install/server/lib/*.jar 2>/dev/null | paste -sd: -)"
CP="${WIGGLE_CLASSPATH:-out/classes:$LIBS}"

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

for port in 8080 8081 8082; do
  WIGGLE_PORT=$port \
  WIGGLE_NODE_NAME="node-$port" \
  WIGGLE_JDBC_URL="$JDBC_URL" \
  WIGGLE_JDBC_USER="$JDBC_USER" \
  WIGGLE_JDBC_PASSWORD="$JDBC_PASSWORD" \
    java -cp "$CP" dev.wiggle.server.WiggleServer > "out/server-$port.log" 2>&1 &
  pids+=($!)
done
sleep 5

echo "--- cluster membership (one leader expected) ---"
java -cp "$CP" dev.wiggle.example.ClusterStatus localhost:8080 | python3 -m json.tool || true

for id in w1 w2; do
  WIGGLE_URL=localhost:8080 WIGGLE_WORKER_ID=$id \
    java -cp "$CP" dev.wiggle.example.WorkerMain > "out/worker-$id.log" 2>&1 &
  pids+=($!)
done
sleep 3

WIGGLE_URL=localhost:8081 java -cp "$CP" dev.wiggle.example.SubmitOrders "${1:-20}"

echo
echo "Kill the node reported as leader above and re-run ClusterStatus to watch failover."
read -r -p "Press enter to shut everything down. "
