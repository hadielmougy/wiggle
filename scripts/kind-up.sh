#!/usr/bin/env bash
# Brings up a full wiggle server cluster on a local kind (Kubernetes-in-Docker) cluster:
# a shared Postgres plus N server nodes, reachable from the host at localhost:30080.
#
#   scripts/kind-up.sh [node-count]     # default 3 server nodes
#
# Then run workers on your machine against it:  scripts/run-workers.sh <count>
# Tear everything down with:                    scripts/kind-down.sh
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${KIND_CLUSTER:-wiggle}"
IMAGE="wiggle-server:local"
REPLICAS="${1:-3}"
NODEPORT=30080

for tool in kind kubectl docker; do
  command -v "$tool" >/dev/null 2>&1 || { echo "error: '$tool' is not installed or not on PATH" >&2; exit 1; }
done

echo "==> building the server distribution (./gradlew :server:installDist)"
./gradlew :server:installDist

echo "==> building image $IMAGE"
docker build -t "$IMAGE" .

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "==> kind cluster '$CLUSTER' already exists"
else
  echo "==> creating kind cluster '$CLUSTER'"
  kind create cluster --name "$CLUSTER" --config deploy/kind/kind-cluster.yaml
fi

echo "==> loading image into the kind cluster"
kind load docker-image "$IMAGE" --name "$CLUSTER"

echo "==> deploying Postgres"
kubectl apply -f deploy/kind/postgres.yaml
kubectl rollout status deploy/postgres --timeout=120s

echo "==> deploying $REPLICAS wiggle server node(s)"
kubectl apply -f deploy/kind/wiggle-server.yaml
kubectl scale deploy/wiggle-server --replicas="$REPLICAS"
# Same image tag across runs, so nudge the pods to pick up the freshly loaded image.
kubectl rollout restart deploy/wiggle-server >/dev/null
kubectl rollout status deploy/wiggle-server --timeout=180s

echo
echo "cluster is up: $REPLICAS server node(s) reachable at localhost:$NODEPORT"
kubectl get pods -l app=wiggle-server -o wide
echo
echo "next:  scripts/run-workers.sh <count>        # run workers locally against localhost:$NODEPORT"
echo "       kubectl logs -l app=wiggle-server -f  # tail server logs"
