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
# Only an existing deployment needs a pod refresh to pick up a rebuilt image; on a first
# deploy the pods start fresh, so we skip the restart to avoid churning kube-proxy's
# conntrack (which would briefly reset new client connections through the NodePort).
REDEPLOY=false
kubectl get deploy/wiggle-server >/dev/null 2>&1 && REDEPLOY=true
kubectl apply -f deploy/kind/wiggle-server.yaml
kubectl scale deploy/wiggle-server --replicas="$REPLICAS"
if [ "$REDEPLOY" = true ]; then
  echo "    redeploy: restarting pods to pick up the rebuilt image"
  kubectl rollout restart deploy/wiggle-server >/dev/null
fi
kubectl rollout status deploy/wiggle-server --timeout=180s
kubectl wait --for=condition=ready pod -l app=wiggle-server --timeout=120s >/dev/null

echo
echo "cluster is up: $REPLICAS server node(s) reachable at localhost:$NODEPORT"
kubectl get pods -l app=wiggle-server -o wide
echo
echo "next:  scripts/run-workers.sh <count>        # run workers locally against localhost:$NODEPORT"
echo "       kubectl logs -l app=wiggle-server -f  # tail server logs"
