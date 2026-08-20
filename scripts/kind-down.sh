#!/usr/bin/env bash
# Tears down the kind cluster created by scripts/kind-up.sh.
set -euo pipefail
CLUSTER="${KIND_CLUSTER:-wiggle}"
command -v kind >/dev/null 2>&1 || { echo "error: kind is not installed" >&2; exit 1; }
echo "==> deleting kind cluster '$CLUSTER'"
kind delete cluster --name "$CLUSTER"
