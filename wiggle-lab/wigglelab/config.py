"""Static configuration and naming conventions for the lab.

Everything the lab creates in Kubernetes is labelled ``app.kubernetes.io/part-of=wiggle-lab``
and lives in one namespace, so teardown and discovery are simple label selectors.
"""
from __future__ import annotations

import os

# kind cluster + k8s namespace the lab owns.
CLUSTER = os.environ.get("WIGGLE_LAB_CLUSTER", "wiggle-lab")
K8S_NAMESPACE = os.environ.get("WIGGLE_LAB_NAMESPACE", "wiggle-lab")

# The wiggle server image (built from the repo Dockerfile) loaded into kind.
IMAGE = os.environ.get("WIGGLE_LAB_IMAGE", "wiggle:local")

# Repo root, so the lab can build the image from the Dockerfile.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# In-cluster ports.
COORD_GRPC_PORT = 8099          # CoordinatorServer (CellCoordinator gRPC)
COORD_RAFT_PORT = 10000         # Apache Ratis peer transport (between coordinator pods)
COORD_DEFAULT_GROUP_SIZE = 3    # a real HA Ratis group (odd sizes give a majority)
CELL_GRPC_PORT = 8080           # WiggleControlPlane gRPC on a cell node
CELL_DASHBOARD_PORT = 8090      # cell web dashboard
DB_PORT = 5432

# Host-side local ports the lab forwards to (kubectl port-forward).
COORD_LOCAL_PORT = int(os.environ.get("WIGGLE_LAB_COORD_LOCAL_PORT", "18099"))
CELL_LOCAL_PORT_BASE = int(os.environ.get("WIGGLE_LAB_CELL_LOCAL_PORT_BASE", "18100"))
CELL_DASHBOARD_LOCAL_PORT_BASE = int(os.environ.get("WIGGLE_LAB_CELL_DASH_LOCAL_PORT_BASE", "18200"))

# Coordinator Ratis store (single-member embedded group; no external store).
COORD_STORE_URI = "ratis:///var/lib/wiggle/coord"
COORD_DATA_DIR = "/var/lib/wiggle/coord"

PART_OF = "wiggle-lab"


def labels(role: str, cell: str | None = None, namespace: str | None = None) -> dict[str, str]:
    """Standard label set. ``role`` is coordinator | cell | db."""
    lb = {"app.kubernetes.io/part-of": PART_OF, "wiggle-lab/role": role}
    if cell:
        lb["wiggle-lab/cell"] = cell
    if namespace:
        lb["wiggle-lab/namespace"] = namespace
    return lb


def dns_name(prefix: str, value: str) -> str:
    """A DNS-1123 safe resource name, e.g. ('cell', 'cellA') -> 'cell-cella'."""
    safe = "".join(c if (c.isalnum() or c == "-") else "-" for c in value.lower()).strip("-")
    return f"{prefix}-{safe}"
