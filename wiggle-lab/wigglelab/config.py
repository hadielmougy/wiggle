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
COORD_DEFAULT_GROUP_SIZE = 1    # start as a single-member group; scale up to an odd size (3, 5) for HA
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


# ---- editable pod tunables ----------------------------------------------------------------------
# The operational config the UI lets you see/edit and apply (→ pod redeploy). Each spec is
# {key: WIGGLE_* env, label, kind: int|float|bool|enum, default (None = server default), help,
# choices? (enum)}. Structural env the lab wires itself -- JDBC URL/user/pass, ports, cell id,
# namespace, coordinator URL, advertise host, region -- is deliberately NOT here and stays locked.

LOG_LEVELS = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR"]

CELL_TUNABLES = [
    {"key": "WIGGLE_POLL_INTERVAL_MILLIS", "label": "Poll interval (ms)", "kind": "int", "default": 200,
     "help": "Leader housekeeping / dispatch loop cadence."},
    {"key": "WIGGLE_HOUSEKEEPING_BATCH", "label": "Housekeeping batch", "kind": "int", "default": 500,
     "help": "Max timers/signals/reclaims swept per housekeeping pass."},
    {"key": "WIGGLE_LEASE_MILLIS", "label": "Task lease (ms)", "kind": "int", "default": None,
     "help": "How long a claimed task may run before it becomes reclaimable."},
    {"key": "WIGGLE_HEARTBEAT_INTERVAL_MILLIS", "label": "Heartbeat interval (ms)", "kind": "int",
     "default": None, "help": "Node heartbeat cadence for liveness / leader election."},
    {"key": "WIGGLE_MISSED_HEARTBEATS", "label": "Missed heartbeats", "kind": "int", "default": None,
     "help": "Missed heartbeats before a node is considered dead."},
    {"key": "WIGGLE_LONGPOLL_MAX_MILLIS", "label": "Long-poll max (ms)", "kind": "int", "default": None,
     "help": "Upper bound the server holds a worker long-poll open."},
    {"key": "WIGGLE_RETENTION_MILLIS", "label": "Retention (ms)", "kind": "int", "default": None,
     "help": "How long terminal instances are kept before purge."},
    {"key": "WIGGLE_QUEUE_LAG_CHECK_INTERVAL_MILLIS", "label": "Queue-lag check (ms)", "kind": "int",
     "default": None, "help": "How often queue lag is sampled."},
    {"key": "WIGGLE_QUEUE_LAG_WARN_MILLIS", "label": "Queue-lag warn (ms)", "kind": "int", "default": None,
     "help": "Queue lag above this logs a warning."},
    {"key": "WIGGLE_JDBC_POOL_SIZE", "label": "JDBC pool size", "kind": "int", "default": None,
     "help": "Max DB connections in the cell's pool."},
    {"key": "WIGGLE_MEMORY_SHEDDING_ENABLED", "label": "Memory shedding", "kind": "bool", "default": None,
     "help": "Shed a fraction of polls under heap pressure."},
    {"key": "WIGGLE_MEMORY_THRESHOLD", "label": "Memory threshold (0-1)", "kind": "float", "default": None,
     "help": "Heap utilisation at which shedding starts."},
    {"key": "WIGGLE_MEMORY_REJECT_RATIO", "label": "Memory reject ratio (0-1)", "kind": "float",
     "default": None, "help": "Fraction of polls shed once over threshold."},
    {"key": "WIGGLE_LOG_LEVEL", "label": "Log level", "kind": "enum", "default": None, "choices": LOG_LEVELS,
     "help": "Server log verbosity."},
]

COORD_TUNABLES = [
    {"key": "WIGGLE_LOG_LEVEL", "label": "Log level", "kind": "enum", "default": None, "choices": LOG_LEVELS,
     "help": "Coordinator log verbosity."},
]


def resolve_tunables(specs: list[dict], overrides: dict | None) -> dict:
    """The effective {env: value} for a tunable spec set: an override when given, else the spec default.
    ``None`` values are kept here and dropped later by the manifest env builder (⇒ server default)."""
    overrides = overrides or {}
    return {s["key"]: overrides.get(s["key"], s.get("default")) for s in specs}
