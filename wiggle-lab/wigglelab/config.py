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
# Every operational config the UI shows/edits and applies (→ pod redeploy), by raw WIGGLE_* env name.
# Each spec: {key, kind: int|float|bool|enum, default (the server's own default, shown as the current
# value when unset), help, choices? (enum), pin? (always emit even at default)}. Structural env the lab
# wires itself -- JDBC URL/user/pass, ports, cell id, namespace, coordinator URL, advertise host,
# region, TLS -- is deliberately NOT here and stays locked.
#
# Emit model: a var is written to the pod only when it's a `pin` (the lab's own baseline, e.g. a snappy
# 200ms poll) or when the user overrides its default; otherwise it's left off so the server uses its own
# default. `default` values mirror ServerConfig.fromEnvironment (and the two dispatch fields on
# WorkflowEngine), so the UI shows the true effective value for every knob.

LOG_LEVELS = ["TRACE", "DEBUG", "INFO", "WARNING", "ERROR"]

CELL_TUNABLES = [
    # Lab baselines -- pinned so a fresh cell keeps the snappy dev cadence (server defaults are 1000/100).
    {"key": "WIGGLE_POLL_INTERVAL_MILLIS", "kind": "int", "default": 200, "pin": True,
     "help": "Housekeeping / dispatch loop cadence. Lab default 200 (server default 1000)."},
    {"key": "WIGGLE_HOUSEKEEPING_BATCH", "kind": "int", "default": 500, "pin": True,
     "help": "Max timers/signals/reclaims swept per pass. Lab default 500 (server default 100)."},
    # In-memory dispatch (WorkflowEngine reads these from env; see docs/in-memory-dispatch.md).
    {"key": "WIGGLE_DISPATCH_LINGER_MILLIS", "kind": "int", "default": 5,
     "help": "Wake-on-produce batch linger; 0 = claim immediately (more, smaller round trips)."},
    {"key": "WIGGLE_FALLBACK_POLL_MILLIS", "kind": "int", "default": 100,
     "help": "Long-poll fallback re-claim interval (bounds cross-node dispatch latency)."},
    # Engine tunables (ServerConfig).
    {"key": "WIGGLE_LEASE_MILLIS", "kind": "int", "default": 30000,
     "help": "How long a claimed task may run before it becomes reclaimable."},
    {"key": "WIGGLE_HEARTBEAT_INTERVAL_MILLIS", "kind": "int", "default": 5000,
     "help": "Node heartbeat cadence for liveness / leader election."},
    {"key": "WIGGLE_MISSED_HEARTBEATS", "kind": "int", "default": 3,
     "help": "Missed heartbeats before a node is considered dead."},
    {"key": "WIGGLE_LONGPOLL_MAX_MILLIS", "kind": "int", "default": 20000,
     "help": "Upper bound the server holds a worker long-poll open."},
    {"key": "WIGGLE_RETENTION_MILLIS", "kind": "int", "default": 86_400_000,
     "help": "How long terminal instances are kept before purge."},
    {"key": "WIGGLE_QUEUE_LAG_CHECK_INTERVAL_MILLIS", "kind": "int", "default": 5000,
     "help": "How often queue lag is sampled."},
    {"key": "WIGGLE_QUEUE_LAG_WARN_MILLIS", "kind": "int", "default": 10000,
     "help": "Queue lag above this logs a warning."},
    {"key": "WIGGLE_JDBC_POOL_SIZE", "kind": "int", "default": 10,
     "help": "Max DB connections in the cell's pool."},
    # Memory-pressure load shedding (ServerConfig.Memory).
    {"key": "WIGGLE_MEMORY_SHEDDING_ENABLED", "kind": "bool", "default": False,
     "help": "Shed a fraction of polls under heap pressure."},
    {"key": "WIGGLE_MEMORY_THRESHOLD", "kind": "float", "default": 0.90,
     "help": "Heap utilisation at which shedding starts."},
    {"key": "WIGGLE_MEMORY_REJECT_RATIO", "kind": "float", "default": 0.10,
     "help": "Fraction of polls shed once over threshold."},
    {"key": "WIGGLE_MEMORY_RETRY_MILLIS", "kind": "int", "default": 2000,
     "help": "Hold-off hint returned to a shed poll."},
    {"key": "WIGGLE_MEMORY_RETRY_JITTER_MILLIS", "kind": "int", "default": 0,
     "help": "Random jitter added to the shed-poll hold-off."},
    {"key": "WIGGLE_LOG_LEVEL", "kind": "enum", "default": "INFO", "choices": LOG_LEVELS,
     "help": "File log level (only takes effect with WIGGLE_LOG_FILE set)."},
]

COORD_TUNABLES = [
    {"key": "WIGGLE_LOG_LEVEL", "kind": "enum", "default": "INFO", "choices": LOG_LEVELS,
     "help": "Coordinator file log level (only with WIGGLE_LOG_FILE). The coordinator runs the control "
             "plane, not the engine, so it has no engine tunables."},
]


def defaults(specs: list[dict]) -> dict:
    """The default (effective) value of every tunable -- what the UI shows when a var is unset."""
    return {s["key"]: s.get("default") for s in specs}


def to_env(specs: list[dict], overrides: dict | None) -> dict:
    """The {env: value} to actually put on the pod: pinned baselines always, plus any user overrides.
    Anything else is omitted so the server falls back to its own default. ``None`` values are dropped."""
    overrides = overrides or {}
    out = {}
    for s in specs:
        key = s["key"]
        if key in overrides:
            value = overrides[key]
        elif s.get("pin"):
            value = s.get("default")
        else:
            continue
        if value is not None:
            out[key] = value
    return out
