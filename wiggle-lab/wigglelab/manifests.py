"""Kubernetes manifest builders. Everything is one image (the wiggle dist), specialised by env:
a coordinator (WIGGLE_ROLE=coordinator, over its own small Postgres) and per-cell (its own Postgres +
wiggle nodes pointed at that DB and at the coordinator)."""
from __future__ import annotations

import yaml

from . import config as C


def _env(pairs: dict) -> list[dict]:
    out = []
    for k, v in pairs.items():
        if v is None:
            continue
        out.append({"name": k, "value": str(v)})
    return out


def _node_name_env() -> dict:
    return {"name": "WIGGLE_NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}}


def _pod_ip_env() -> dict:
    # The node advertises its pod IP to the coordinator, which fans workflows out to n.endpoint()
    # pod-to-pod. Without this it advertises 127.0.0.1 (the server default) and the coordinator dials
    # its own pod → INTERNAL on RegisterWorkflow.
    return {"name": "WIGGLE_ADVERTISE_HOST", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}


def _deployment(name, labels, replicas, container) -> dict:
    return {
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": name, "namespace": C.K8S_NAMESPACE, "labels": labels},
        "spec": {
            "replicas": replicas,
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": {**labels, "app": name}},
                "spec": {"containers": [container], **container.pop("_pod", {})},
            },
        },
    }


def _service(name, labels, port, target_port) -> dict:
    return {
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": C.K8S_NAMESPACE, "labels": labels},
        "spec": {"selector": {"app": name}, "ports": [{"port": port, "targetPort": target_port}]},
    }


def namespace_manifest() -> dict:
    return {"apiVersion": "v1", "kind": "Namespace",
            "metadata": {"name": C.K8S_NAMESPACE, "labels": {"app.kubernetes.io/part-of": C.PART_OF}}}


def coordinator_db_manifests() -> list[dict]:
    """The control plane's own Postgres. Separate from every cell's database on purpose -- a cell must
    never know about coordinators -- and, like the cell databases here, it has no volume: this is a lab,
    and deleting the pod is how you reset the control plane."""
    labels = C.labels("coord-db")
    container = {
        "name": "postgres", "image": "postgres:16-alpine",
        "env": _env({"POSTGRES_DB": C.COORD_DB, "POSTGRES_USER": C.COORD_DB_USER,
                     "POSTGRES_PASSWORD": C.COORD_DB_PASSWORD}),
        "ports": [{"containerPort": C.DB_PORT}],
        # TCP probe, not the unix socket -- see cell_db_manifests for why.
        "readinessProbe": {"exec": {"command": ["pg_isready", "-h", "127.0.0.1", "-U", C.COORD_DB_USER]},
                           "initialDelaySeconds": 3, "periodSeconds": 3},
    }
    return [_deployment(C.COORD_DB_NAME, labels, 1, container),
            _service(C.COORD_DB_NAME, labels, C.DB_PORT, C.DB_PORT)]


def coordinator_manifests(replicas: int = C.COORD_DEFAULT_REPLICAS,
                          tunables: dict | None = None) -> list[dict]:
    """``replicas`` coordinator pods over one shared database, as an ordinary Deployment behind an
    ordinary Service.

    Coordinators hold no state of their own, so there is nothing here that used to be needed when the
    control plane was an embedded Raft group: no StatefulSet, no per-pod volume, no peer transport
    port, no headless Service publishing not-ready addresses to break a bootstrap deadlock, and no
    fixed peer list pinning the size at deploy time. Any pod serves the same state because they read
    the same database, and scaling is just a replica count.

    They elect one leader between themselves -- the same announce-and-heartbeat election the cells run
    -- and only the leader runs the reconcile/retire loop."""
    labels = C.labels("coordinator")

    container = {
        "name": "coordinator", "image": C.IMAGE, "imagePullPolicy": "IfNotPresent",
        "ports": [{"containerPort": C.COORD_GRPC_PORT, "name": "grpc"}],
        "env": [
            # The id this pod announces in the coordinator roster, and so what the election names.
            {"name": "WIGGLE_NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            *_env({
                "WIGGLE_ROLE": "coordinator",
                "WIGGLE_PORT": C.COORD_GRPC_PORT,
                "WIGGLE_COORD_STORE": C.COORD_STORE_URI,
                "WIGGLE_COORD_JDBC_USER": C.COORD_DB_USER,
                "WIGGLE_COORD_JDBC_PASSWORD": C.COORD_DB_PASSWORD,
            }),
            # Operational tunables from the UI (thin for the coordinator -- it runs CoordinatorServer,
            # not the engine); unset ones fall back to server defaults.
            *_env(C.to_env(C.COORD_TUNABLES, tunables)),
        ],
        "readinessProbe": {"tcpSocket": {"port": C.COORD_GRPC_PORT},
                           "initialDelaySeconds": 5, "periodSeconds": 3},
    }
    return [_service("coordinator", labels, C.COORD_GRPC_PORT, C.COORD_GRPC_PORT),
            _deployment("coordinator", labels, replicas, container)]


def cell_db_manifests(cell: str) -> list[dict]:
    name = C.dns_name("db", cell)
    labels = C.labels("db", cell=cell)
    container = {
        "name": "postgres", "image": "postgres:16-alpine",
        "env": _env({"POSTGRES_DB": "wiggle", "POSTGRES_USER": "wiggle", "POSTGRES_PASSWORD": "wiggle"}),
        "ports": [{"containerPort": C.DB_PORT}],
        # Probe over TCP (-h 127.0.0.1), not the unix socket: the postgres image's first-boot init runs a
        # temporary server with TCP disabled, so a socket probe would mark the pod Ready mid-init. A client
        # that connects then gets "terminating connection due to administrator command" when the init
        # server shuts down. TCP probing stays not-Ready until the real server accepts connections.
        "readinessProbe": {"exec": {"command": ["pg_isready", "-h", "127.0.0.1", "-U", "wiggle"]},
                           "initialDelaySeconds": 3, "periodSeconds": 3},
    }
    return [_deployment(name, labels, 1, container), _service(name, labels, C.DB_PORT, C.DB_PORT)]


def cell_manifests(cell: str, namespace: str, replicas: int, region: str = "",
                   tunables: dict | None = None) -> list[dict]:
    name = C.dns_name("cell", cell)
    db = C.dns_name("db", cell)
    labels = C.labels("cell", cell=cell, namespace=namespace)
    container = {
        "name": "wiggle", "image": C.IMAGE, "imagePullPolicy": "IfNotPresent",
        "ports": [{"containerPort": C.CELL_GRPC_PORT}, {"containerPort": C.CELL_DASHBOARD_PORT}],
        "env": [
            _node_name_env(),
            _pod_ip_env(),
            # Structural env the lab wires itself (never user-editable).
            *_env({
                "WIGGLE_PORT": C.CELL_GRPC_PORT,
                "WIGGLE_DASHBOARD_PORT": C.CELL_DASHBOARD_PORT,
                "WIGGLE_JDBC_URL": f"jdbc:postgresql://{db}:{C.DB_PORT}/wiggle",
                "WIGGLE_JDBC_USER": "wiggle",
                "WIGGLE_JDBC_PASSWORD": "wiggle",
                "WIGGLE_CELL_ID": cell,
                "WIGGLE_NAMESPACE": namespace,
                "WIGGLE_COORDINATOR_URL": f"coordinator:{C.COORD_GRPC_PORT}",
                "WIGGLE_REGION": region or None,
            }),
            # Operational tunables from the UI (poll interval + housekeeping default here); unset ones
            # are dropped by _env ⇒ the server falls back to its own defaults.
            *_env(C.to_env(C.CELL_TUNABLES, tunables)),
        ],
        "readinessProbe": {"tcpSocket": {"port": C.CELL_GRPC_PORT},
                           "initialDelaySeconds": 4, "periodSeconds": 3},
        "livenessProbe": {"tcpSocket": {"port": C.CELL_GRPC_PORT},
                          "initialDelaySeconds": 12, "periodSeconds": 10},
    }
    # The cell Service exposes both gRPC (8080) and the web dashboard (8090) so each can be port-forwarded.
    svc = {
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": C.K8S_NAMESPACE, "labels": labels},
        "spec": {"selector": {"app": name}, "ports": [
            {"name": "grpc", "port": C.CELL_GRPC_PORT, "targetPort": C.CELL_GRPC_PORT},
            {"name": "dashboard", "port": C.CELL_DASHBOARD_PORT, "targetPort": C.CELL_DASHBOARD_PORT},
        ]},
    }
    return [_deployment(name, labels, replicas, container), svc]


def console_manifests(namespace: str, password: str | None = None,
                      viewer_password: str | None = None) -> list[dict]:
    """The standalone ops console (one image, WIGGLE_ROLE=console) for a namespace: a pure gRPC client
    of the coordinator that fans instance queries across the namespace's cells and serves the web UI on
    8090. Runs in-cluster, so it reaches cell pod IPs directly -- no endpoint rewrite.

    With no ``password`` the console is open (full access). Set ``password`` for an operator login;
    ``viewer_password`` additionally enables a read-only account (can view, but not cancel/signal/schedule)
    -- only meaningful alongside an operator password."""
    name = C.dns_name("console", namespace)
    labels = C.labels("console", namespace=namespace)
    container = {
        "name": "console", "image": C.IMAGE, "imagePullPolicy": "IfNotPresent",
        "ports": [{"containerPort": C.CONSOLE_HTTP_PORT}],
        "env": _env({
            "WIGGLE_ROLE": "console",
            "WIGGLE_COORDINATOR_URL": f"coordinator:{C.COORD_GRPC_PORT}",
            "WIGGLE_NAMESPACE": namespace,
            "WIGGLE_DASHBOARD_PORT": C.CONSOLE_HTTP_PORT,
            "WIGGLE_DASHBOARD_PASSWORD": password or None,
            "WIGGLE_DASHBOARD_VIEWER_PASSWORD": viewer_password or None,
        }),
        "readinessProbe": {"httpGet": {"path": "/healthz", "port": C.CONSOLE_HTTP_PORT},
                           "initialDelaySeconds": 4, "periodSeconds": 3},
    }
    return [_deployment(name, labels, 1, container),
            _service(name, labels, C.CONSOLE_HTTP_PORT, C.CONSOLE_HTTP_PORT)]


def to_yaml(docs: list[dict]) -> str:
    return yaml.safe_dump_all(docs, sort_keys=False)
