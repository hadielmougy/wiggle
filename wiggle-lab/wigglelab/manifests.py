"""Kubernetes manifest builders. Everything is one image (the wiggle dist), specialised by env.

A server deployment is its own Postgres plus one or more wiggle nodes pointed at that DB."""
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


def cell_manifests(cell: str, replicas: int, tunables: dict | None = None) -> list[dict]:
    """``replicas`` wiggle nodes over one Postgres -- one server cluster."""
    name = C.dns_name("cell", cell)
    db = C.dns_name("db", cell)
    labels = C.labels("cell", cell=cell)
    container = {
        "name": "wiggle", "image": C.IMAGE, "imagePullPolicy": "IfNotPresent",
        "ports": [{"containerPort": C.CELL_GRPC_PORT}, {"containerPort": C.CELL_DASHBOARD_PORT},
                  {"containerPort": C.CELL_PORTAL_PORT}],
        "env": [
            _node_name_env(),
            # Structural env the lab wires itself (never user-editable).
            *_env({
                "WIGGLE_PORT": C.CELL_GRPC_PORT,
                "WIGGLE_DASHBOARD_PORT": C.CELL_DASHBOARD_PORT,
                "WIGGLE_PORTAL_PORT": C.CELL_PORTAL_PORT,
                "WIGGLE_JDBC_URL": f"jdbc:postgresql://{db}:{C.DB_PORT}/wiggle",
                "WIGGLE_JDBC_USER": "wiggle",
                "WIGGLE_JDBC_PASSWORD": "wiggle",
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
    # The cell Service exposes gRPC (8080), /healthz (8090) and the portal (8070) so each can be port-forwarded.
    svc = {
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": name, "namespace": C.K8S_NAMESPACE, "labels": labels},
        "spec": {"selector": {"app": name}, "ports": [
            {"name": "grpc", "port": C.CELL_GRPC_PORT, "targetPort": C.CELL_GRPC_PORT},
            {"name": "dashboard", "port": C.CELL_DASHBOARD_PORT, "targetPort": C.CELL_DASHBOARD_PORT},
            {"name": "portal", "port": C.CELL_PORTAL_PORT, "targetPort": C.CELL_PORTAL_PORT},
        ]},
    }
    return [_deployment(name, labels, replicas, container), svc]


def to_yaml(docs: list[dict]) -> str:
    return yaml.safe_dump_all(docs, sort_keys=False)
