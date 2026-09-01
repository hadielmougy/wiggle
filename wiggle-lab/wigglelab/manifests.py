"""Kubernetes manifest builders. Everything is one image (the wiggle dist), specialised by env:
a coordinator (WIGGLE_ROLE=coordinator, Ratis store) and per-cell (its own Postgres + wiggle nodes
pointed at that DB and at the coordinator)."""
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


def coordinator_manifests(size: int = C.COORD_DEFAULT_GROUP_SIZE) -> list[dict]:
    """A single Apache Ratis group of ``size`` coordinator pods, as a StatefulSet behind a headless
    Service. Every pod serves the CellCoordinator gRPC (8099) backed by the SAME replicated store, so a
    client reaching any pod sees consistent state -- unlike independent single-member coordinators.
    A fixed peer list means this is not dynamically scalable: choose ``size`` at deploy time (odd for a
    majority); redeploying re-forms the group."""
    labels = C.labels("coordinator")
    ns = C.K8S_NAMESPACE

    def peer(i: int) -> str:
        host = f"coordinator-{i}.coordinator.{ns}.svc.cluster.local"
        return f"coordinator-{i}@{host}:{C.COORD_RAFT_PORT}"

    peers = ",".join(peer(i) for i in range(size))
    # Each pod's Raft id is its own name; peers is the whole group. k8s expands $(POD_NAME) from the env
    # defined just above it.
    store_uri = f"ratis://{C.COORD_DATA_DIR}?peers={peers}&id=$(POD_NAME)"

    container = {
        "name": "coordinator", "image": C.IMAGE, "imagePullPolicy": "IfNotPresent",
        "ports": [{"containerPort": C.COORD_GRPC_PORT, "name": "grpc"},
                  {"containerPort": C.COORD_RAFT_PORT, "name": "raft"}],
        "env": [
            {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            {"name": "WIGGLE_NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
            *_env({
                "WIGGLE_ROLE": "coordinator",
                "WIGGLE_PORT": C.COORD_GRPC_PORT,
                "WIGGLE_COORD_STORE": store_uri,
            }),
        ],
        "volumeMounts": [{"name": "coord-data", "mountPath": C.COORD_DATA_DIR}],
        "readinessProbe": {"tcpSocket": {"port": C.COORD_GRPC_PORT},
                           "initialDelaySeconds": 5, "periodSeconds": 3},
    }
    sts = {
        "apiVersion": "apps/v1", "kind": "StatefulSet",
        "metadata": {"name": "coordinator", "namespace": ns, "labels": labels},
        "spec": {
            "serviceName": "coordinator",
            "replicas": size,
            "podManagementPolicy": "Parallel",   # start all peers together so the group can form quorum
            "selector": {"matchLabels": {"app": "coordinator"}},
            "template": {
                "metadata": {"labels": {**labels, "app": "coordinator"}},
                "spec": {
                    "containers": [container],
                    "volumes": [{"name": "coord-data", "emptyDir": {}}],
                    # Run as root so the embedded Ratis+RocksDB store can write its data dir on the volume.
                    "securityContext": {"runAsUser": 0, "runAsGroup": 0},
                },
            },
        },
    }
    # Headless Service: gives each pod stable DNS (coordinator-i.coordinator...) for the Raft peers, and
    # also fronts the gRPC API for clients (any pod serves the same replicated state).
    svc = {
        "apiVersion": "v1", "kind": "Service",
        "metadata": {"name": "coordinator", "namespace": ns, "labels": labels},
        "spec": {"clusterIP": "None", "selector": {"app": "coordinator"}, "ports": [
            {"name": "grpc", "port": C.COORD_GRPC_PORT, "targetPort": C.COORD_GRPC_PORT},
            {"name": "raft", "port": C.COORD_RAFT_PORT, "targetPort": C.COORD_RAFT_PORT},
        ]},
    }
    return [svc, sts]


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


def cell_manifests(cell: str, namespace: str, replicas: int, region: str = "") -> list[dict]:
    name = C.dns_name("cell", cell)
    db = C.dns_name("db", cell)
    labels = C.labels("cell", cell=cell, namespace=namespace)
    container = {
        "name": "wiggle", "image": C.IMAGE, "imagePullPolicy": "IfNotPresent",
        "ports": [{"containerPort": C.CELL_GRPC_PORT}, {"containerPort": C.CELL_DASHBOARD_PORT}],
        "env": [
            _node_name_env(),
            _pod_ip_env(),
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
                "WIGGLE_POLL_INTERVAL_MILLIS": 200,
                "WIGGLE_HOUSEKEEPING_BATCH": 500,
            }),
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


def to_yaml(docs: list[dict]) -> str:
    return yaml.safe_dump_all(docs, sort_keys=False)
