"""What the lab wires into each pod: a server is plain JDBC config, and serves the portal."""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from wigglelab import manifests


def env_of(docs):
    for d in docs:
        if d["kind"] == "Deployment":
            c = d["spec"]["template"]["spec"]["containers"][0]
            return {e["name"]: e.get("value") for e in c["env"] if "value" in e}
    raise AssertionError("no Deployment in manifests")


def test_a_server_is_its_nodes_over_its_own_database():
    e = env_of(manifests.cell_manifests("srv1", 1))
    assert e["WIGGLE_JDBC_URL"].startswith("jdbc:postgresql://db-srv1")
    assert not any(k.startswith("WIGGLE_COORD") or k in ("WIGGLE_NAMESPACE", "WIGGLE_CELL_ID")
                   for k in e), "no removed coordinator setting may reach the pod: the server refuses them"


def test_every_server_node_serves_the_portal():
    docs = manifests.cell_manifests("srv1", 2)
    assert env_of(docs)["WIGGLE_PORTAL_PORT"] == "8070"
    svc = next(d for d in docs if d["kind"] == "Service")
    assert {"name": "portal", "port": 8070, "targetPort": 8070} in svc["spec"]["ports"]
    assert "WIGGLE_ROLE" not in env_of(docs)


def test_portal_passwords_reach_the_pod_only_when_set():
    assert "WIGGLE_DASHBOARD_PASSWORD" not in env_of(manifests.cell_manifests("srv1", 1))
    e = env_of(manifests.cell_manifests("srv1", 1, {"WIGGLE_DASHBOARD_PASSWORD": "s3cret"}))
    assert e["WIGGLE_DASHBOARD_PASSWORD"] == "s3cret"
