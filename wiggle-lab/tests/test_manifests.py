"""What the lab wires into each pod: a server is plain JDBC config, and a console talks straight to
its server."""
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


def test_console_talks_straight_to_its_server():
    e = env_of(manifests.console_manifests("srv1"))
    assert e["WIGGLE_ROLE"] == "console"
    assert e["WIGGLE_URL"] == "cell-srv1:8080"


def test_console_carries_the_target_label_it_is_removed_by():
    docs = manifests.console_manifests("Srv1")
    labels = docs[0]["metadata"]["labels"]
    assert labels["wiggle-lab/console"] == "srv1"      # what remove_console selects on
    assert labels["wiggle-lab/cell"] == "Srv1"
