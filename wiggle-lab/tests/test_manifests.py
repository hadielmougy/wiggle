"""What the lab wires into each pod -- specifically, what it must NOT wire.

A standalone deployment is the ordinary way to run wiggle: no coordinator, no placement. The lab
expresses that by leaving the namespace blank, and these pin that a blank namespace really does
produce a plain server and a console that talks straight to it.
"""
import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from wigglelab import manifests


def env_of(docs):
    for d in docs:
        if d["kind"] == "Deployment":
            c = d["spec"]["template"]["spec"]["containers"][0]
            return {e["name"]: e.get("value") for e in c["env"] if "value" in e}
    raise AssertionError("no Deployment in manifests")


def test_blank_namespace_is_a_standalone_server():
    e = env_of(manifests.cell_manifests("srv1", "", 1))
    assert "WIGGLE_COORDINATOR_URL" not in e
    assert "WIGGLE_NAMESPACE" not in e
    assert e["WIGGLE_JDBC_URL"].startswith("jdbc:postgresql://db-srv1")


def test_a_namespace_still_joins_the_coordinator():
    e = env_of(manifests.cell_manifests("cellA", "orders", 1))
    assert e["WIGGLE_COORDINATOR_URL"].startswith("coordinator:")
    assert e["WIGGLE_NAMESPACE"] == "orders"


def test_console_for_a_standalone_server_needs_no_coordinator():
    e = env_of(manifests.console_manifests("srv1", server="srv1"))
    assert e["WIGGLE_ROLE"] == "console"
    assert e["WIGGLE_URL"] == "cell-srv1:8080"
    assert "WIGGLE_COORDINATOR_URL" not in e
    assert "WIGGLE_NAMESPACE" not in e


def test_console_for_a_namespace_is_unchanged():
    e = env_of(manifests.console_manifests("orders"))
    assert e["WIGGLE_COORDINATOR_URL"].startswith("coordinator:")
    assert e["WIGGLE_NAMESPACE"] == "orders"
    assert "WIGGLE_URL" not in e


def test_console_carries_the_target_label_it_is_removed_by():
    docs = manifests.console_manifests("Srv1", server="Srv1")
    labels = docs[0]["metadata"]["labels"]
    assert labels["wiggle-lab/console"] == "srv1"      # what remove_console selects on
    assert labels["wiggle-lab/cell"] == "Srv1"
    assert "wiggle-lab/namespace" not in labels
