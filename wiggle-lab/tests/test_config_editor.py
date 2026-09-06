"""Config editor: curated tunables flow into pod env on (re)deploy, structural env stays intact, and
edited config round-trips through the lab's persisted state."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from wigglelab import config as C
from wigglelab import manifests


def _cell_container(docs):
    dep = next(d for d in docs if d["kind"] == "Deployment")
    return dep["spec"]["template"]["spec"]["containers"][0]


def _env_map(container):
    # Only literal `value` env (skip valueFrom fieldRefs like POD_IP / node name).
    return {e["name"]: e["value"] for e in container["env"] if "value" in e}


def test_cell_tunables_override_and_defaults():
    env = _env_map(_cell_container(
        manifests.cell_manifests("cellA", "orders", 1, "eu", {"WIGGLE_LEASE_MILLIS": 5000})))
    assert env["WIGGLE_LEASE_MILLIS"] == "5000"          # override applied
    assert env["WIGGLE_POLL_INTERVAL_MILLIS"] == "200"   # baseline default preserved
    assert env["WIGGLE_HOUSEKEEPING_BATCH"] == "500"
    assert "WIGGLE_RETENTION_MILLIS" not in env          # unset tunable ⇒ server default (absent)


def test_cell_structural_env_intact():
    env = _env_map(_cell_container(manifests.cell_manifests("cellA", "orders", 1, "eu", None)))
    assert env["WIGGLE_CELL_ID"] == "cellA"
    assert env["WIGGLE_NAMESPACE"] == "orders"
    assert env["WIGGLE_REGION"] == "eu"
    assert env["WIGGLE_JDBC_URL"].startswith("jdbc:postgresql://")
    assert env["WIGGLE_COORDINATOR_URL"].startswith("coordinator:")


def test_cell_empty_region_dropped():
    env = _env_map(_cell_container(manifests.cell_manifests("cellB", "orders", 1)))
    assert "WIGGLE_REGION" not in env
    assert env["WIGGLE_POLL_INTERVAL_MILLIS"] == "200"   # baseline unchanged with no tunables


def test_coordinator_tunables_merge():
    docs = manifests.coordinator_manifests(1, {"WIGGLE_LOG_LEVEL": "DEBUG"})
    sts = next(d for d in docs if d["kind"] == "StatefulSet")
    env = {e["name"]: e["value"] for e in sts["spec"]["template"]["spec"]["containers"][0]["env"]
           if "value" in e}
    assert env["WIGGLE_LOG_LEVEL"] == "DEBUG"
    assert env["WIGGLE_ROLE"] == "coordinator"           # structural env intact


def test_resolve_tunables_defaults_and_override():
    r = C.resolve_tunables(C.CELL_TUNABLES, {"WIGGLE_LEASE_MILLIS": 5000})
    assert r["WIGGLE_LEASE_MILLIS"] == 5000              # override
    assert r["WIGGLE_POLL_INTERVAL_MILLIS"] == 200       # spec default
    assert r["WIGGLE_RETENTION_MILLIS"] is None          # no default ⇒ None (dropped by _env)


def test_state_roundtrip(tmp_path, monkeypatch):
    from wigglelab import controller
    monkeypatch.setattr(controller, "STATE_DIR", tmp_path)
    monkeypatch.setattr(controller, "STATE_FILE", tmp_path / "state.json")
    lab = controller.Lab()
    lab.cell_config = {"cellA": {"WIGGLE_LEASE_MILLIS": 5000}}
    lab.coord_config = {"WIGGLE_LOG_LEVEL": "DEBUG"}
    lab._save_state()

    reloaded = controller.Lab()   # __init__ -> _load_state() from the same tmp state file
    assert reloaded.cell_config == {"cellA": {"WIGGLE_LEASE_MILLIS": 5000}}
    assert reloaded.coord_config == {"WIGGLE_LOG_LEVEL": "DEBUG"}
