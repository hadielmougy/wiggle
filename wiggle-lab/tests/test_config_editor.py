"""Config editor: pinned baselines + user overrides flow into pod env on (re)deploy, structural env
stays intact, unset knobs fall back to the server default, and edited config round-trips through the
lab's persisted state."""
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


def test_to_env_pins_baselines_only_by_default():
    env = C.to_env(C.CELL_TUNABLES, None)
    assert env["WIGGLE_POLL_INTERVAL_MILLIS"] == 200     # pinned lab baseline
    assert env["WIGGLE_HOUSEKEEPING_BATCH"] == 500       # pinned lab baseline
    assert "WIGGLE_LEASE_MILLIS" not in env              # not pinned ⇒ server default (omitted)
    assert "WIGGLE_DISPATCH_LINGER_MILLIS" not in env    # not pinned ⇒ server default (omitted)


def test_to_env_includes_overrides():
    env = C.to_env(C.CELL_TUNABLES, {"WIGGLE_LEASE_MILLIS": 5000, "WIGGLE_DISPATCH_LINGER_MILLIS": 10})
    assert env["WIGGLE_LEASE_MILLIS"] == 5000
    assert env["WIGGLE_DISPATCH_LINGER_MILLIS"] == 10
    assert env["WIGGLE_POLL_INTERVAL_MILLIS"] == 200     # baselines still pinned alongside overrides


def test_defaults_cover_every_knob_incl_linger():
    d = C.defaults(C.CELL_TUNABLES)
    assert d["WIGGLE_DISPATCH_LINGER_MILLIS"] == 5       # linger is now a first-class, visible config
    assert d["WIGGLE_FALLBACK_POLL_MILLIS"] == 100
    assert d["WIGGLE_LEASE_MILLIS"] == 30000             # mirrors ServerConfig.fromEnvironment


def test_cell_manifest_baseline_and_override():
    env = _env_map(_cell_container(
        manifests.cell_manifests("cellA", "orders", 1, "eu", {"WIGGLE_DISPATCH_LINGER_MILLIS": 10})))
    assert env["WIGGLE_DISPATCH_LINGER_MILLIS"] == "10"  # override applied as pod env
    assert env["WIGGLE_POLL_INTERVAL_MILLIS"] == "200"   # baseline pinned
    assert "WIGGLE_LEASE_MILLIS" not in env              # unset ⇒ server default
    # structural env intact and never editable
    assert env["WIGGLE_CELL_ID"] == "cellA"
    assert env["WIGGLE_NAMESPACE"] == "orders"
    assert env["WIGGLE_REGION"] == "eu"
    assert env["WIGGLE_JDBC_URL"].startswith("jdbc:postgresql://")


def test_coordinator_tunables_merge():
    docs = manifests.coordinator_manifests(1, {"WIGGLE_LOG_LEVEL": "DEBUG"})
    sts = next(d for d in docs if d["kind"] == "StatefulSet")
    env = {e["name"]: e["value"] for e in sts["spec"]["template"]["spec"]["containers"][0]["env"]
           if "value" in e}
    assert env["WIGGLE_LOG_LEVEL"] == "DEBUG"
    assert env["WIGGLE_ROLE"] == "coordinator"           # structural env intact


def test_state_roundtrip(tmp_path, monkeypatch):
    from wigglelab import controller
    monkeypatch.setattr(controller, "STATE_DIR", tmp_path)
    monkeypatch.setattr(controller, "STATE_FILE", tmp_path / "state.json")
    lab = controller.Lab()
    lab.cell_config = {"cellA": {"WIGGLE_DISPATCH_LINGER_MILLIS": 10}}
    lab.coord_config = {"WIGGLE_LOG_LEVEL": "DEBUG"}
    lab._save_state()

    reloaded = controller.Lab()   # __init__ -> _load_state() from the same tmp state file
    assert reloaded.cell_config == {"cellA": {"WIGGLE_DISPATCH_LINGER_MILLIS": 10}}
    assert reloaded.coord_config == {"WIGGLE_LOG_LEVEL": "DEBUG"}
