"""Offline tests for the Wiggle Python DSL and wire conversions -- no server required.

They build workflows and assert on the compiled definition (node kinds, edge wiring, queues,
version determinism) plus the value/diff helpers, so `gradle build` exercises the client.
"""
import pytest

from wiggle import Branch, Case, Retry, Workflow
from wiggle._convert import from_value, shallow_diff, to_value


def _by_id(bp):
    return {n["id"]: n for n in bp.definition["nodes"]}


def _kind(bp, kind):
    return [n for n in bp.definition["nodes"] if n["kind"] == kind]


# ---------------------------------------------------------------- linear + gate

def test_linear_chain_and_gated_end():
    bp = (Workflow("wf")
          .step("a", lambda o: o)
          .gate("g", lambda o: True)
          .effect("b", lambda o: None)
          .build())
    byid = _by_id(bp)
    a = next(n for n in byid.values() if n.get("name") == "a")
    g = next(n for n in byid.values() if n.get("name") == "g")
    b = next(n for n in byid.values() if n.get("name") == "b")
    assert bp.definition["startNode"] == a["id"]
    assert a["kind"] == "TASK" and a["next"] == g["id"]
    assert g["kind"] == "PREDICATE" and g["next"] == b["id"]
    # the gate's false path ends the instance as gated:g
    gated = byid[g["altNext"]]
    assert gated["kind"] == "END" and gated["reason"] == "gated:g"
    # handlers are keyed by "<workflow>#<step>"
    assert set(bp.handlers) == {"wf#a", "wf#g", "wf#b"}


def test_default_queue_is_workflow_name_and_per_step_override():
    bp = (Workflow("orders")
          .step("a", lambda o: o)
          .step("b", lambda o: o, queue="payments")
          .build())
    byid = _by_id(bp)
    assert next(n for n in byid.values() if n.get("name") == "a")["queue"] == "orders"
    assert next(n for n in byid.values() if n.get("name") == "b")["queue"] == "payments"
    assert bp.queues == ["orders", "payments"]


def test_retry_json_shape():
    bp = Workflow("wf").step("a", lambda o: o, retry=Retry.exponential(5, 0.1)).build()
    retry = next(n for n in bp.definition["nodes"] if n.get("name") == "a")["retry"]
    assert retry == {"maxAttempts": 5, "initialBackoffMillis": 100, "multiplier": 2.0,
                     "maxBackoffMillis": 300000, "jitter": 0.2}


def test_version_is_deterministic_and_positive_int32():
    def build():
        return (Workflow("wf").step("a", lambda o: o).gate("g", lambda o: True).build())
    v1, v2 = build().version, build().version
    assert v1 == v2
    assert 0 < v1 <= 0x7FFFFFFF


def test_version_is_structural_independent_of_node_ids():
    from wiggle.workflow import _content_version
    bp = (Workflow("wf")
          .step("a", lambda o: o)
          .fork(Branch.of("l", lambda b: b.step("l1", lambda o: o)),
                Branch.of("r", lambda b: b.step("r1", lambda o: o)))
          .step("z", lambda o: o)
          .build())
    # relabel every node id to a totally different scheme, rewriting all edge references too
    remap = {n["id"]: f"x{i}" for i, n in enumerate(bp.definition["nodes"])}
    relabelled = {**bp.definition, "startNode": remap[bp.definition["startNode"]], "nodes": []}
    for n in bp.definition["nodes"]:
        m = dict(n)
        m["id"] = remap[n["id"]]
        for e in ("next", "altNext"):
            if e in m:
                m[e] = remap[m[e]]
        if "branches" in m:
            m["branches"] = [remap[b] for b in m["branches"]]
        relabelled["nodes"].append(m)
    assert _content_version(relabelled) == bp.version, "hash depends on structure, not the id numbering"


def test_version_changes_when_structure_changes():
    a = Workflow("wf").step("a", lambda o: o).build().version
    b = Workflow("wf").step("a", lambda o: o).step("b", lambda o: o).build().version
    assert a != b


def test_explicit_version_override_and_validation():
    bp = Workflow("wf", version=42).step("a", lambda o: o).build()
    assert bp.version == 42 and bp.definition["version"] == 42
    with pytest.raises(ValueError):
        Workflow("wf", version=0)
    with pytest.raises(ValueError):
        Workflow("wf", version=0x80000000)
    with pytest.raises(TypeError):
        Workflow("wf", version=True)   # bool is not a valid version


# ---------------------------------------------------------------- timers / signals

def test_sleep_and_signal_nodes():
    bp = (Workflow("wf")
          .sleep("nap", millis=250)
          .await_signal("go", timeout_s=2)
          .step("done", lambda o: o)
          .build())
    byid = _by_id(bp)
    nap = next(n for n in byid.values() if n.get("name") == "nap")
    go = next(n for n in byid.values() if n.get("name") == "go")
    assert nap["kind"] == "SLEEP" and nap["sleepMillis"] == 250
    assert go["kind"] == "SIGNAL" and go["sleepMillis"] == 2000
    # timers/signals have no worker handler
    assert "wf#nap" not in bp.handlers and "wf#go" not in bp.handlers


def test_await_signal_escalation_branch_wiring():
    bp = (Workflow("wf")
          .step("request", lambda o: o)
          .await_signal("approval", timeout_s=60,
                        escalation=lambda b: b.step("auto-approve", lambda o: {**o, "auto": True}))
          .step("finish", lambda o: o)
          .build())
    byid = _by_id(bp)
    sig = next(n for n in byid.values() if n.get("name") == "approval")
    auto = next(n for n in byid.values() if n.get("name") == "auto-approve")
    finish = next(n for n in byid.values() if n.get("name") == "finish")
    assert sig["kind"] == "SIGNAL" and sig["sleepMillis"] == 60000
    assert sig["next"] == finish["id"]        # delivery continues to `finish`
    assert sig["altNext"] == auto["id"]       # timeout escalates to the branch
    assert auto["next"] == finish["id"]       # escalation rejoins the flow at `finish`
    assert bp.handlers["wf#auto-approve"]     # the escalation step is a real worker handler


def test_await_signal_without_escalation_has_no_alt_edge():
    bp = Workflow("wf").step("a", lambda o: o).await_signal("s", timeout_s=5).step("b", lambda o: o).build()
    sig = next(n for n in bp.definition["nodes"] if n.get("name") == "s")
    assert "altNext" not in sig


def test_await_signal_escalation_requires_a_timeout():
    with pytest.raises(ValueError, match="positive timeout"):
        Workflow("wf").await_signal("s", escalation=lambda b: b.step("x", lambda o: o)).build()


def test_await_signal_empty_escalation_rejected():
    with pytest.raises(ValueError, match="defines no steps"):
        Workflow("wf").await_signal("s", timeout_s=5, escalation=lambda b: b).build()


# ---------------------------------------------------------------- fork / join

def test_fork_creates_fork_and_join_with_expected():
    bp = (Workflow("wf")
          .fork(
              Branch.of("l", lambda b: b.step("l1", lambda o: o)),
              Branch.of("r", lambda b: b.step("r1", lambda o: o).step("r2", lambda o: o)))
          .step("after", lambda o: o)
          .build())
    fork = _kind(bp, "FORK")[0]
    join = _kind(bp, "JOIN")[0]
    byid = _by_id(bp)
    assert len(fork["branches"]) == 2
    assert join["expected"] == 2
    for start in fork["branches"]:
        assert start in byid                      # branch starts exist
    # both branch tails lead to the join, and the join continues to "after"
    after = next(n for n in byid.values() if n.get("name") == "after")
    assert join["next"] == after["id"]


def test_fork_requires_two_branches():
    with pytest.raises(ValueError):
        Workflow("wf").fork(Branch.of("only", lambda b: b.step("x", lambda o: o))).build()


# ---------------------------------------------------------------- forkEach (dynamic)

def test_fork_each_dynfork_and_dynamic_join():
    bp = (Workflow("wf")
          .fork_each("each", "items", "item", lambda b: b.step("price", lambda o: o))
          .step("sum", lambda o: o)
          .build())
    df = _kind(bp, "DYN_FORK")[0]
    join = _kind(bp, "JOIN")[0]
    assert df["itemsKey"] == "items" and df["itemKey"] == "item"
    assert df["branches"] and df["next"] == join["id"]      # empty-list skip -> join
    assert "expected" not in join                           # dynamic width, not a fixed count


# ---------------------------------------------------------------- choose

def test_choose_guard_cascade_with_otherwise():
    bp = (Workflow("wf")
          .choose(
              Case.when("vip", lambda o: o.get("vip"), lambda b: b.step("v", lambda o: o)),
              Case.when("big", lambda o: o.get("big"), lambda b: b.step("g", lambda o: o)),
              Case.otherwise("std", lambda b: b.step("s", lambda o: o)))
          .step("after", lambda o: o)
          .build())
    byid = _by_id(bp)
    vip = next(n for n in byid.values() if n.get("name") == "vip")
    big = next(n for n in byid.values() if n.get("name") == "big")
    # vip false -> big (the next guard); big false -> the otherwise branch (a task, not a guard)
    assert vip["altNext"] == big["id"]
    assert byid[big["altNext"]]["kind"] == "TASK"
    # only the two guarded cases are predicates
    assert {n["name"] for n in _kind(bp, "PREDICATE")} == {"vip", "big"}


def test_choose_rejects_otherwise_not_last():
    with pytest.raises(ValueError):
        (Workflow("wf").choose(
            Case.otherwise("std", lambda b: b.step("s", lambda o: o)),
            Case.when("vip", lambda o: True, lambda b: b.step("v", lambda o: o))).build())


# ---------------------------------------------------------------- do_while

def test_do_while_is_a_cycle():
    bp = (Workflow("wf")
          .do_while("again", lambda o: o.get("more"), lambda b: b.step("body", lambda o: o))
          .step("done", lambda o: o)
          .build())
    byid = _by_id(bp)
    cond = next(n for n in byid.values() if n.get("name") == "again")
    body = next(n for n in byid.values() if n.get("name") == "body")
    assert body["next"] == cond["id"]        # body tail feeds the condition
    assert cond["next"] == body["id"]        # true edge loops back to the body
    assert byid[cond["altNext"]]["name"] == "done"   # false edge continues


# ---------------------------------------------------------------- sub-workflow

def test_sub_workflow_carries_child_name_and_has_no_handler():
    child = Workflow("child").step("x", lambda o: o).build()
    bp = (Workflow("parent")
          .step("prep", lambda o: o)
          .sub_workflow("call", child)         # accepts a Blueprint
          .build())
    sub = _kind(bp, "SUB_WORKFLOW")[0]
    assert sub["activity"] == "child" and sub["name"] == "call"
    assert "parent#call" not in bp.handlers
    # also accepts a bare workflow name
    assert _kind(Workflow("p2").sub_workflow("c", "child").build(), "SUB_WORKFLOW")[0]["activity"] == "child"


# ---------------------------------------------------------------- validation

def test_duplicate_step_name_rejected():
    with pytest.raises(ValueError):
        Workflow("wf").step("a", lambda o: o).step("a", lambda o: o).build()


# ---------------------------------------------------------------- conversions

def test_value_round_trip_and_int_coercion():
    obj = {"s": "x", "n": 3, "f": 1.5, "b": True, "none": None, "list": [1, 2], "nested": {"k": 4}}
    back = from_value(to_value(obj))
    assert back == obj
    assert isinstance(back["n"], int) and isinstance(back["nested"]["k"], int)
    assert isinstance(back["f"], float)


def test_shallow_diff_matches_engine_merge():
    assert shallow_diff({"a": 1, "b": 2}, {"a": 1, "b": 3, "c": 4}) == {"b": 3, "c": 4}
    assert shallow_diff({"a": 1, "b": 2}, {"a": 1}) == {"b": None}   # dropped key -> null
    assert shallow_diff({}, {"a": 1}) == {"a": 1}
