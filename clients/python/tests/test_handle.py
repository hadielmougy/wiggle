"""Offline tests for name-only handler binding (``Worker.handle``) and its start-time reconciliation
against the server's registered graph -- no server required. A tiny fake client stands in for the
control plane, returning a canned graph from ``get_workflow``; the tests drive ``_reconcile`` and the
stored wrappers directly, so ``gradle build`` exercises the binding without a live stack.
"""
import grpc
import pytest

from wiggle import Workflow, Worker


def _graph(*, authorise_queue="payments"):
    """The registered graph as ``get_workflow`` returns it: a Java-authored order flow that a Python
    worker will implement one step of, by name."""
    return (Workflow("order-fulfilment")
            .step("validate", lambda o: o)
            .gate("in-stock", lambda o: o["qty"] > 0)
            .step("authorise", lambda o: o, queue=authorise_queue)
            .effect("audit", lambda o: None)
            .build()).definition


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class _FakeClient:
    """Only the surface ``_reconcile`` touches: ``get_workflow`` returning a canned dict (or raising
    NOT_FOUND for an unknown name)."""

    def __init__(self, graphs):
        self._graphs = graphs

    def get_workflow(self, name):
        if name not in self._graphs:
            raise _FakeRpcError(grpc.StatusCode.NOT_FOUND)
        return self._graphs[name]


def _worker(graphs, **kw):
    w = Worker(_FakeClient(graphs), "w-test", register_on_start=False, **kw)
    return w


# ---------------------------------------------------------------- binding + reconcile

def test_handle_registers_by_activity_name():
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "authorise", lambda o: {**o, "paid": True})
    assert "order-fulfilment#authorise" in w._handlers


def test_reconcile_discovers_the_steps_queue():
    w = _worker({"order-fulfilment": _graph(authorise_queue="payments")})
    w.handle("order-fulfilment", "authorise", lambda o: o)
    w._reconcile()
    # the worker had no blueprint, so the only queue it now polls is the one learned from the graph
    assert w._served_queues == {"payments"}


def test_reconcile_defaults_queue_to_workflow_name_when_unset():
    graph = _graph()
    # drop the explicit queue so the node falls back to the workflow-name default
    for n in graph["nodes"]:
        if n.get("name") == "authorise":
            n.pop("queue", None)
    w = _worker({"order-fulfilment": graph})
    w.handle("order-fulfilment", "authorise", lambda o: o)
    w._reconcile()
    assert w._served_queues == {"order-fulfilment"}


def test_reconcile_rejects_unknown_step():
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "autorise", lambda o: o)   # typo
    with pytest.raises(ValueError, match="no step 'autorise'.*available steps"):
        w._reconcile()


def test_reconcile_rejects_kind_mismatch():
    w = _worker({"order-fulfilment": _graph()})
    # "in-stock" is a PREDICATE in the graph, but bound as a task
    w.handle("order-fulfilment", "in-stock", lambda o: o)
    with pytest.raises(ValueError, match="is a PREDICATE.*handle_gate"):
        w._reconcile()


def test_handle_gate_matches_a_predicate_node():
    w = _worker({"order-fulfilment": _graph()})
    w.handle_gate("order-fulfilment", "in-stock", lambda o: o["qty"] > 0)
    w._reconcile()   # no raise: kinds agree


def test_reconcile_missing_workflow_is_fatal():
    w = _worker({})   # nothing registered
    w.handle("order-fulfilment", "authorise", lambda o: o)
    with pytest.raises(ValueError, match="is not registered"):
        w._reconcile()


def test_duplicate_binding_rejected():
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "authorise", lambda o: o)
    with pytest.raises(ValueError, match="duplicate handler"):
        w.handle("order-fulfilment", "authorise", lambda o: o)


# ---------------------------------------------------------------- wrapper semantics

def test_handle_wrapper_sends_only_the_diff():
    w = _worker({"order-fulfilment": _graph()})
    w.handle("order-fulfilment", "authorise", lambda o: {**o, "paid": True})
    wrapper = w._handlers["order-fulfilment#authorise"]
    assert wrapper({"orderId": "o1", "qty": 1}) == {"paid": True}   # unchanged keys are not resent


def test_handle_effect_wrapper_returns_none():
    w = _worker({"order-fulfilment": _graph()})
    seen = {}
    w.handle_effect("order-fulfilment", "audit", lambda o: seen.update(o))
    wrapper = w._handlers["order-fulfilment#audit"]
    assert wrapper({"orderId": "o1"}) is None
    assert seen == {"orderId": "o1"}


def test_handle_gate_wrapper_returns_bool():
    w = _worker({"order-fulfilment": _graph()})
    w.handle_gate("order-fulfilment", "in-stock", lambda o: o["qty"] > 0)
    wrapper = w._handlers["order-fulfilment#in-stock"]
    assert wrapper({"qty": 3}) is True
    assert wrapper({"qty": 0}) is False
