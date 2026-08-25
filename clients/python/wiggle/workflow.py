"""The workflow DSL: build a graph of steps, gates, timers, signal waits, parallel ``fork`` and
exclusive ``choose``, then ``build()`` it into a :class:`Blueprint` you register, start, and serve
with a :class:`~wiggle.worker.Worker`.

The context is a plain ``dict`` that flows through the steps. A step returns the *whole* context
(usually ``{**ctx, ...}``); the engine merges only what changed, so parallel branches that touch
different fields merge cleanly.
"""
from __future__ import annotations

import hashlib
import json
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Optional

from ._convert import shallow_diff

Context = dict
Activity = Callable[[Context], Context]      # step: ctx -> new ctx
SideEffect = Callable[[Context], Any]        # effect: ctx -> (ignored)
Predicate = Callable[[Context], bool]        # gate / case guard: ctx -> bool
BranchFn = Callable[["Workflow"], "Workflow"]  # builds a nested branch/case body

_MAX_INT = 2_147_483_647


@dataclass(frozen=True)
class Retry:
    """A per-step retry policy. Backoffs are given in seconds."""

    max_attempts: int = _MAX_INT
    initial_backoff_s: float = 1.0
    multiplier: float = 1.0
    max_backoff_s: float = 60.0
    jitter: float = 0.0

    def to_json(self) -> dict:
        return {
            "maxAttempts": int(self.max_attempts),
            "initialBackoffMillis": int(self.initial_backoff_s * 1000),
            "multiplier": float(self.multiplier),
            "maxBackoffMillis": int(self.max_backoff_s * 1000),
            "jitter": float(self.jitter),
        }

    @staticmethod
    def forever() -> "Retry":
        return Retry(_MAX_INT, 1.0, 1.0, 60.0, 0.0)

    @staticmethod
    def none() -> "Retry":
        return Retry(1, 0.0, 1.0, 0.0, 0.0)

    @staticmethod
    def exponential(max_attempts: int, initial_backoff_s: float) -> "Retry":
        return Retry(max_attempts, initial_backoff_s, 2.0, 300.0, 0.2)

    @staticmethod
    def fixed(max_attempts: int, backoff_s: float) -> "Retry":
        return Retry(max_attempts, backoff_s, 1.0, backoff_s, 0.0)


@dataclass
class Branch:
    """One parallel branch of a :meth:`Workflow.fork`."""

    name: str
    body: BranchFn

    @staticmethod
    def of(name: str, body: BranchFn) -> "Branch":
        return Branch(name, body)


@dataclass
class Case:
    """One arm of a :meth:`Workflow.choose`. ``guard is None`` marks the ``otherwise`` default."""

    name: str
    guard: Optional[Predicate]
    body: BranchFn

    @staticmethod
    def when(name: str, guard: Predicate, body: BranchFn) -> "Case":
        return Case(name, guard, body)

    @staticmethod
    def otherwise(name: str, body: BranchFn) -> "Case":
        return Case(name, None, body)


@dataclass
class Blueprint:
    """A compiled workflow: the definition sent to the server, plus the worker-side handlers."""

    name: str
    version: int
    definition: dict
    handlers: dict[str, Callable[[Context], Any]]
    queues: list[str]


class _Graph:
    """The accumulating node store shared by a workflow and all its nested branches."""

    def __init__(self, name: str, default_queue: str):
        self.name = name
        self.default_queue = default_queue
        self.nodes: dict[str, dict] = {}
        self.handlers: dict[str, Callable] = {}
        self.queues: set[str] = set()
        self.reserved: set[str] = set()
        self.start_node: Optional[str] = None
        self._counter = 0

    def _nid(self, prefix: str = "n") -> str:
        self._counter += 1
        return f"{prefix}{self._counter}"

    def _reserve(self, name: str) -> None:
        if name in self.reserved:
            raise ValueError(f"duplicate step name '{name}'")
        self.reserved.add(name)

    def add_worker(self, kind: str, name: str, wrapper: Callable, queue: Optional[str],
                   retry: Optional[Retry]) -> str:
        self._reserve(name)
        nid = self._nid()
        q = queue or self.default_queue
        self.queues.add(q)
        activity = f"{self.name}#{name}"
        self.handlers[activity] = wrapper
        self.nodes[nid] = {"id": nid, "kind": kind, "name": name, "activity": activity,
                           "queue": q, "retry": (retry or Retry.forever()).to_json()}
        return nid

    def add_timer(self, kind: str, name: str, millis: int, reserve: bool) -> str:
        if reserve:
            self._reserve(name)
        nid = self._nid()
        node = {"id": nid, "kind": kind, "name": name}
        if millis > 0:
            node["sleepMillis"] = millis
        self.nodes[nid] = node
        return nid

    def add_fork(self) -> str:
        nid = self._nid("fork")
        self.nodes[nid] = {"id": nid, "kind": "FORK", "name": nid}
        return nid

    def add_subworkflow(self, name: str, child_workflow: str) -> str:
        self._reserve(name)
        nid = self._nid("sub")
        # the child workflow's name travels in `activity`; the engine starts it (no worker handler)
        self.nodes[nid] = {"id": nid, "kind": "SUB_WORKFLOW", "name": name, "activity": child_workflow}
        return nid

    def add_dynfork(self, name: str, items_key: str, item_key: str) -> str:
        self._reserve(name)
        nid = self._nid("dynfork")
        self.nodes[nid] = {"id": nid, "kind": "DYN_FORK", "name": name,
                           "itemsKey": items_key, "itemKey": item_key}
        return nid

    def add_join(self, expected: int) -> str:
        nid = self._nid("join")
        node = {"id": nid, "kind": "JOIN", "name": nid}
        if expected > 0:                       # dynamic joins carry their width in the runtime group
            node["expected"] = expected
        self.nodes[nid] = node
        return nid

    def add_end(self, reason: Optional[str] = None) -> str:
        nid = self._nid("end")
        node = {"id": nid, "kind": "END", "success": True}
        if reason is not None:
            node["reason"] = reason
        self.nodes[nid] = node
        return nid

    def wire(self, node_id: str, edge: str, target: str) -> None:
        self.nodes[node_id]["next" if edge == "next" else "altNext"] = target

    def set_branches(self, fork_id: str, starts: list[str]) -> None:
        self.nodes[fork_id]["branches"] = list(starts)


class Workflow:
    """Fluent builder. Every operator returns ``self`` so calls chain.

    >>> wf = (Workflow("order")
    ...       .step("validate", lambda o: {**o, "status": "VALIDATED"})
    ...       .gate("in-stock", lambda o: o["quantity"] > 0)
    ...       .fork(
    ...           Branch.of("payment", lambda b: b.step("charge", charge)),
    ...           Branch.of("shipping", lambda b: b.step("label", label)))
    ...       .choose(
    ...           Case.when("vip", lambda o: o.get("vip"), lambda b: b.effect("concierge", concierge)),
    ...           Case.otherwise("standard", lambda b: b.effect("thanks", thanks)))
    ...       .build())
    """

    def __init__(self, name: str, *, version: Optional[int] = None, default_queue: Optional[str] = None):
        """``version`` pins an explicit version instead of the auto content hash. Use it to choose a
        stable, human-meaningful number (or to match another client) -- but then **you** own bumping
        it when the graph changes: the server overwrites the stored graph for a reused version, which
        affects instances already running on it. Leave it unset for the safe content-addressed default.
        """
        if not name or not name.strip():
            raise ValueError("workflow name is required")
        self.name = name
        self._explicit_version = _check_version(version) if version is not None else None
        self._graph = _Graph(name, default_queue or name)   # default queue = the workflow name
        self._enclosing_join: Optional[str] = None
        self._open: list[tuple[str, str]] = []              # (node_id, "next"|"alt") ends to wire next
        self._start: Optional[str] = None
        self._is_root = True

    @classmethod
    def _sub(cls, graph: _Graph, enclosing_join: Optional[str]) -> "Workflow":
        w = cls.__new__(cls)
        w.name = graph.name
        w._graph = graph
        w._enclosing_join = enclosing_join
        w._open = []
        w._start = None
        w._is_root = False
        return w

    def default_queue(self, queue: str) -> "Workflow":
        self._graph.default_queue = queue
        return self

    # ---- linear operators ----

    def step(self, name: str, fn: Activity, *, queue: Optional[str] = None,
             retry: Optional[Retry] = None) -> "Workflow":
        """A unit of work run on a worker; ``fn``'s returned context is merged back."""
        def wrapper(ctx: Context) -> Any:
            return shallow_diff(ctx, fn(ctx))
        return self._chain(self._graph.add_worker("TASK", name, wrapper, queue, retry))

    def then(self, name: str, fn: Activity, *, queue: Optional[str] = None,
             retry: Optional[Retry] = None) -> "Workflow":
        """Alias for :meth:`step` that reads well when sequencing."""
        return self.step(name, fn, queue=queue, retry=retry)

    def effect(self, name: str, fn: SideEffect, *, queue: Optional[str] = None,
               retry: Optional[Retry] = None) -> "Workflow":
        """Run ``fn`` for its side effect only; the context is left unchanged."""
        def wrapper(ctx: Context) -> Any:
            fn(ctx)
            return None
        return self._chain(self._graph.add_worker("TASK", name, wrapper, queue, retry))

    def gate(self, name: str, test: Predicate, *, queue: Optional[str] = None,
             retry: Optional[Retry] = None) -> "Workflow":
        """Continue only while ``test`` holds; a false result ends the instance as ``gated:<name>``
        (inside a fork/choose branch it short-circuits to the enclosing join instead)."""
        def wrapper(ctx: Context) -> Any:
            return bool(test(ctx))
        nid = self._graph.add_worker("PREDICATE", name, wrapper, queue, retry)
        self._attach(nid)
        target = self._enclosing_join if self._enclosing_join is not None else self._graph.add_end(f"gated:{name}")
        self._graph.wire(nid, "alt", target)
        self._open = [(nid, "next")]
        return self

    def sleep(self, name: str, *, seconds: float = 0.0, millis: int = 0) -> "Workflow":
        """A server-side timer; no worker is held while the instance waits."""
        return self._chain(self._graph.add_timer("SLEEP", name, int(seconds * 1000) + int(millis), reserve=False))

    def await_signal(self, name: str, *, timeout_s: float = 0.0,
                     escalation: Optional[BranchFn] = None) -> "Workflow":
        """Wait for a named external signal (delivered via :meth:`WiggleClient.signal`); the payload
        merges into the context like a step's result, and the flow continues down the next step. No
        worker is held while it waits.

        With a ``timeout_s`` the instance **fails** if the signal does not arrive in time -- unless an
        ``escalation`` branch is given, in which case that branch runs instead on timeout and then
        rejoins the flow after the wait (exactly one of delivery / escalation happens). ``escalation``
        is a function that receives a nested builder and chains onto it (like a fork branch), and it
        needs a positive ``timeout_s``."""
        nid = self._graph.add_timer("SIGNAL", name, int(timeout_s * 1000), reserve=True)
        self._attach(nid)
        if escalation is None:
            self._open = [(nid, "next")]                      # delivery path only
            return self
        if timeout_s <= 0:
            raise ValueError("await_signal escalation needs a positive timeout_s")
        # The delivery path is `next`; the escalation branch hangs off `alt` and its tail rejoins,
        # so both continue to whatever follows -- exactly the SIGNAL next/altNext shape Java emits.
        sub = Workflow._sub(self._graph, enclosing_join=self._enclosing_join)
        escalation(sub)
        if sub._start is None:
            raise ValueError(f"escalation branch of '{name}' defines no steps")
        self._graph.wire(nid, "alt", sub._start)
        self._open = [(nid, "next"), *sub._open]
        return self

    def sub_workflow(self, name: str, workflow: "Workflow | Blueprint | str") -> "Workflow":
        """Run another workflow as a child: it starts with this instance's context, and on completion
        its final context merges back here (a failed or cancelled child fails this instance). The
        child must be registered separately on the server; its latest version is used."""
        if isinstance(workflow, str):
            child = workflow
        elif isinstance(workflow, (Workflow, Blueprint)):
            child = workflow.name
        else:
            raise TypeError("workflow must be a Workflow, Blueprint, or workflow name")
        if not child:
            raise ValueError("child workflow name is required")
        return self._chain(self._graph.add_subworkflow(name, child))

    # ---- branching ----

    def fork(self, *branches: Branch) -> "Workflow":
        """Fan out into parallel branches and wait for all of them (fan-out / join). Needs >= 2."""
        if len(branches) < 2:
            raise ValueError("fork needs at least two branches")
        fork_id = self._graph.add_fork()
        self._attach(fork_id)
        join_id = self._graph.add_join(len(branches))
        starts = [self._build_branch(b, join_id) for b in branches]
        self._graph.set_branches(fork_id, starts)
        self._open = [(join_id, "next")]
        return self

    def fork_each(self, name: str, items_key: str, item_key: str, body: BranchFn) -> "Workflow":
        """Runtime fan-out: at run time the engine reads the list in the context at ``items_key`` and
        spawns one parallel branch per element, running ``body`` with that element injected under
        ``item_key`` (and its position under ``item_key + "Index"``), branch-scoped. All branches
        join before the flow continues; an empty or missing list skips straight through.

        Branch writes merge like :meth:`fork` -- last write to the same key wins -- so put
        per-element results under per-element keys (use the index)."""
        fork_id = self._graph.add_dynfork(name, items_key, item_key)
        self._attach(fork_id)
        join_id = self._graph.add_join(0)                    # 0 = dynamic width
        template_start = self._build_branch(Branch(name, body), join_id)
        self._graph.set_branches(fork_id, [template_start])
        self._graph.wire(fork_id, "next", join_id)           # followed directly when the list is empty
        self._open = [(join_id, "next")]
        return self

    def do_while(self, condition_name: str, condition: Predicate, body: BranchFn) -> "Workflow":
        """A do-while loop: run ``body`` once, then evaluate ``condition`` on a worker; while it
        holds, the body runs again. Compiles to a plain cycle -- the condition is an ordinary
        predicate whose true edge points back at the body -- so the body always runs at least once."""
        sub = Workflow._sub(self._graph, enclosing_join=self._enclosing_join)
        body(sub)
        if sub._start is None:
            raise ValueError("doWhile body defines no steps")
        cond_id = self._graph.add_worker("PREDICATE", condition_name, _guard_wrapper(condition), None, None)
        self._attach(sub._start)                          # enter at the body
        sub._wire_open_to(cond_id)                        # body tail -> condition
        self._graph.wire(cond_id, "next", sub._start)     # true: loop back to the body
        self._open = [(cond_id, "alt")]                   # false: continue onward
        return self

    def choose(self, *cases: Case) -> "Workflow":
        """Exclusive choice: the first matching case's branch runs, the rest are skipped. An
        ``otherwise`` case (or, without one, the step after ``choose``) handles no match. A cascade
        of guards -- nothing runs in parallel and there is no join."""
        all_cases = list(cases)
        has_default = self._validate_choose(all_cases)
        n_guards = len(all_cases) - (1 if has_default else 0)

        guard_ids: list[str] = []
        for i in range(n_guards):
            c = all_cases[i]
            guard_ids.append(self._graph.add_worker("PREDICATE", c.name, _guard_wrapper(c.guard), None, None))

        self._attach(guard_ids[0])
        self._open = []
        for i in range(n_guards - 1):                       # each guard's false path -> next guard
            self._graph.wire(guard_ids[i], "alt", guard_ids[i + 1])
        for i in range(n_guards):                           # each guard's true path -> its branch
            self._collect_case(all_cases[i], guard_ids[i], "next")

        last = guard_ids[-1]
        if has_default:
            self._collect_case(all_cases[-1], last, "alt")
        else:
            self._open.append((last, "alt"))                # no match skips the choose entirely
        return self

    # ---- build ----

    def build(self) -> Blueprint:
        end_id = self._graph.add_end()
        self._wire_open_to(end_id)
        if self._graph.start_node is None:
            self._graph.start_node = end_id
        queues = sorted(self._graph.queues)
        definition = {
            "name": self.name,
            "startNode": self._graph.start_node,
            "nodes": list(self._graph.nodes.values()),
            "queues": queues,
            "executionMode": "SERVER",
        }
        version = self._explicit_version if self._explicit_version is not None else _content_version(definition)
        definition = {"version": version, **definition}
        return Blueprint(self.name, version, definition, dict(self._graph.handlers), queues)

    # ---- internals ----

    def _chain(self, node_id: str) -> "Workflow":
        self._attach(node_id)
        self._open = [(node_id, "next")]
        return self

    def _attach(self, node_id: str) -> None:
        """Wire the current open ends to ``node_id``; the very first node becomes this stream's start."""
        if self._open:
            for nid, edge in self._open:
                self._graph.wire(nid, edge, node_id)
            self._open = []
        elif self._start is None:
            self._start = node_id
            if self._is_root:
                self._graph.start_node = node_id

    def _wire_open_to(self, target: str) -> None:
        for nid, edge in self._open:
            self._graph.wire(nid, edge, target)
        self._open = []

    def _build_branch(self, branch: Branch, join_id: str) -> str:
        sub = Workflow._sub(self._graph, enclosing_join=join_id)
        branch.body(sub)
        if sub._start is None:
            raise ValueError(f"branch '{branch.name}' defines no steps")
        sub._wire_open_to(join_id)
        return sub._start

    def _collect_case(self, case: Case, guard_id: str, edge: str) -> None:
        sub = Workflow._sub(self._graph, enclosing_join=self._enclosing_join)
        case.body(sub)
        if sub._start is None:
            raise ValueError(f"case '{case.name}' defines no steps")
        self._graph.wire(guard_id, edge, sub._start)
        self._open.extend(sub._open)

    @staticmethod
    def _validate_choose(cases: list[Case]) -> bool:
        if not cases:
            raise ValueError("choose needs at least one case")
        for c in cases[:-1]:
            if c.guard is None:
                raise ValueError("otherwise() must be the last case")
        has_default = cases[-1].guard is None
        if has_default and len(cases) == 1:
            raise ValueError("choose needs at least one guarded case")
        return has_default


def _guard_wrapper(guard: Predicate) -> Callable[[Context], bool]:
    return lambda ctx: bool(guard(ctx))


_MAX_VERSION = 0x7FFFFFFF


def _check_version(version: int) -> int:
    if not isinstance(version, int) or isinstance(version, bool):
        raise TypeError("version must be an int")
    if not 1 <= version <= _MAX_VERSION:
        raise ValueError(f"version must be in 1..{_MAX_VERSION}")
    return version


def _edges(node: dict) -> list:
    return [node.get("next"), node.get("altNext"), *node.get("branches", [])]


def _content_version(definition: dict) -> int:
    """A deterministic, positive 31-bit content hash of the graph's *structure* (name, node contents,
    edge topology, execution mode) -- independent of the incidental node-id numbering. The same
    structure always hashes the same, so re-registering is idempotent and the server de-duplicates it.

    Node ids are relabelled by a deterministic breadth-first walk from the start node (edges visited
    as next, altNext, then branches in order), so changing how ids are minted never changes the hash.
    """
    nodes = {n["id"]: n for n in definition["nodes"]}
    canon: dict[str, str] = {}
    queue = deque([definition["startNode"]])
    while queue:
        nid = queue.popleft()
        if nid in canon or nid not in nodes:
            continue
        canon[nid] = f"c{len(canon)}"
        for target in _edges(nodes[nid]):
            if target is not None and target in nodes and target not in canon:
                queue.append(target)
    for nid in sorted(nodes):                        # any unreachable nodes, deterministically
        canon.setdefault(nid, f"c{len(canon)}")

    canon_nodes = []
    for nid, label in sorted(canon.items(), key=lambda kv: int(kv[1][1:])):
        n = dict(nodes[nid])
        n["id"] = label
        if "next" in n:
            n["next"] = canon[n["next"]]
        if "altNext" in n:
            n["altNext"] = canon[n["altNext"]]
        if "branches" in n:
            n["branches"] = [canon[b] for b in n["branches"]]
        canon_nodes.append(n)

    material = {
        "name": definition["name"],
        "startNode": canon[definition["startNode"]],
        "nodes": canon_nodes,
        "executionMode": definition["executionMode"],
    }
    canonical = json.dumps(material, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).digest()
    v = ((digest[0] & 0x7F) << 24) | (digest[1] << 16) | (digest[2] << 8) | digest[3]
    return v or 1
