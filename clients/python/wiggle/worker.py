"""The data plane: a worker pulls tasks it has capacity for, runs the matching handler, and reports
the result. Workers hold no durable state -- a crash loses at most the in-flight step, which the
server re-leases and re-runs."""
from __future__ import annotations

import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, Optional

import grpc

from ._convert import from_value, shallow_diff
from .client import WiggleClient
from .workflow import Activity, Blueprint, Predicate, SideEffect

log = logging.getLogger("wiggle.worker")


class PermanentError(Exception):
    """Raise from a handler to fail the step without retrying (a bad request, not a blip)."""


class Worker:
    """Register blueprints (for their handlers), then :meth:`start` to pull and run work.

    >>> worker = Worker(client, "worker-1").register(wf)
    >>> worker.start()          # background threads; call stop() to drain
    """

    def __init__(self, client: WiggleClient, worker_id: str, *,
                 concurrency: Optional[int] = None, lease_s: float = 30.0,
                 long_poll_wait_s: float = 10.0, idle_backoff_s: float = 0.2,
                 error_backoff_s: float = 2.0, queues: Optional[Iterable[str]] = None,
                 register_on_start: bool = True, await_registration_s: float = 0.0):
        self._client = client
        self.worker_id = worker_id
        self._concurrency = concurrency or os.cpu_count() or 4
        self._lease_ms = int(lease_s * 1000)
        self._wait_ms = int(long_poll_wait_s * 1000)
        self._idle_backoff = idle_backoff_s
        self._error_backoff = error_backoff_s
        self._explicit_queues = set(queues) if queues else None
        self._register_on_start = register_on_start
        self._await_registration_s = await_registration_s

        self._handlers: dict[str, Callable] = {}
        self._queues: set[str] = set()
        self._blueprints: list[Blueprint] = []
        self._claims: list[tuple[str, str, str]] = []   # (workflow, step, expected NodeKind)
        self._executor: Optional[ThreadPoolExecutor] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._running = threading.Event()
        self._inflight = 0
        self._lock = threading.Lock()

    def register(self, blueprint: Blueprint) -> "Worker":
        self._handlers.update(blueprint.handlers)
        self._queues.update(blueprint.queues)
        self._blueprints.append(blueprint)
        return self

    def handle(self, workflow: str, step: str, fn: Activity) -> "Worker":
        """Bind a handler to one step of an already-registered workflow, by name -- no topology
        re-declaration. The graph lives on the server; this worker just implements ``step``. ``fn``
        takes the context and returns the new context (only what changed is sent back), exactly like
        :meth:`Workflow.step`. Which queue the step polls is discovered from the graph on
        :meth:`start`, which also fails fast if ``step`` does not exist (or is the wrong kind)."""
        def wrapper(ctx):
            out = fn(ctx)
            return shallow_diff(ctx, out) if out is not None else None
        return self._bind(workflow, step, "TASK", wrapper)

    def handle_gate(self, workflow: str, step: str, test: Predicate) -> "Worker":
        """Bind a predicate (gate / choose guard / do-while condition) step by name; ``test`` returns
        a bool. See :meth:`handle`."""
        return self._bind(workflow, step, "PREDICATE", lambda ctx: bool(test(ctx)))

    def handle_effect(self, workflow: str, step: str, fn: SideEffect) -> "Worker":
        """Bind a side-effect step by name; ``fn``'s return is ignored and the context is unchanged.
        See :meth:`handle`."""
        def wrapper(ctx):
            fn(ctx)
            return None
        return self._bind(workflow, step, "TASK", wrapper)

    def _bind(self, workflow: str, step: str, kind: str, wrapper: Callable) -> "Worker":
        if not workflow or not step:
            raise ValueError("workflow and step are required")
        activity = f"{workflow}#{step}"
        if activity in self._handlers:
            raise ValueError(f"duplicate handler for activity '{activity}'")
        self._handlers[activity] = wrapper
        self._claims.append((workflow, step, kind))
        return self

    @property
    def _served_queues(self) -> set[str]:
        return self._explicit_queues if self._explicit_queues is not None else self._queues

    def start(self) -> "Worker":
        if self._running.is_set():
            return self
        if self._register_on_start:
            for bp in self._blueprints:
                self._client.register(bp)
        if self._claims:
            self._reconcile()
        self._running.set()
        self._executor = ThreadPoolExecutor(max_workers=self._concurrency,
                                            thread_name_prefix=f"wiggle-{self.worker_id}")
        self._poll_thread = threading.Thread(target=self._poll_loop,
                                             name=f"wiggle-poll-{self.worker_id}", daemon=True)
        self._poll_thread.start()
        log.info("worker %s polling queues %s with concurrency %d",
                 self.worker_id, sorted(self._served_queues), self._concurrency)
        return self

    def stop(self, timeout_s: float = 10.0) -> None:
        self._running.clear()
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=False)
        if self._poll_thread:
            self._poll_thread.join(timeout=timeout_s)

    def __enter__(self) -> "Worker":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    def run_forever(self) -> None:
        """Start and block until interrupted (Ctrl-C)."""
        self.start()
        try:
            while self._running.is_set():
                self._running.wait(1.0)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    # ---- internals ----

    def _reconcile(self) -> None:
        """Check every :meth:`handle`-bound claim against the server's registered graph, and learn
        the queue each claimed step polls (a name-only binding has no other way to know it). Fails
        fast on a typo'd step name or a kind mismatch, so a bad binding is caught at start rather
        than as a silent runtime "no handler" much later."""
        for wf in sorted({w for w, _, _ in self._claims}):
            graph = self._fetch_graph(wf)
            nodes = {n["activity"]: n for n in graph.get("nodes", [])
                     if n.get("kind") in ("TASK", "PREDICATE") and "activity" in n}
            served: set[str] = set()
            for w, step, kind in self._claims:
                if w != wf:
                    continue
                activity = f"{w}#{step}"
                node = nodes.get(activity)
                if node is None:
                    avail = sorted(a.split("#", 1)[1] for a in nodes)
                    raise ValueError(f"no step '{step}' in registered workflow '{wf}' "
                                     f"(available steps: {avail})")
                if node["kind"] != kind:
                    verb = "handle_gate" if node["kind"] == "PREDICATE" else "handle"
                    raise ValueError(f"activity '{activity}' is a {node['kind']} in the graph but was "
                                     f"bound as {kind}; use {verb}() instead")
                self._queues.add(node.get("queue", wf))
                served.add(activity)
            unclaimed = sorted(a.split("#", 1)[1] for a in set(nodes) - served)
            if unclaimed:   # info, not an error: a polyglot worker may intentionally serve a subset
                log.info("workflow '%s' has steps served by no handler on this worker: %s", wf, unclaimed)

    def _fetch_graph(self, workflow: str) -> dict:
        """Fetch the registered graph, optionally waiting out a registration race (the authoring
        client may still be starting up). Beyond the grace window, a missing graph is fatal."""
        deadline = time.monotonic() + self._await_registration_s
        while True:
            try:
                return self._client.get_workflow(workflow)
            except grpc.RpcError as e:
                not_found = e.code() == grpc.StatusCode.NOT_FOUND
                if not_found and time.monotonic() < deadline:
                    self._running.wait(0.25)
                    continue
                if not_found:
                    raise ValueError(
                        f"workflow '{workflow}' is not registered; register its graph before "
                        f"starting a worker that binds handlers to it (or pass await_registration_s)") from e
                raise

    def _poll_loop(self) -> None:
        while self._running.is_set():
            try:
                self._poll_once()
            except Exception as e:  # noqa: BLE001 - a poll failure must not kill the loop
                if not self._running.is_set():
                    return
                log.warning("poll failed: %s", e)
                self._running.wait(self._error_backoff)

    def _poll_once(self) -> None:
        with self._lock:
            free = self._concurrency - self._inflight
        if free <= 0:
            self._running.wait(self._idle_backoff)
            return
        result = self._client.poll(self.worker_id, self._served_queues, free,
                                   self._lease_ms, self._wait_ms)
        if not result.tasks:
            # honour the server's backpressure hold-off if it is shedding load
            backoff = (result.retry_after_millis / 1000) if result.retry_after_millis > 0 else self._idle_backoff
            self._running.wait(backoff)
            return
        for task in result.tasks:
            with self._lock:
                self._inflight += 1
            self._executor.submit(self._run, task)

    def _run(self, task) -> None:
        try:
            self._execute(task)
        finally:
            with self._lock:
                self._inflight -= 1

    def _execute(self, task) -> None:
        handler = self._handlers.get(task.activity)
        if handler is None:
            self._client.fail(task.task_id, task.lease_owner,
                              f"no handler registered for activity '{task.activity}'", retryable=False)
            return

        stop_heartbeat = self._start_heartbeat(task)
        try:
            result = handler(from_value(task.context))
            if task.kind == "PREDICATE":
                self._client.complete(task.task_id, task.lease_owner, {"value": bool(result)})
            else:
                self._client.complete(task.task_id, task.lease_owner, result)
        except PermanentError as e:
            self._client.fail(task.task_id, task.lease_owner, str(e), retryable=False)
        except Exception as e:  # noqa: BLE001 - a handler failure is a step failure, retried
            log.debug("step %s of %s failed: %s", task.step_name, task.instance_id, e)
            self._client.fail(task.task_id, task.lease_owner, f"{type(e).__name__}: {e}", retryable=True)
        finally:
            stop_heartbeat.set()

    def _start_heartbeat(self, task) -> threading.Event:
        """Extend the lease periodically while a handler runs, so a slow step keeps its claim."""
        stop = threading.Event()
        interval = max(self._lease_ms / 2000, 1.0)   # half the lease, in seconds

        def beat() -> None:
            while not stop.wait(interval):
                try:
                    self._client.heartbeat(task.task_id, task.lease_owner, self._lease_ms)
                except Exception:  # noqa: BLE001 - the step will just fail/retry if the lease is lost
                    return

        threading.Thread(target=beat, name=f"wiggle-hb-{task.task_id}", daemon=True).start()
        return stop
