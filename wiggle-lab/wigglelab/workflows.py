"""Built-in workflow definitions, built directly as the engine's JSON-native definition form
(see core.WorkflowDefinition#toJson). No Java compiler needed — the definition travels as an
opaque JSON Struct over gRPC.

Because v1 deploys no workers, the useful flows are ones the *server* can advance on its own:
  * ``sleep``   — SLEEP -> END: an instance goes RUNNING then COMPLETED via server-side timers.
  * ``instant`` — a lone END: COMPLETED immediately.
  * ``park``    — a TASK on a queue -> END: stays RUNNING (no worker to claim the task), which is
                  the clearest way to *see* where work lands across cells/epochs after a reshard.
"""
from __future__ import annotations


def _wf(name: str, start: str, nodes: list[dict], queues: list[str] | None = None) -> dict:
    return {
        "name": name,
        "version": 1,
        "startNode": start,
        "nodes": nodes,
        "queues": sorted(queues or []),
        "executionMode": "DEFAULT",
    }


def sleep_flow(name: str = "lab-sleep", sleep_millis: int = 8000) -> dict:
    """SLEEP -> END. Server-advanced: RUNNING, then COMPLETED after ``sleep_millis``."""
    return _wf(name, "nap", [
        {"id": "nap", "kind": "SLEEP", "name": "nap", "sleepMillis": sleep_millis, "next": "done"},
        {"id": "done", "kind": "END", "name": "done", "success": True},
    ])


def instant_flow(name: str = "lab-instant") -> dict:
    """A lone END node: COMPLETED as soon as it starts."""
    return _wf(name, "done", [
        {"id": "done", "kind": "END", "name": "done", "success": True},
    ])


def park_flow(name: str = "lab-park", queue: str = "lab") -> dict:
    """A TASK on a queue -> END. With no worker it parks in RUNNING, so instances accumulate
    visibly on whichever cell/epoch minted them."""
    return _wf(name, "work", [
        {"id": "work", "kind": "TASK", "name": "work", "activity": "noop", "queue": queue, "next": "done"},
        {"id": "done", "kind": "END", "name": "done", "success": True},
    ], queues=[queue])


BUILTINS = {
    "sleep": ("SLEEP -> END (runs then completes on its own)", sleep_flow),
    "instant": ("lone END (completes immediately)", instant_flow),
    "park": ("TASK on a queue (parks as RUNNING; no worker)", park_flow),
}
