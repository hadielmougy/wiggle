"""Records the sequence of lab actions so an issue can be reproduced and fixed.

Every mutating Lab method is wrapped with @record; while a Recording is active, each call appends an
Event (method + JSON-safe args + outcome). Export the Recording to JSON, send it over, and replay it
with replay.py to reproduce the exact sequence — including the failing step.
"""
from __future__ import annotations

import functools
import json
import time
from dataclasses import asdict, dataclass, field


def _jsonable(x):
    """Coerce args to JSON-safe form (tuples->lists, everything else via str fallback)."""
    return json.loads(json.dumps(x, default=str))


@dataclass
class Event:
    seq: int
    ts: float
    method: str
    args: list
    kwargs: dict
    status: str = "ok"          # ok | error
    error: str | None = None
    error_type: str | None = None
    duration_ms: int = 0
    data: dict | None = None    # optional payload for snapshot events


@dataclass
class Recording:
    id: str
    created_at: float
    meta: dict
    events: list = field(default_factory=list)
    note: str = ""

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "meta": self.meta,
            "note": self.note,
            "events": [asdict(e) for e in self.events],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @staticmethod
    def from_dict(d: dict) -> "Recording":
        rec = Recording(id=d.get("id", "?"), created_at=d.get("created_at", 0.0),
                        meta=d.get("meta", {}), note=d.get("note", ""))
        rec.events = [Event(**e) for e in d.get("events", [])]
        return rec

    def append(self, ev: Event):
        self.events.append(ev)


def record(fn):
    """Wrap a Lab method so calls are captured while ``self.recording`` is active (and re-raised)."""
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        rec: Recording | None = getattr(self, "recording", None)
        if rec is None:
            return fn(self, *args, **kwargs)
        ev = Event(seq=len(rec.events) + 1, ts=time.time(), method=fn.__name__,
                   args=_jsonable(list(args)), kwargs=_jsonable(dict(kwargs)))
        start = time.time()
        try:
            result = fn(self, *args, **kwargs)
            ev.status = "ok"
            return result
        except Exception as e:  # noqa: BLE001 - recorded then re-raised
            ev.status = "error"
            ev.error = str(e)
            ev.error_type = type(e).__name__
            raise
        finally:
            ev.duration_ms = int((time.time() - start) * 1000)
            rec.append(ev)
    return wrapper
