#!/usr/bin/env python3
"""Reproduce a wiggle-lab recording against the current machine.

    python replay.py path/to/wiggle-lab-<id>.json [--no-wait] [--settle SECONDS]

Runs each recorded action in order, applying a readiness barrier after infra steps, and stops at the
first step that errors — the reproduction point. Requires the same prerequisites as the lab (docker,
kind, kubectl) and the wiggle image available to build/load.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from wigglelab.controller import Lab  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Reproduce a wiggle-lab recording.")
    ap.add_argument("recording", help="recording JSON exported from the lab UI")
    ap.add_argument("--no-wait", action="store_true", help="don't wait for readiness between steps")
    ap.add_argument("--settle", type=float, default=2.0, help="pause after each step (seconds)")
    args = ap.parse_args()

    with open(args.recording) as f:
        rec = json.load(f)

    events = rec.get("events", [])
    print(f"# recording {rec.get('id')} — {len(events)} events")
    if rec.get("note"):
        print(f"# note: {rec['note']}")
    print(f"# meta: {rec.get('meta', {})}\n")

    def on_event(res: dict):
        mark = {"ok": "✓", "error": "✗", "skip": "·"}.get(res["status"], "?")
        argstr = ", ".join(str(a) for a in res.get("args", []))
        line = f"[{res['seq']:>2}] {mark} {res['method']}({argstr})"
        if res.get("recorded_status") == "error" and res["status"] == "ok":
            line += "   (errored when recorded — did NOT reproduce)"
        print(line)
        if res["status"] == "error":
            print(f"       ↳ FAILED: {res['error']}")

    lab = Lab()
    results = lab.replay(rec, on_event=on_event, wait=not args.no_wait, settle=args.settle)

    print()
    failed = [r for r in results if r["status"] == "error"]
    if failed:
        f = failed[0]
        print(f"# REPRODUCED: failed at step {f['seq']} → {f['method']}: {f['error']}")
        print("# (cluster left running so you can inspect it: kubectl get pods -n wiggle-lab)")
        return 1
    print("# replay completed with no errors")
    if any(e.get("status") == "error" for e in events):
        print("# note: the recording contained an error that did NOT reproduce — likely timing/env dependent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
