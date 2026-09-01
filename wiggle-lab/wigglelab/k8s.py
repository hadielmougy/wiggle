"""kubectl-driven Kubernetes operations against the lab's kind cluster."""
from __future__ import annotations

import json

from . import config as C
from . import kind
from . import shell


def kubectl(args: list[str], input_text: str | None = None, timeout: int = 120) -> shell.Result:
    return shell.run(["kubectl", "--context", kind.context(), "-n", C.K8S_NAMESPACE, *args],
                     input_text=input_text, timeout=timeout)


def kubectl_nons(args: list[str], input_text: str | None = None, timeout: int = 120) -> shell.Result:
    return shell.run(["kubectl", "--context", kind.context(), *args], input_text=input_text, timeout=timeout)


def apply(yaml_str: str) -> shell.Result:
    return kubectl_nons(["apply", "-f", "-"], input_text=yaml_str)


def scale(deployment: str, replicas: int) -> shell.Result:
    return kubectl(["scale", f"deployment/{deployment}", f"--replicas={replicas}"])


def rollout_restart(deployment: str) -> shell.Result:
    return kubectl(["rollout", "restart", f"deployment/{deployment}"])


def delete_pod(pod: str) -> shell.Result:
    return kubectl(["delete", "pod", pod, "--wait=false"])


def delete_by_label(selector: str) -> shell.Result:
    return kubectl(["delete", "deployment,statefulset,service,pod", "-l", selector, "--wait=false"],
                   timeout=120)


def get_json(resource: str, selector: str | None = None) -> dict:
    args = ["get", resource, "-o", "json"]
    if selector:
        args += ["-l", selector]
    r = kubectl(args)
    if not r.ok:
        return {"items": []}
    try:
        return json.loads(r.out)
    except json.JSONDecodeError:
        return {"items": []}


def pods(selector: str | None = None) -> list[dict]:
    out = []
    for it in get_json("pods", selector).get("items", []):
        meta, status = it.get("metadata", {}), it.get("status", {})
        cstats = status.get("containerStatuses", []) or []
        ready = bool(cstats) and all(c.get("ready") for c in cstats)
        restarts = sum(c.get("restartCount", 0) for c in cstats)
        out.append({
            "name": meta.get("name", ""),
            "labels": meta.get("labels", {}),
            "phase": status.get("phase", "?"),
            "ready": ready,
            "restarts": restarts,
            "ip": status.get("podIP", ""),
        })
    return out


def psql(pod: str, sql: str, tuples_only: bool = False, timeout: int = 30) -> shell.Result:
    """Run SQL in a Postgres pod via `kubectl exec ... psql`. With tuples_only, output is
    tab-separated rows with no header/decoration (easy to parse)."""
    flags = ["-U", "wiggle", "-d", "wiggle", "-P", "pager=off", "-v", "ON_ERROR_STOP=1"]
    if tuples_only:
        flags += ["-t", "-A", "-F", "\t"]
    return kubectl(["exec", pod, "--", "psql", *flags, "-c", sql], timeout=timeout)


def exec_sh(pod: str, script: str, timeout: int = 30) -> str:
    """Run a shell snippet inside a pod via `kubectl exec ... sh -c`."""
    r = kubectl(["exec", pod, "--", "sh", "-c", script], timeout=timeout)
    return r.out if r.ok else (r.err.strip() or r.out.strip() or "(no output)")


def logs(pod: str, tail: int = 200, previous: bool = False, prefix: bool = True) -> str:
    args = ["logs", pod, f"--tail={tail}", "--all-containers=true"]
    if prefix:
        args.append("--prefix=true")
    if previous:
        args.append("--previous")
    r = kubectl(args, timeout=30)
    return r.out if r.ok else (r.err or r.out or "(no logs)")


def deployments(selector: str | None = None) -> list[dict]:
    out = []
    for it in get_json("deployments", selector).get("items", []):
        meta, spec, status = it.get("metadata", {}), it.get("spec", {}), it.get("status", {})
        out.append({
            "name": meta.get("name", ""),
            "labels": meta.get("labels", {}),
            "desired": spec.get("replicas", 0),
            "ready": status.get("readyReplicas", 0) or 0,
        })
    return out
