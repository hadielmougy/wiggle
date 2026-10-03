"""The lab's orchestration brain. The Streamlit app holds one Lab instance and calls these methods;
all cluster/gRPC state lives here so the logic is usable without the UI.

Cluster state (which servers exist, their pods) is discovered live from Kubernetes labels, so it
survives app restarts. Each server's tunables are persisted to disk.
"""
from __future__ import annotations

import json
import os
import pathlib
import time
import uuid

from . import config as C
from . import k8s, kind, manifests
from .cell_client import CellClient
from .portforward import PortForwards
from .recorder import Event, Recording, record

STATE_DIR = pathlib.Path(os.environ.get("WIGGLE_LAB_HOME", os.path.expanduser("~/.wiggle-lab")))
STATE_FILE = STATE_DIR / "state.json"
RECORDINGS_DIR = STATE_DIR / "recordings"


class Lab:
    def __init__(self):
        self.pf = PortForwards()
        self.cell_config: dict[str, dict] = {}   # cell -> {WIGGLE_*: value} applied to that cell
        self.recording: Recording | None = None
        self._load_state()

    # ---- recording lifecycle ----
    def start_recording(self, note: str = ""):
        self.recording = Recording(
            id=uuid.uuid4().hex[:8], created_at=time.time(),
            meta={"cluster": C.CLUSTER, "namespace": C.K8S_NAMESPACE, "image": C.IMAGE}, note=note)

    def stop_recording(self, note: str | None = None) -> Recording | None:
        rec = self.recording
        self.recording = None
        if rec is not None:
            if note is not None:
                rec.note = note
            self._save_recording(rec)
        return rec

    def is_recording(self) -> bool:
        return self.recording is not None

    def recording_events(self) -> list:
        return list(self.recording.events) if self.recording else []

    def snapshot(self, label: str, data: dict):
        """Record the current observed state (not an action) so a non-crash issue has context."""
        if self.recording is not None:
            self.recording.append(Event(
                seq=len(self.recording.events) + 1, ts=time.time(),
                method="snapshot", args=[label], kwargs={}, status="ok", data=data))

    def _save_recording(self, rec: Recording):
        RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
        (RECORDINGS_DIR / f"{rec.id}.json").write_text(rec.to_json())

    # ---- persisted state ----
    def _load_state(self):
        try:
            state = json.loads(STATE_FILE.read_text())
        except (OSError, json.JSONDecodeError):
            state = {}
        self.cell_config = state.get("cell_config", {})

    def _save_state(self):
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps({"cell_config": self.cell_config}, indent=2))

    # ---- live container env (source of truth for "current config" in the UI) ----
    def _live_env(self, selector: str, resource: str = "deployment") -> dict[str, str]:
        """The first container's literal env (name -> value) from the running spec, or {} if absent.
        Only ``value`` env entries are returned (valueFrom fieldRefs like POD_IP are skipped)."""
        items = k8s.get_json(resource, selector).get("items", [])
        if not items:
            return {}
        conts = items[0].get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
        env = {}
        for e in (conts[0].get("env", []) if conts else []):
            if "value" in e:
                env[e["name"]] = e["value"]
        return env

    # ---- prerequisites / cluster lifecycle ----
    def prereqs(self) -> dict[str, bool]:
        return kind.prereqs()

    def cluster_exists(self) -> bool:
        return kind.cluster_exists()

    def image_local(self) -> bool:
        return kind.image_exists_local()

    def node_disk(self) -> str:
        return kind.node_disk()

    def host_disk(self) -> str:
        return kind.host_docker_df()

    def reclaim_disk(self):
        kind.prune_node_images().check()

    @record
    def create_cluster(self):
        kind.create_cluster().check()

    @record
    def load_image(self):
        kind.load_image().check()

    def ensure_namespace(self):
        k8s.apply(manifests.to_yaml([manifests.namespace_manifest()])).check()

    def teardown(self):
        self.pf.stop_all()
        kind.delete_cluster()

    # ---- readiness waits (used by replay to reproduce faithfully) ----
    def wait_cell_ready(self, cell: str, timeout: int = 180):
        def ready():
            ps = self.pods(role="cell", cell=cell)
            return bool(ps) and all(p["ready"] for p in ps)
        self._wait(ready, timeout, f"cell '{cell}' ready")

    @staticmethod
    def _wait(pred, timeout: int, what: str):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if pred():
                return
            time.sleep(2)
        raise TimeoutError(f"timed out after {timeout}s waiting for {what}")

    # ---- cells ----
    def cells(self) -> list[dict]:
        """Live cell inventory from Kubernetes deployment labels."""
        out = []
        for d in k8s.deployments(selector="wiggle-lab/role=cell"):
            lb = d["labels"]
            out.append({
                "cell": lb.get("wiggle-lab/cell", d["name"]),
                "deployment": d["name"],
                "desired": d["desired"],
                "ready": d["ready"],
            })
        return sorted(out, key=lambda c: c["cell"])

    def pods(self, role: str | None = None, cell: str | None = None) -> list[dict]:
        sel = ["app.kubernetes.io/part-of=wiggle-lab"]
        if role:
            sel.append(f"wiggle-lab/role={role}")
        if cell:
            sel.append(f"wiggle-lab/cell={cell}")
        return k8s.pods(selector=",".join(sel))

    def logs(self, pod: str, tail: int = 200, previous: bool = False) -> str:
        return k8s.logs(pod, tail=tail, previous=previous)

    def collect_pod_errors(self, tail: int = 3000) -> str:
        """Scan every pod's recent logs for error/warning lines (plus their stack-trace continuations)
        and return one report. For grabbing failures before tearing the cluster down."""
        import re
        pat = re.compile(r"ERROR|SEVERE|FATAL|WARNING|Exception|Traceback|Caused by|\bfailed\b")
        cont = ("at ", "Caused by", "...", "Suppressed:")
        sections = []
        for p in self.pods():
            name = p["name"]
            role = p["labels"].get("wiggle-lab/role", "?")
            text = k8s.logs(name, tail=tail, prefix=False)
            hits = [ln for ln in text.splitlines()
                    if pat.search(ln) or ln.strip().startswith(cont)]
            if hits:
                sections.append(f"===== {name} ({role}) — {len(hits)} line(s) =====")
                sections.extend(hits)
                sections.append("")
        return "\n".join(sections) if sections else "(no error/warning lines found across pods)"

    # ---- per-cell database inspection ----
    def db_pod(self, cell: str) -> str | None:
        ps = self.pods(role="db", cell=cell)
        return ps[0]["name"] if ps else None

    def db_pods(self) -> list[dict]:
        """All Postgres pods (one per cell), each tagged with its cell label."""
        return [{"pod": p["name"], "cell": p["labels"].get("wiggle-lab/cell", "?"),
                 "ready": p["ready"], "phase": p["phase"]}
                for p in self.pods(role="db")]

    def list_tables(self, pod: str) -> list[dict]:
        sql = ("SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables "
               "ORDER BY schemaname, relname")
        r = k8s.psql(pod, sql, tuples_only=True)
        if not r.ok:
            raise RuntimeError(r.err.strip() or r.out.strip() or "query failed")
        rows = []
        for line in r.out.strip().splitlines():
            parts = line.split("\t")
            if len(parts) >= 3:
                rows.append({"schema": parts[0], "table": parts[1], "rows": parts[2]})
        return rows

    def query(self, pod: str, sql: str) -> str:
        """Run arbitrary SQL against a specific Postgres pod and return psql's rendered output."""
        r = k8s.psql(pod, sql)
        return r.out if r.ok else (r.err.strip() or r.out.strip() or "(no output)")

    @record
    def create_cell(self, cell: str, replicas: int = 1, tunables: dict | None = None):
        self._apply_cell(cell, replicas, tunables)

    def _apply_cell(self, cell: str, replicas: int, tunables: dict | None):
        """(Re)apply a cell's DB + node manifests. Persisting the tunables and re-applying updates the
        Deployment spec, so k8s rolls the pods with the new config."""
        self.ensure_namespace()
        self.cell_config[cell] = {k: v for k, v in (tunables or {}).items() if v is not None}
        self._save_state()
        docs = (manifests.cell_db_manifests(cell)
                + manifests.cell_manifests(cell, replicas, self.cell_config[cell]))
        k8s.apply(manifests.to_yaml(docs)).check()

    @record
    def update_cell_config(self, cell: str, tunables: dict):
        """Apply edited config to an existing cell and redeploy it, keeping its replica count."""
        c = next((x for x in self.cells() if x["cell"] == cell), None)
        if c is None:
            raise RuntimeError(f"unknown cell '{cell}'")
        self._apply_cell(cell, int(c["desired"]) or 1, tunables)

    def cell_config(self, cell: str) -> dict:
        """The cell's current tunables for the UI: live pod env wins (source of truth), else the persisted
        config, filtered to the editable keys."""
        keys = {s["key"] for s in C.CELL_TUNABLES}
        env = self._live_env(f"wiggle-lab/role=cell,wiggle-lab/cell={cell}")
        live = {k: v for k, v in env.items() if k in keys}
        return live or dict(self.cell_config.get(cell, {}))

    def all_cell_configs(self) -> dict[str, dict]:
        """Live tunables for every cell in one kubectl call (so the Cells tab doesn't read per-cell each
        render). Falls back to persisted config for any cell whose live env can't be read."""
        keys = {s["key"] for s in C.CELL_TUNABLES}
        out: dict[str, dict] = {}
        for it in k8s.get_json("deployment", "wiggle-lab/role=cell").get("items", []):
            cell = it.get("metadata", {}).get("labels", {}).get("wiggle-lab/cell")
            if not cell:
                continue
            conts = it.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
            env = {e["name"]: e["value"] for e in (conts[0].get("env", []) if conts else []) if "value" in e}
            out[cell] = {k: v for k, v in env.items() if k in keys} or dict(self.cell_config.get(cell, {}))
        return out

    @record
    def scale_cell(self, cell: str, replicas: int):
        k8s.scale(C.dns_name("cell", cell), replicas).check()

    @record
    def restart_cell(self, cell: str):
        """Roll the cell's pods (e.g. after reloading a new image). Drops the stale port-forwards since
        the pods are being replaced."""
        self.pf.stop(f"cell:{cell}")
        self.pf.stop(f"dash:{cell}")
        k8s.rollout_restart(C.dns_name("cell", cell)).check()

    @record
    def remove_cell(self, cell: str):
        self.pf.stop(f"cell:{cell}")
        self.pf.stop(f"dash:{cell}")
        k8s.delete_by_label(f"wiggle-lab/cell={cell}").check()

    @record
    def kill_cell_pod(self, cell: str):
        """Kill one (arbitrary) pod of a cell — recorded by cell, so it replays regardless of pod names."""
        ps = self.pods(role="cell", cell=cell)
        if not ps:
            raise RuntimeError(f"no pods for cell '{cell}'")
        self.kill_pod(ps[0]["name"])

    def kill_pod(self, pod: str):
        k8s.delete_pod(pod).check()

    def _cell_local_port(self, cell: str) -> int:
        ids = [c["cell"] for c in self.cells()]
        idx = ids.index(cell) if cell in ids else len(ids)
        return C.CELL_LOCAL_PORT_BASE + idx

    def cell_client(self, cell: str) -> CellClient:
        target = self.pf.ensure(f"cell:{cell}", C.dns_name("cell", cell),
                                C.CELL_GRPC_PORT, self._cell_local_port(cell))
        return CellClient(target)

    # ---- port-forwards (so host-run workers/clients can reach in-cluster gRPC) ----
    def forward_cell(self, cell: str) -> str:
        self.pf.ensure(f"cell:{cell}", C.dns_name("cell", cell), C.CELL_GRPC_PORT,
                       self._cell_local_port(cell))
        return self.pf.target(f"cell:{cell}") or ""

    def stop_forward_cell(self, cell: str):
        self.pf.stop(f"cell:{cell}")

    # ---- per-pod forwards: a cell Service balances over its pods, so reaching ONE node (its
    # health port, its gRPC as the leader or a follower specifically) needs a forward pinned to
    # the pod. Pod names churn on restart; a stale forward just dies with its pod, and the
    # status map only ever reports live pods.
    def _pod_local_port(self, pod: str) -> int:
        names = sorted(p["name"] for p in self.pods(role="cell"))
        idx = names.index(pod) if pod in names else len(names)
        return C.POD_LOCAL_PORT_BASE + idx

    def forward_pod(self, pod: str) -> str:
        self.pf.ensure(f"pod:{pod}", pod, C.CELL_GRPC_PORT, self._pod_local_port(pod),
                       resource="pod")
        return self.pf.target(f"pod:{pod}") or ""

    def stop_forward_pod(self, pod: str):
        self.pf.stop(f"pod:{pod}")

    def pod_forward_status(self, cell: str | None = None) -> dict[str, str | None]:
        """Live local address per cell pod (None if not forwarded)."""
        return {p["name"]: self.pf.target(f"pod:{p['name']}")
                for p in self.pods(role="cell", cell=cell)}

    # ---- ops console (per-server pod: a gRPC client of the server + the web UI) ----
    @record
    def deploy_console(self, target: str, password: str | None = None,
                       viewer_password: str | None = None):
        """A console for the server ``target``."""
        self.ensure_namespace()
        docs = manifests.console_manifests(target, password or None, viewer_password or None)
        k8s.apply(manifests.to_yaml(docs)).check()

    @record
    def remove_console(self, target: str):
        self.pf.stop(f"console:{target}")
        k8s.delete_by_label(f"wiggle-lab/role=console,wiggle-lab/console={C.dns_safe(target)}")

    def consoles(self) -> list[dict]:
        out = []
        for d in k8s.deployments(selector="wiggle-lab/role=console"):
            lb = d["labels"]
            server = lb.get("wiggle-lab/cell", "")
            out.append({"target": lb.get("wiggle-lab/console", server), "server": server,
                        "deployment": d["name"], "desired": d["desired"], "ready": d["ready"]})
        return sorted(out, key=lambda c: c["target"])

    def _console_local_port(self, target: str) -> int:
        keys = [c["target"] for c in self.consoles()]
        key = C.dns_safe(target)
        idx = keys.index(key) if key in keys else len(keys)
        return C.CONSOLE_LOCAL_PORT_BASE + idx

    def forward_console(self, target: str) -> str:
        self.pf.ensure(f"console:{target}", C.dns_name("console", target),
                       C.CONSOLE_HTTP_PORT, self._console_local_port(target))
        return self.pf.target(f"console:{target}") or ""

    def stop_forward_console(self, target: str):
        self.pf.stop(f"console:{target}")

    def console_target(self, target: str) -> str | None:
        return self.pf.target(f"console:{target}")

    def forward_status(self) -> dict[str, str | None]:
        """Live local address of each cell forward (None if not forwarded)."""
        status: dict[str, str | None] = {}
        for c in self.cells():
            status[c["cell"]] = self.pf.target(f"cell:{c['cell']}")
        return status

    # ---- workflows / client scenarios ----
    @record
    def register(self, cell: str, definition: dict) -> dict:
        with self.cell_client(cell) as cc:
            return cc.register_workflow(definition)

    def list_workflows(self, cell: str) -> list[str]:
        with self.cell_client(cell) as cc:
            return cc.list_workflow_names()

    @record
    def start_instances(self, cell: str, workflow: str, count: int) -> dict:
        """Start ``count`` instances on one server. Returns how many started and any errors."""
        started, errors = 0, []
        with self.cell_client(cell) as cc:
            for _ in range(count):
                try:
                    cc.start_instance(workflow)
                    started += 1
                except Exception as e:  # noqa: BLE001 - surface to UI
                    errors.append(str(e))
        return {"started": started, "errors": errors}

    def observe(self, cell: str, workflow: str | None = None) -> dict:
        """Instance counts by status on one server."""
        with self.cell_client(cell) as cc:
            instances = cc.list_instances(workflow=workflow, limit=500)
        counts: dict[str, int] = {}
        for i in instances:
            st = i.get("status", "?")
            counts[st] = counts.get(st, 0) + 1
        return {"total": len(instances), "by_status": counts}

    # ---- replay (reproduce a recording) ----
    def replay(self, recording: dict, on_event=None, wait: bool = True, settle: float = 2.0) -> list[dict]:
        """Re-run a recording's events in order against the current machine. Stops at the first step
        that errors (the reproduction point). ``on_event`` gets each step result as it runs.

        Creating a cell gets a readiness barrier after it, so timing-sensitive sequences reproduce
        faithfully. Recording is off during replay, so nothing is re-captured.
        """
        results = []
        for ev in recording.get("events", []):
            method = ev.get("method")
            if method == "snapshot":
                res = {"seq": ev.get("seq"), "method": "snapshot", "status": "skip", "error": None,
                       "recorded_status": "ok"}
                results.append(res)
                if on_event:
                    on_event(res)
                continue
            args = list(ev.get("args", []))
            kwargs = dict(ev.get("kwargs", {}))
            fn = getattr(self, method, None)
            status, err = "ok", None
            if not callable(fn):
                status, err = "error", f"unknown method '{method}'"
            else:
                try:
                    fn(*args, **kwargs)
                    if wait and method == "create_cell" and args:
                        self.wait_cell_ready(args[0])
                    time.sleep(settle)
                except Exception as e:  # noqa: BLE001
                    status, err = "error", f"{type(e).__name__}: {e}"
            res = {"seq": ev.get("seq"), "method": method, "args": args,
                   "status": status, "error": err, "recorded_status": ev.get("status")}
            results.append(res)
            if on_event:
                on_event(res)
            if status == "error":
                break   # first failure is the reproduction point
        return results
