"""The lab's orchestration brain. The Streamlit app holds one Lab instance and calls these methods;
all cluster/gRPC state lives here so the logic is usable without the UI.

Cluster state (which cells exist, their pods) is discovered live from Kubernetes labels, so it
survives app restarts. Placement policy (per-namespace ring) is cached from OpenEpoch responses and
persisted to disk, since the coordinator exposes no read-policy RPC.
"""
from __future__ import annotations

import json
import os
import pathlib
import random
import time
import uuid

from . import config as C
from . import k8s, kind, manifests
from .cell_client import CellClient
from .coord_client import CoordinatorClient
from .portforward import PortForwards
from .recorder import Event, Recording, record

STATE_DIR = pathlib.Path(os.environ.get("WIGGLE_LAB_HOME", os.path.expanduser("~/.wiggle-lab")))
STATE_FILE = STATE_DIR / "state.json"
RECORDINGS_DIR = STATE_DIR / "recordings"


class Lab:
    def __init__(self):
        self.pf = PortForwards()
        self.policies: dict[str, dict] = {}
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

    # ---- persisted policy cache ----
    def _load_state(self):
        try:
            self.policies = json.loads(STATE_FILE.read_text()).get("policies", {})
        except (OSError, json.JSONDecodeError):
            self.policies = {}

    def _save_state(self):
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps({"policies": self.policies}, indent=2))

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

    # ---- coordinator ----
    @record
    def deploy_coordinator(self, size: int = C.COORD_DEFAULT_GROUP_SIZE):
        """(Re)deploy the coordinator as one Ratis group of ``size`` pods (a StatefulSet behind a headless
        Service). Every pod serves the same replicated store, so a client reaching any pod sees consistent
        state. Redeploying deletes the old group and re-forms a fresh one (emptyDir stores start empty),
        wiping prior nodes/epochs/policies. A fixed peer list means it is not dynamically scalable — pick a
        size at deploy time (odd for a majority)."""
        self.ensure_namespace()
        self.pf.stop("coordinator")
        existed = bool(self.pods(role="coordinator")) or bool(
            k8s.get_json("statefulset", "wiggle-lab/role=coordinator").get("items"))
        if existed:
            k8s.delete_by_label("wiggle-lab/role=coordinator")
            self._wait(lambda: not self.pods(role="coordinator"), 120, "old coordinator removed")
            self.policies.clear()
            self._save_state()
        k8s.apply(manifests.to_yaml(manifests.coordinator_manifests(size))).check()

    def coordinator_ready(self) -> bool:
        return any(p["ready"] for p in k8s.pods(selector="wiggle-lab/role=coordinator"))

    def coordinator_group_size(self) -> int:
        items = k8s.get_json("statefulset", "wiggle-lab/role=coordinator").get("items", [])
        return items[0].get("spec", {}).get("replicas", 0) if items else 0

    def dump_coordinator_store(self) -> dict:
        """The coordinator store's logical contents (policies/namespaces/nodes/definitions) via the Dump
        RPC — served by whichever coordinator pod the Service routes to."""
        with self.coord_client() as cc:
            return cc.dump()

    def coordinator_store_files(self, pod: str) -> str:
        """The Ratis + RocksDB store files inside one coordinator pod (per-pod, so divergence is visible
        when scaled)."""
        return k8s.exec_sh(pod, "echo '# tree'; ls -R /var/lib/wiggle/coord 2>/dev/null; "
                                "echo; echo '# sizes'; du -sh /var/lib/wiggle/coord/* 2>/dev/null")

    # ---- readiness waits (used by replay to reproduce faithfully) ----
    def wait_coordinator_ready(self, timeout: int = 180):
        self._wait(self.coordinator_ready, timeout, "coordinator ready")

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

    def coord_client(self) -> CoordinatorClient:
        target = self.pf.ensure("coordinator", "coordinator", C.COORD_GRPC_PORT, C.COORD_LOCAL_PORT)
        return CoordinatorClient(target)

    # ---- cells ----
    def cells(self) -> list[dict]:
        """Live cell inventory from Kubernetes deployment labels."""
        out = []
        for d in k8s.deployments(selector="wiggle-lab/role=cell"):
            lb = d["labels"]
            out.append({
                "cell": lb.get("wiggle-lab/cell", d["name"]),
                "namespace": lb.get("wiggle-lab/namespace", ""),
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
    def create_cell(self, cell: str, namespace: str, replicas: int = 1, region: str = ""):
        self.ensure_namespace()
        docs = manifests.cell_db_manifests(cell) + manifests.cell_manifests(cell, namespace, replicas, region)
        k8s.apply(manifests.to_yaml(docs)).check()

    @record
    def scale_cell(self, cell: str, replicas: int):
        k8s.scale(C.dns_name("cell", cell), replicas).check()

    @record
    def restart_cell(self, cell: str):
        """Roll the cell's pods (e.g. after reloading a new image so nodes re-register with a fresh
        pod IP). Drops the stale port-forwards since the pods are being replaced."""
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
    def forward_coordinator(self) -> str:
        self.pf.ensure("coordinator", "coordinator", C.COORD_GRPC_PORT, C.COORD_LOCAL_PORT)
        return self.pf.target("coordinator") or ""

    def forward_cell(self, cell: str) -> str:
        self.pf.ensure(f"cell:{cell}", C.dns_name("cell", cell), C.CELL_GRPC_PORT,
                       self._cell_local_port(cell))
        return self.pf.target(f"cell:{cell}") or ""

    def stop_forward_coordinator(self):
        self.pf.stop("coordinator")

    def stop_forward_cell(self, cell: str):
        self.pf.stop(f"cell:{cell}")

    def _cell_dashboard_local_port(self, cell: str) -> int:
        ids = [c["cell"] for c in self.cells()]
        idx = ids.index(cell) if cell in ids else len(ids)
        return C.CELL_DASHBOARD_LOCAL_PORT_BASE + idx

    def forward_cell_dashboard(self, cell: str) -> str:
        self.pf.ensure(f"dash:{cell}", C.dns_name("cell", cell), C.CELL_DASHBOARD_PORT,
                       self._cell_dashboard_local_port(cell))
        return self.pf.target(f"dash:{cell}") or ""

    def stop_forward_cell_dashboard(self, cell: str):
        self.pf.stop(f"dash:{cell}")

    def dashboard_target(self, cell: str) -> str | None:
        return self.pf.target(f"dash:{cell}")

    def endpoint_rewrite_spec(self, namespace: str | None = None) -> str:
        """Build a WIGGLE_ENDPOINT_REWRITE value that maps each cell's live pod IP(s) to that cell's own
        port-forward, so a host-run client/worker routes to the RIGHT cell (not all to one). Ensures a
        forward per cell first. Restrict to one namespace with ``namespace``."""
        entries = []
        for c in self.cells():
            if namespace and c["namespace"] != namespace:
                continue
            cell = c["cell"]
            local = self.forward_cell(cell).split(":")[-1]   # ensure the forward; take its local port
            for p in self.pods(role="cell", cell=cell):
                if p.get("ip"):
                    entries.append(f"{p['ip']}:{C.CELL_GRPC_PORT}=127.0.0.1:{local}")
        return "WIGGLE_ENDPOINT_REWRITE=" + ",".join(entries) if entries else "WIGGLE_ENDPOINT_REWRITE="

    def forward_status(self) -> dict[str, str | None]:
        """Live local addresses for the coordinator and each cell forward (None if not forwarded)."""
        status: dict[str, str | None] = {"coordinator": self.pf.target("coordinator")}
        for c in self.cells():
            status[c["cell"]] = self.pf.target(f"cell:{c['cell']}")
        return status

    # ---- placement (epochs / rings) ----
    @record
    def open_epoch(self, namespace: str, ring: list[tuple[int, str, str]]) -> dict:
        with self.coord_client() as cc:
            policy = cc.open_epoch(namespace, ring)
        self.policies[namespace] = policy
        self._save_state()
        return policy

    def policy(self, namespace: str) -> dict | None:
        return self.policies.get(namespace)

    def current_ring(self, namespace: str) -> list[dict]:
        """The shard->cell slots of the namespace's current (OPEN) epoch, from the cached policy."""
        pol = self.policies.get(namespace)
        if not pol:
            return []
        epochs = pol.get("epochs", {})
        cur = str(pol.get("current_epoch", 0))
        return epochs.get(cur, {}).get("ring", [])

    # ---- workflows / client scenarios ----
    @record
    def allocate(self, namespace: str, name: str, definition: dict) -> dict:
        with self.coord_client() as cc:
            return cc.register_workflow(namespace, name, definition)

    def list_allocations(self, namespace: str) -> list[dict]:
        with self.coord_client() as cc:
            return cc.list_workflows(namespace)

    @record
    def start_instances(self, namespace: str, workflow: str, count: int) -> dict:
        """Start ``count`` instances, spread across the current epoch's ring cells (mirrors how the
        coordinator picks a random slot per new start). Returns per-cell counts and any errors."""
        ring = self.current_ring(namespace)
        if not ring:
            raise RuntimeError(f"namespace '{namespace}' has no open epoch yet — open one first")
        cells = sorted({s["cell_id"] for s in ring})
        started: dict[str, int] = {c: 0 for c in cells}
        errors: list[str] = []
        for _ in range(count):
            cell = random.choice(ring)["cell_id"]
            try:
                self.cell_client(cell).start_instance(workflow)
                started[cell] += 1
            except Exception as e:  # noqa: BLE001 - surface to UI
                errors.append(f"{cell}: {e}")
        return {"started": started, "errors": errors}

    def observe(self, namespace: str, workflow: str | None = None) -> dict:
        """Aggregate instance state per cell (each cell owns its own instances in its own DB)."""
        per_cell: dict[str, dict] = {}
        totals: dict[str, int] = {}
        cells = [c["cell"] for c in self.cells() if c["namespace"] == namespace]
        for cell in cells:
            try:
                instances = self.cell_client(cell).list_instances(workflow=workflow, limit=500)
            except Exception as e:  # noqa: BLE001
                per_cell[cell] = {"error": str(e)}
                continue
            counts: dict[str, int] = {}
            for i in instances:
                st = i.get("status", "?")
                counts[st] = counts.get(st, 0) + 1
                totals[st] = totals.get(st, 0) + 1
            per_cell[cell] = {"total": len(instances), "by_status": counts}
        return {"per_cell": per_cell, "totals": totals}

    # ---- replay (reproduce a recording) ----
    def replay(self, recording: dict, on_event=None, wait: bool = True, settle: float = 2.0) -> list[dict]:
        """Re-run a recording's events in order against the current machine. Stops at the first step
        that errors (the reproduction point). ``on_event`` gets each step result as it runs.

        Infra steps get a readiness barrier after them (coordinator/cell), so timing-sensitive
        sequences reproduce faithfully. Recording is off during replay, so nothing is re-captured.
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
            if method == "open_epoch" and len(args) >= 2:
                args[1] = [tuple(s) for s in args[1]]   # rings serialize to lists
            fn = getattr(self, method, None)
            status, err = "ok", None
            if not callable(fn):
                status, err = "error", f"unknown method '{method}'"
            else:
                try:
                    fn(*args, **kwargs)
                    if wait and method == "deploy_coordinator":
                        self.wait_coordinator_ready()
                    elif wait and method == "create_cell" and args:
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
