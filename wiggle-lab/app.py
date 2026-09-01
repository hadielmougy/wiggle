"""Wiggle Lab — a Streamlit control panel to spin up a wiggle cell cluster on kind and play with it:
deploy cells (each with its own database), reshard via epochs, scale/kill/remove, and run client
scenarios against the real environment.

Run:  streamlit run app.py
"""
from __future__ import annotations

import json

import streamlit as st

from wigglelab import config as C
from wigglelab import kind, workflows
from wigglelab.controller import Lab
from wigglelab.ringspec import parse_ring

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:  # optional dep; without it, refresh stays manual
    st_autorefresh = None

st.set_page_config(page_title="Wiggle Lab", page_icon="🧫", layout="wide")


@st.cache_resource
def get_lab() -> Lab:
    return Lab()


lab = get_lab()


def action(label: str, fn, *args, spinner: str | None = None, **kwargs):
    """Run a controller action with error surfacing; returns the result or None."""
    try:
        with st.spinner(spinner or f"{label}…"):
            res = fn(*args, **kwargs)
        st.success(label + " ✓")
        return res
    except Exception as e:  # noqa: BLE001
        st.error(f"{label} failed: {e}")
        return None


def _build_image():
    """Stream the docker build to the server console; raise on non-zero exit."""
    code = 0
    for kind_, val in kind.build_image_stream():
        if kind_ == "line":
            print(val, flush=True)
        else:
            code = val
    if code != 0:
        raise RuntimeError(f"docker build exited {code}")


# ─────────────────────────────── sidebar: prerequisites + cluster lifecycle ───────────────────────
with st.sidebar:
    st.title("🧫 Wiggle Lab")
    st.caption(f"cluster `{C.CLUSTER}` · image `{C.IMAGE}`")

    pr = lab.prereqs()
    st.write(" ".join(f"{'✅' if ok else '❌'} {b}" for b, ok in pr.items()))
    if not all(pr.values()):
        st.warning("Install the missing tools (docker, kind, kubectl) to use the lab.")

    exists = lab.cluster_exists() if pr.get("kind") else False
    img = lab.image_local() if pr.get("docker") else False

    st.divider()
    st.subheader("Cluster")
    st.write(f"kind cluster: {'🟢 up' if exists else '⚪️ down'}")
    st.write(f"image `{C.IMAGE}`: {'🟢 present' if img else '⚪️ missing'}")

    if not exists:
        if st.button("① Create kind cluster", use_container_width=True):
            action("Create cluster", lab.create_cluster, spinner="Creating kind cluster (~30s)…")
            st.rerun()
    else:
        if not img:
            st.info("Build the image (compiles Java + dashboard — several minutes), then load it.")
        c1, c2 = st.columns(2)
        if c1.button("Build image", use_container_width=True, disabled=not pr.get("docker")):
            st.info("Building `%s` from the repo Dockerfile — watch your terminal; this is slow." % C.IMAGE)
            action("Build image", _build_image, spinner="docker build (several minutes)…")
            st.rerun()
        if c2.button("② Load image → kind", use_container_width=True, disabled=not img):
            action("Load image into kind", lab.load_image, spinner="kind load docker-image…")
        if st.button("③ Deploy coordinator", use_container_width=True, disabled=not img,
                     help="Deploys a Ratis group; redeploying re-forms it fresh (wipes nodes/epochs/policies)."):
            action("Deploy coordinator", lab.deploy_coordinator,
                   st.session_state.get("coord_size", C.COORD_DEFAULT_GROUP_SIZE))
            st.rerun()
        with st.expander("🗄 disk"):
            st.caption("Postgres `initdb` fails with \"No space left on device\" when the node fills — "
                       "check **inodes** too, not just bytes.")
            st.markdown("**kind node**")
            st.code(lab.node_disk(), language="text")
            if st.button("🧹 Reclaim node disk", use_container_width=True,
                         help="prune images the node isn't using (safe; keeps running pods' images)"):
                action("Reclaim node disk", lab.reclaim_disk, spinner="pruning unused node images…")
                st.rerun()
            st.markdown("**host Docker**")
            st.code(lab.host_disk(), language="text")
            st.caption("Reclaim build cache from repeated image builds with "
                       "`docker builder prune -af` (safe). Do NOT `docker volume prune` — it deletes "
                       "other projects' volumes (minikube, other DBs).")

        st.divider()
        if st.button("🩺 Collect errors from all pods", use_container_width=True):
            with st.spinner("scanning pod logs…"):
                st.session_state["error_report"] = lab.collect_pod_errors()
        rep = st.session_state.get("error_report")
        if rep:
            st.caption(f"{rep.count(chr(10)) + 1} line(s) collected")
            st.download_button("⬇︎ Download errors", data=rep, file_name="wiggle-lab-errors.txt",
                               mime="text/plain", use_container_width=True)
            if st.button("Discard errors", use_container_width=True):
                st.session_state.pop("error_report", None)
                st.rerun()

        st.divider()
        if st.button("🧨 Tear down cluster", type="primary", use_container_width=True):
            action("Teardown", lab.teardown, spinner="Deleting kind cluster…")
            st.rerun()

    st.divider()
    st.subheader("Recording")
    st.caption("Record what you do; if you hit an issue, stop and download the sequence to send for a fix.")
    if lab.is_recording():
        st.markdown(f"🔴 **Recording** · {len(lab.recording_events())} step(s)")
        if st.button("■ Stop recording", type="primary", use_container_width=True):
            st.session_state["stopped_recording"] = lab.stop_recording()
            st.rerun()
        with st.expander("recorded steps"):
            for e in lab.recording_events():
                mark = "✓" if e.status == "ok" else "✗"
                st.write(f"{e.seq}. {mark} `{e.method}` {e.args}")
    else:
        note = st.text_input("scenario note (optional)", key="rec_note",
                             placeholder="what are you testing?")
        if st.button("● Start recording", use_container_width=True):
            lab.start_recording(note or "")
            st.session_state.pop("stopped_recording", None)
            st.rerun()

    stopped = st.session_state.get("stopped_recording")
    if stopped is not None and not lab.is_recording():
        st.success(f"captured recording `{stopped.id}` — {len(stopped.events)} step(s)")
        issue = st.text_area("describe the issue (optional)", key="issue_note",
                             placeholder="what went wrong / what you expected")
        payload = stopped.to_dict()
        if issue:
            payload["note"] = (payload.get("note", "") + "\n\nISSUE: " + issue).strip()
        st.download_button("⬇︎ Download recording JSON", data=json.dumps(payload, indent=2),
                           file_name=f"wiggle-lab-{stopped.id}.json", mime="application/json",
                           use_container_width=True)
        st.caption("Send this file over to reproduce & fix (replay it with `python replay.py <file>`).")

    st.divider()
    ar1, ar2 = st.columns([1.3, 1])
    auto = ar1.checkbox("Auto-refresh", value=False, help="periodically re-read cluster status")
    interval = ar2.selectbox("every", [2, 5, 10, 30], index=1, format_func=lambda s: f"{s}s",
                             label_visibility="collapsed", disabled=not auto)
    if auto:
        if st_autorefresh:
            st_autorefresh(interval=interval * 1000, key="autorefresh")
        else:
            st.caption("`pip install -r requirements.txt` to enable auto-refresh")
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()


# ─────────────────────────────── main ───────────────────────
if not lab.cluster_exists():
    st.info("No cluster yet. Use the sidebar: **Create kind cluster → Build/Load image → Deploy coordinator**.")
    st.stop()

coord_ready = lab.coordinator_ready()
st.header("Wiggle cell cluster" + ("  🔴 recording" if lab.is_recording() else ""))
cols = st.columns(3)
cols[0].metric("Coordinator", "ready" if coord_ready else "pending")
cells = lab.cells()
cols[1].metric("Cells", len(cells))
cols[2].metric("Namespaces", len({c["namespace"] for c in cells if c["namespace"]}))

overview, cells_tab, placement, coord_tab, client, forwards, logs_tab, db_tab = st.tabs(
    ["📊 Overview", "🗄 Cells", "🧭 Placement (epochs)", "⚙️ Coordinator", "🚀 Client tests",
     "🔌 Forwards", "📜 Logs", "🗃 Database"])

# ---- Overview ----
with overview:
    st.subheader("Cells")
    if cells:
        st.dataframe(
            [{"cell": c["cell"], "namespace": c["namespace"], "ready": f'{c["ready"]}/{c["desired"]}'}
             for c in cells],
            use_container_width=True, hide_index=True)
    else:
        st.caption("No cells yet — create one in the **Cells** tab.")

    st.subheader("Pods")
    pods = lab.pods()
    if pods:
        st.dataframe(
            [{"pod": p["name"], "role": p["labels"].get("wiggle-lab/role", ""),
              "cell": p["labels"].get("wiggle-lab/cell", ""), "phase": p["phase"],
              "ready": "✅" if p["ready"] else "⏳", "restarts": p["restarts"]} for p in pods],
            use_container_width=True, hide_index=True)

# ---- Cells ----
with cells_tab:
    st.subheader("Create a cell")
    st.caption("A cell gets its **own Postgres** container and one or more wiggle nodes pointed at it "
               "and at the coordinator.")
    with st.form("create_cell"):
        c1, c2, c3, c4 = st.columns(4)
        cell_id = c1.text_input("Cell id", value="cellA")
        ns = c2.text_input("Namespace", value="orders")
        replicas = c3.number_input("Nodes", min_value=1, max_value=9, value=1)
        region = c4.text_input("Region", value="")
        if st.form_submit_button("Create cell", disabled=not coord_ready):
            action(f"Create cell {cell_id}", lab.create_cell, cell_id, ns, int(replicas), region,
                   spinner="Applying DB + cell manifests…")
            st.rerun()
    if not coord_ready:
        st.warning("Deploy the coordinator (sidebar) before creating cells.")

    st.divider()
    st.subheader("Manage cells")
    for c in cells:
        cell = c["cell"]
        with st.container(border=True):
            h1, h2, h3, h4, h5, h6 = st.columns([2.6, 1, 1.1, 1.3, 1.2, 1.2])
            h1.markdown(f"**{cell}**  · ns `{c['namespace']}`  · {c['ready']}/{c['desired']} ready")
            n = h2.number_input("scale", 0, 9, value=c["desired"], key=f"scale-{cell}",
                                label_visibility="collapsed")
            if h3.button("Scale", key=f"do-scale-{cell}"):
                action(f"Scale {cell}", lab.scale_cell, cell, int(n))
                st.rerun()
            if h4.button("Restart", key=f"restart-{cell}", help="roll the pods (e.g. after an image reload)"):
                action(f"Restart {cell}", lab.restart_cell, cell)
                st.rerun()
            if h5.button("Kill pod", key=f"kill-{cell}"):
                action(f"Kill a pod of {cell}", lab.kill_cell_pod, cell)
                st.rerun()
            if h6.button("Remove", key=f"rm-{cell}"):
                action(f"Remove cell {cell}", lab.remove_cell, cell)
                st.rerun()

# ---- Placement ----
with placement:
    st.subheader("Open an epoch (reshard)")
    st.caption("Publish a shard→cell ring. Opening a new epoch marks the previous one **DRAINING**; "
               "the coordinator retires it automatically once its instances finish.")
    all_ns = sorted({c["namespace"] for c in cells if c["namespace"]})
    with st.form("open_epoch"):
        ns = st.selectbox("Namespace", all_ns or ["orders"])
        st.caption("Ring slots as `shard=cellId[@region]`, separated by commas/spaces/newlines "
                   "(e.g. `0=cellA, 1=cellB`).")
        ring_text = st.text_area("Ring", value="0=cellA", height=80, label_visibility="collapsed")
        if st.form_submit_button("Open epoch", disabled=not coord_ready):
            ring = parse_ring(ring_text)
            if ring is None:
                st.error("Bad ring — use `shard=cellId[@region]` per line.")
            else:
                pol = action(f"Open epoch for {ns}", lab.open_epoch, ns, ring)
                if pol:
                    st.json(pol)

    st.divider()
    st.subheader("Current policy (cached)")
    for ns in all_ns:
        pol = lab.policy(ns)
        if pol:
            with st.expander(f"namespace `{ns}` — current epoch {pol.get('current_epoch')}  "
                             f"(rev {pol.get('revision')})"):
                st.json(pol)


# ---- Client tests ----
with client:
    st.subheader("Run a client scenario")
    st.caption("Allocate a built-in workflow to a namespace, start instances, and watch where they "
               "land across cells/epochs. (v1 deploys no workers; `sleep`/`instant` flows still "
               "progress on the server; `park` stays RUNNING so you can see distribution.)")
    all_ns = sorted({c["namespace"] for c in cells if c["namespace"]})
    if not all_ns:
        st.info("Create a cell and open an epoch first.")
    else:
        c1, c2, c3 = st.columns([2, 2, 1])
        ns = c1.selectbox("Namespace", all_ns, key="client-ns")
        wf_key = c2.selectbox("Workflow", list(workflows.BUILTINS),
                              format_func=lambda k: f"{k} — {workflows.BUILTINS[k][0]}")
        count = c3.number_input("Instances", 1, 500, value=10)
        wf_builder = workflows.BUILTINS[wf_key][1]
        wf_def = wf_builder()
        wf_name = wf_def["name"]

        b1, b2, b3 = st.columns(3)
        if b1.button("① Allocate workflow", use_container_width=True):
            action(f"Allocate {wf_name} → {ns}", lab.allocate, ns, wf_name, wf_def)
        if b2.button("② Start instances", use_container_width=True):
            res = action(f"Start {count} × {wf_name}", lab.start_instances, ns, wf_name, int(count))
            if res:
                st.write("started per cell:", res["started"])
                for err in res["errors"][:5]:
                    st.warning(err)
        if b3.button("③ Observe", use_container_width=True):
            st.session_state["observe_ns"] = ns

        obs_ns = st.session_state.get("observe_ns")
        if obs_ns:
            st.divider()
            st.subheader(f"Instances in `{obs_ns}`")
            obs = lab.observe(obs_ns)
            st.write("totals by status:", obs["totals"] or "—")
            rows = []
            for cell, info in obs["per_cell"].items():
                if "error" in info:
                    rows.append({"cell": cell, "total": "error", "detail": info["error"][:60]})
                else:
                    rows.append({"cell": cell, "total": info["total"],
                                 "detail": ", ".join(f"{k}:{v}" for k, v in info["by_status"].items())})
            st.dataframe(rows, use_container_width=True, hide_index=True)
            o1, o2 = st.columns(2)
            if o1.button("🔄 Re-observe"):
                st.rerun()
            if lab.is_recording() and o2.button("📌 Snapshot into recording"):
                lab.snapshot(f"observe:{obs_ns}", obs)
                st.toast("state snapshot added to recording")

# ---- Port-forwards ----
with forwards:
    st.subheader("Port-forwards")
    st.caption("Open a local port to the coordinator or a cell so your host-run workers/clients can "
               "reach it. Point a worker at a cell with `WIGGLE_URL=<address>`. A cell forward targets "
               "one backing pod (fine for a worker).")
    status = lab.forward_status()

    fc1, fc2, fc3 = st.columns([2, 3, 1.3])
    fc1.markdown("**coordinator**")
    caddr = status.get("coordinator")
    fc2.code(caddr or "— not forwarded —", language=None)
    if caddr:
        if fc3.button("Stop", key="fw-stop-coord"):
            lab.stop_forward_coordinator()
            st.rerun()
    elif fc3.button("Forward", key="fw-coord"):
        action("Forward coordinator", lab.forward_coordinator)
        st.rerun()

    st.divider()
    if not cells:
        st.caption("No cells yet — create one in the Cells tab.")
    for c in cells:
        cell = c["cell"]
        addr = status.get(cell)
        r1, r2, r3 = st.columns([2, 3, 1.3])
        r1.markdown(f"**{cell}** · ns `{c['namespace']}`")
        r2.code(addr or "— not forwarded —", language=None)
        if addr:
            r2.caption(f"gRPC (worker): WIGGLE_URL={addr}")
            if r3.button("Stop", key=f"fw-stop-{cell}"):
                lab.stop_forward_cell(cell)
                st.rerun()
        elif r3.button("Forward", key=f"fw-{cell}"):
            action(f"Forward {cell}", lab.forward_cell, cell)
            st.rerun()

        # dashboard (HTTP) sub-row
        dash = lab.dashboard_target(cell)
        d1, d2, d3 = st.columns([2, 3, 1.3])
        d1.caption("↳ dashboard")
        if dash:
            d2.markdown(f"[http://{dash}](http://{dash})")
            if d3.button("Stop", key=f"dash-stop-{cell}"):
                lab.stop_forward_cell_dashboard(cell)
                st.rerun()
        else:
            d2.code("— not forwarded —", language=None)
            if d3.button("Forward", key=f"dash-{cell}"):
                action(f"Forward {cell} dashboard", lab.forward_cell_dashboard, cell)
                st.rerun()

    st.divider()
    st.markdown("**Client routing (multi-cell)**")
    st.caption("The coordinator hands clients in-cluster cell addresses (pod IPs). To reach them from a "
               "host-run worker/client AND spread starts across cells, run it with this "
               "`WIGGLE_ENDPOINT_REWRITE` — it maps each cell's pod IP to its own port-forward. Without it, "
               "every start lands on whichever single cell you rewrote to.")
    if st.button("Generate WIGGLE_ENDPOINT_REWRITE (forwards all cells)"):
        with st.spinner("forwarding cells…"):
            st.session_state["rewrite_spec"] = lab.endpoint_rewrite_spec()
    spec = st.session_state.get("rewrite_spec")
    if spec:
        st.code(spec, language="bash")

# ---- Logs ----
with logs_tab:
    st.subheader("Pod logs")
    all_pods = lab.pods()
    if not all_pods:
        st.caption("No pods yet.")
    else:
        def _plabel(p):
            role = p["labels"].get("wiggle-lab/role", "?")
            cell = p["labels"].get("wiggle-lab/cell", "")
            tag = f"{role}/{cell}" if cell else role
            state = "✅" if p["ready"] else f"⏳{p['phase']}"
            return f"{tag} · {p['name']} · {state}" + (f" · ↻{p['restarts']}" if p["restarts"] else "")

        options = {_plabel(p): p["name"] for p in all_pods}
        lc1, lc2, lc3 = st.columns([4, 1, 1])
        picked = lc1.selectbox("Pod", list(options))
        tail = lc2.number_input("tail", 20, 5000, 300, step=20)
        prev = lc3.checkbox("previous", help="logs from the previously crashed container")
        pod = options[picked]
        if st.button("🔄 Refresh logs"):
            st.rerun()
        text = lab.logs(pod, int(tail), previous=prev)
        st.code(text or "(no output)", language="text")

# ---- Coordinator ----
with coord_tab:
    st.subheader("Coordinator")
    cpods = lab.pods(role="coordinator")
    ready = sum(1 for p in cpods if p["ready"])
    size = lab.coordinator_group_size()
    s1, s2, s3 = st.columns([2, 1, 1])
    s1.markdown(f"Ratis group: **{ready}/{len(cpods)}** ready" + (f" · size {size}" if size else " · (none)"))
    opts = [1, 3, 5]
    cur = st.session_state.get("coord_size", C.COORD_DEFAULT_GROUP_SIZE)
    chosen = s2.selectbox("size", opts, index=opts.index(cur) if cur in opts else 1,
                          key="coord-size-sel", label_visibility="collapsed")
    st.session_state["coord_size"] = chosen
    if s3.button("Deploy", key="coord-deploy-btn", help="(re)form the group at this size — fresh state"):
        action(f"Deploy coordinator group ({chosen})", lab.deploy_coordinator, int(chosen))
        st.rerun()
    st.caption("One replicated Ratis group — any pod serves consistent state. The peer list is fixed, so "
               "it is not dynamically scalable: choose a size (odd for a majority); redeploying re-forms it "
               "fresh.")
    if cpods:
        st.dataframe([{"pod": p["name"], "phase": p["phase"], "ready": "✅" if p["ready"] else "⏳",
                       "restarts": p["restarts"]} for p in cpods], use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("**Store contents** — policies / namespaces / node roster / definitions "
                "(via the coordinator the Service routes to)")
    if st.button("Dump store", disabled=not lab.coordinator_ready()):
        try:
            st.json(lab.dump_coordinator_store())
        except Exception as e:  # noqa: BLE001
            st.error(f"dump failed: {e}")

    st.divider()
    st.markdown("**Store files per pod** — the Ratis log + RocksDB in each coordinator pod")
    for p in cpods:
        with st.expander(p["name"]):
            st.code(lab.coordinator_store_files(p["name"]), language="text")

# ---- Database ----
with db_tab:
    st.subheader("Databases (one Postgres per cell)")
    dbpods = lab.db_pods()
    if not dbpods:
        st.caption("No database pods yet.")
    else:
        opts = {f'{d["cell"]}  ·  {d["pod"]}' + ("" if d["ready"] else f'  ({d["phase"]})'): d["pod"]
                for d in dbpods}
        picked = st.selectbox("Database pod", list(opts), key="db-podsel")
        pod = opts[picked]

        left, right = st.columns([1, 2])
        with left:
            st.markdown("**Tables** — click to preview last 20 rows")
            try:
                tables = lab.list_tables(pod)
            except Exception as e:  # noqa: BLE001
                tables = []
                st.warning(f"could not list tables: {e}")
            if not tables:
                st.caption("no user tables yet (has the cell finished migrating?)")
            for t in tables:
                if st.button(f'{t["table"]}  ·  {t["rows"]} rows', use_container_width=True,
                             key=f'tbl-{pod}-{t["schema"]}-{t["table"]}'):
                    st.session_state["db_sql"] = (
                        f'SELECT * FROM "{t["schema"]}"."{t["table"]}" ORDER BY ctid DESC LIMIT 20;')
                    st.session_state["db_autorun"] = True
                    st.rerun()

        with right:
            st.markdown(f"**Query** on `{pod}` — edit and re-run")
            st.session_state.setdefault(
                "db_sql", "SELECT id, workflow, status FROM wf_instance ORDER BY updated_at DESC LIMIT 20;")
            st.text_area("SQL", key="db_sql", height=110, label_visibility="collapsed")
            run = st.button("Run query")
            if run or st.session_state.pop("db_autorun", False):
                with st.spinner("running…"):
                    out = lab.query(pod, st.session_state["db_sql"])
                st.code(out or "(no output)", language="text")
