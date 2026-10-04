"""Wiggle Lab — a Streamlit control panel to spin up wiggle servers on kind and play with them:
deploy servers (each with its own database), scale/kill/remove, and run client scenarios against the
real environment.

Run:  streamlit run app.py
"""
from __future__ import annotations

import json

import streamlit as st

from wigglelab import config as C
from wigglelab import kind, workflows
from wigglelab.controller import Lab

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


def render_tunables(specs: list[dict], current: dict, key_prefix: str, cols: int = 2) -> dict:
    """Render one widget per tunable, labelled by its raw WIGGLE_* env name and seeded with its current
    value (the live pod value if set, else the server default shown for reference). Returns only the
    values the user set away from the default -- those become pod env; everything else stays the
    server's own default. `current` holds live env values (strings)."""
    out: dict = {}
    columns = st.columns(cols)
    for i, s in enumerate(specs):
        col = columns[i % cols]
        key, default, kind_ = s["key"], s.get("default"), s["kind"]
        cur = current.get(key)
        wkey = f"{key_prefix}-{key}"
        if kind_ in ("int", "float"):
            seed = cur if cur not in (None, "") else ("" if default is None else str(default))
            raw = col.text_input(key, value=str(seed), key=wkey, help=s["help"]).strip()
            if raw == "":
                continue
            try:
                val = int(raw) if kind_ == "int" else float(raw)
            except ValueError:
                col.error(f"{key}: not a {kind_}")
                continue
            if val != default:
                out[key] = val
        elif kind_ == "bool":
            seed = (str(cur).lower() == "true") if cur is not None else bool(default)
            val = col.checkbox(key, value=seed, key=wkey, help=s["help"])
            if val != default:
                out[key] = val
        elif kind_ == "secret":
            raw = col.text_input(key, value=cur or "", key=wkey, type="password", help=s["help"]).strip()
            if raw:
                out[key] = raw
        elif kind_ == "enum":
            choices = s["choices"]
            seed = cur if cur in choices else (default if default in choices else choices[0])
            val = col.selectbox(key, choices, index=choices.index(seed), key=wkey, help=s["help"])
            if val != default:
                out[key] = val
    return out


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
    st.info("No cluster yet. Use the sidebar: **Create kind cluster → Build/Load image**, then deploy a "
            "server.")
    st.stop()

st.header("Wiggle servers" + ("  🔴 recording" if lab.is_recording() else ""))
cells = lab.cells()
st.metric("Servers", len(cells))

overview, cells_tab, client, forwards, logs_tab, db_tab = st.tabs(
    ["📊 Overview", "🗄 Servers", "🚀 Client tests", "🔌 Forwards", "📜 Logs", "🗃 Database"])

# ---- Overview ----
with overview:
    st.subheader("Servers")
    if cells:
        st.dataframe(
            [{"server": c["cell"], "ready": f'{c["ready"]}/{c["desired"]}'} for c in cells],
            use_container_width=True, hide_index=True)
    else:
        st.caption("No servers yet — deploy one in the **Servers** tab.")

    st.subheader("Pods")
    pods = lab.pods()
    if pods:
        st.dataframe(
            [{"pod": p["name"], "role": p["labels"].get("wiggle-lab/role", ""),
              "server": p["labels"].get("wiggle-lab/cell", ""), "phase": p["phase"],
              "ready": "✅" if p["ready"] else "⏳", "restarts": p["restarts"]} for p in pods],
            use_container_width=True, hide_index=True)

# ---- Servers ----
with cells_tab:
    st.subheader("Deploy a server")
    st.caption("A server gets its **own Postgres** container and one or more wiggle nodes pointed at "
               "it; the nodes form one cluster over that database.")
    with st.form("create_cell"):
        c1, c2 = st.columns(2)
        cell_id = c1.text_input("Server id", value="srv1")
        replicas = c2.number_input("Nodes", min_value=1, max_value=9, value=1)
        if st.form_submit_button("Deploy server"):
            action(f"Deploy server {cell_id}", lab.create_cell, cell_id, int(replicas),
                   spinner="Applying DB + server manifests…")
            st.rerun()

    st.divider()
    st.subheader("Manage servers")
    cell_cfgs = lab.all_cell_configs() if cells else {}   # one kubectl for all cells' live config
    for c in cells:
        cell = c["cell"]
        with st.container(border=True):
            h1, h2, h3, h4, h5, h6 = st.columns([2.6, 1, 1.1, 1.3, 1.2, 1.2])
            h1.markdown(f"**{cell}**  · {c['ready']}/{c['desired']} ready")
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
                action(f"Remove server {cell}", lab.remove_cell, cell)
                st.rerun()
            with st.expander("⚙️ Config"):
                st.caption("Operational tuning applied as pod env. Blank = server default. "
                           "**Apply & redeploy** rolls this server's pods with the new config.")
                with st.form(f"cellcfg-{cell}"):
                    values = render_tunables(C.CELL_TUNABLES, cell_cfgs.get(cell, {}), f"cellcfg-{cell}")
                    if st.form_submit_button("Apply & redeploy"):
                        action(f"Update config for {cell}", lab.update_cell_config, cell, values,
                               spinner="Re-applying server manifest (pods will roll)…")
                        st.rerun()

# ---- Client tests ----
with client:
    st.subheader("Run a client scenario")
    st.caption("Register a built-in workflow on a server, start instances, and watch them run. (The lab "
               "deploys no workers; `sleep`/`instant` flows still progress on the server; `park` stays "
               "RUNNING.)")
    servers = [c["cell"] for c in cells]
    if not servers:
        st.info("Deploy a server first.")
    else:
        c1, c2, c3 = st.columns([2, 2, 1])
        srv = c1.selectbox("Server", servers, key="client-srv")
        wf_key = c2.selectbox("Workflow", list(workflows.BUILTINS),
                              format_func=lambda k: f"{k} — {workflows.BUILTINS[k][0]}")
        count = c3.number_input("Instances", 1, 500, value=10)
        wf_builder = workflows.BUILTINS[wf_key][1]
        wf_def = wf_builder()
        wf_name = wf_def["name"]

        b1, b2, b3 = st.columns(3)
        if b1.button("① Register workflow", use_container_width=True):
            action(f"Register {wf_name} on {srv}", lab.register, srv, wf_def)
        if b2.button("② Start instances", use_container_width=True):
            res = action(f"Start {count} × {wf_name}", lab.start_instances, srv, wf_name, int(count))
            if res:
                st.write("started:", res["started"])
                for err in res["errors"][:5]:
                    st.warning(err)
        if b3.button("③ Observe", use_container_width=True):
            st.session_state["observe_srv"] = srv

        obs_srv = st.session_state.get("observe_srv")
        if obs_srv:
            st.divider()
            st.subheader(f"Instances on `{obs_srv}`")
            try:
                obs = lab.observe(obs_srv)
            except Exception as e:  # noqa: BLE001
                st.warning(f"could not read instances: {e}")
                obs = None
            if obs is not None:
                st.write(f"{obs['total']} instance(s) by status:", obs["by_status"] or "—")
            o1, o2 = st.columns(2)
            if o1.button("🔄 Re-observe"):
                st.rerun()
            if obs is not None and lab.is_recording() and o2.button("📌 Snapshot into recording"):
                lab.snapshot(f"observe:{obs_srv}", obs)
                st.toast("state snapshot added to recording")

# ---- Port-forwards ----
with forwards:
    st.subheader("Port-forwards")
    st.caption("Open a local port to a server so your host-run workers/clients can reach it. Point a "
               "worker at it with `WIGGLE_URL=<address>`. A server forward targets one backing pod (fine "
               "for a worker).")
    status = lab.forward_status()

    if not cells:
        st.caption("No servers yet — deploy one in the Servers tab.")
    for c in cells:
        cell = c["cell"]
        addr = status.get(cell)
        r1, r2, r3 = st.columns([2, 3, 1.3])
        r1.markdown(f"**{cell}**")
        r2.code(addr or "— not forwarded —", language=None)
        if addr:
            r2.caption(f"gRPC (worker): WIGGLE_URL={addr}")
            if r3.button("Stop", key=f"fw-stop-{cell}"):
                lab.stop_forward_cell(cell)
                st.rerun()
        elif r3.button("Forward", key=f"fw-{cell}"):
            action(f"Forward {cell}", lab.forward_cell, cell)
            st.rerun()

        # The Service balances over this server's pods; these rows pin a forward to ONE pod, for
        # talking to a specific node (the leader, a follower, a pod being debugged).
        pod_status = lab.pod_forward_status(cell)
        for pod, paddr in pod_status.items():
            p1, p2, p3 = st.columns([2, 3, 1.3])
            p1.markdown(f"&nbsp;&nbsp;&nbsp;↳ `{pod}`")
            p2.code(paddr or "— not forwarded —", language=None)
            if paddr:
                p2.caption(f"this pod only: WIGGLE_URL={paddr}")
                if p3.button("Stop", key=f"fw-stop-pod-{pod}"):
                    lab.stop_forward_pod(pod)
                    st.rerun()
            elif p3.button("Forward", key=f"fw-pod-{pod}"):
                action(f"Forward {pod}", lab.forward_pod, pod)
                st.rerun()

    st.divider()
    st.markdown("**Portal (web UI)**")
    st.caption("Every server node serves the portal; forward a server's and open the link. Set "
               "WIGGLE_DASHBOARD_PASSWORD (and optionally WIGGLE_DASHBOARD_VIEWER_PASSWORD) in the "
               "server's config to require a login; blank leaves it open. Further accounts and roles are "
               "managed in the portal's Users tab and kept in the server's database.")
    if not cells:
        st.caption("No servers yet — deploy one on the Servers tab first.")
    for ns in sorted(c["cell"] for c in cells):
        k1, k2, k3 = st.columns([2, 3, 1.3])
        k1.markdown(f"**{ns}**")
        addr = lab.portal_target(ns)
        if addr:
            k2.markdown(f"[http://{addr}](http://{addr})")
            if k3.button("Stop", key=f"portal-stop-{ns}"):
                lab.stop_forward_portal(ns)
                st.rerun()
        else:
            k2.code("— not forwarded —", language=None)
            if k3.button("Forward", key=f"portal-fw-{ns}"):
                action(f"Forward portal for {ns}", lab.forward_portal, ns)
                st.rerun()

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

# ---- Database ----
with db_tab:
    st.subheader("Databases (one Postgres per server)")
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
                st.caption("no user tables yet (has the server finished migrating?)")
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
