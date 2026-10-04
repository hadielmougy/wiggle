# Wiggle Lab

A local control panel for **playing with real wiggle servers** on Kubernetes (via
[kind](https://kind.sigs.k8s.io/)) — deploy servers, scale, kill, and run client scenarios, all from a
Streamlit UI. Built to make manual, multi-scenario testing fast instead
of tedious.

It talks to the cluster two ways:
- **kind / kubectl** (subprocess) for infrastructure: cluster, servers (each with its own Postgres),
  consoles, scaling, killing pods.
- **gRPC** (Python stubs generated from `proto/`) for the control plane: register workflows, start
  instances, observe state.

## What you can do

- Create/tear down a kind cluster.
- Deploy a **server**: its own Postgres container and N wiggle nodes over it.
- **Scale** a server up/down, **kill** individual pods, **remove** it (and its DB).
- Run **client scenarios**: start instances and watch them run.

## Prerequisites

`docker`, `kind`, `kubectl`, and `python3` on your PATH. (The sidebar shows which are present.)

## Setup

```bash
cd wiggle-lab
./setup.sh                     # venv + deps + generate gRPC stubs from ../proto
source .venv/bin/activate
streamlit run app.py           # opens http://localhost:8501
```

`setup.sh` runs `gen_proto.sh`, which compiles `proto/src/main/proto/wiggle.proto` into
`wigglelab/pb/`. Re-run `./gen_proto.sh` if the protos change.

## Typical flow (in the UI)

1. **Sidebar → Create kind cluster.**
2. **Build image** (compiles the Java dist + dashboard — several minutes; or build `wiggle:local`
   yourself first with `docker build -t wiggle:local ..`), then **Load image → kind**.
3. **Servers tab →** deploy `srv1` (1 node). You get a Postgres and a wiggle node.
4. **Forwards tab →** deploy the **ops console** against `srv1` and open the link. It talks straight
   to the server (`WIGGLE_URL`).
5. **Client tests tab →** pick `srv1` and the `sleep` workflow → **Register workflow → Start
   instances → Observe**.
6. **Scale/kill/remove** the server from the Servers tab; **Tear down cluster** from the sidebar when
   done.

## Record & replay (reproduce an issue)

When something misbehaves, capture the exact sequence and hand it off for a fix:

1. **Sidebar → Recording → ● Start recording** (optionally note what you're testing).
2. Do your thing — every mutating action (create server, scale, kill, register, start instances, …) is captured with its arguments and outcome. A failing action is recorded as an error.
   For a *behavioural* issue (no crash — e.g. instances stuck), hit **📌 Snapshot into recording** in the
   Client-tests tab to bake the observed state in.
3. **■ Stop recording**, describe the issue, **⬇︎ Download recording JSON**, and send that file over.

Recordings also save to `~/.wiggle-lab/recordings/<id>.json`. To reproduce one:

```bash
python replay.py wiggle-lab-<id>.json
```

It re-runs the steps in order (waiting for each new server to be ready) and
**stops at the first step that errors — the reproduction point** — leaving the cluster up for
inspection (`kubectl get pods -n wiggle-lab`). `--no-wait` and `--settle N` tune the pacing. Replay is
what makes a bug report actionable: same sequence, same failure, then a fix.

## Notes & limits (v1)

- **No workers yet.** The built-in `sleep` and `instant` flows are advanced by the server itself
  (so they actually complete); the `park` flow holds a task in a queue and stays `RUNNING`.
  Deploying real workers (the `example` order workflow) is the natural next iteration.
- Host↔cluster gRPC goes over `kubectl port-forward` (managed automatically): each server on
  `127.0.0.1:1810x`.
- No database in this lab has a volume, so its state is ephemeral — a redeploy keeps it, but a node
  restart does not.

## Config (env vars)

| Var | Default | Meaning |
|-----|---------|---------|
| `WIGGLE_LAB_CLUSTER` | `wiggle-lab` | kind cluster name |
| `WIGGLE_LAB_NAMESPACE` | `wiggle-lab` | Kubernetes namespace for all lab resources |
| `WIGGLE_LAB_IMAGE` | `wiggle:local` | the wiggle server image to deploy |
| `WIGGLE_LAB_HOME` | `~/.wiggle-lab` | where server config and recordings are stored |

## Layout

```
wiggle-lab/
  app.py                 Streamlit UI (calls the controller)
  replay.py              reproduce a recording: python replay.py <file.json>
  gen_proto.sh           proto -> Python stubs
  setup.sh               venv + deps + stubs
  wigglelab/
    config.py            names, ports, labels
    shell.py             subprocess helpers
    kind.py              cluster lifecycle + image build/load
    manifests.py         server (+DB) and console Kubernetes manifests
    k8s.py               kubectl apply/scale/delete/list
    portforward.py       managed kubectl port-forwards
    cell_client.py       WiggleControlPlane gRPC (StartInstance, ListInstances, …)
    workflows.py         built-in workflow definitions (JSON-native)
    recorder.py          @record decorator + Recording (capture actions for replay)
    controller.py        orchestration brain (all state + logic; UI-agnostic)
    pb/                  generated gRPC stubs (git-ignored)
```
