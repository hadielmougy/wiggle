# Wiggle Lab

A local control panel for **playing with a real wiggle cell cluster** on Kubernetes (via
[kind](https://kind.sigs.k8s.io/)) — deploy cells, reshard partitions/epochs, scale, kill, and run
client scenarios, all from a Streamlit UI. Built to make manual, multi-scenario testing fast instead
of tedious.

It talks to the cluster two ways:
- **kind / kubectl** (subprocess) for infrastructure: cluster, coordinator, cells (each with its own
  Postgres), scaling, killing pods.
- **gRPC** (Python stubs generated from `proto/`) for control + data plane: open epochs, allocate
  workflows, start instances, observe state.

## What you can do

- Create/tear down a kind cluster.
- Deploy the **coordinator** (embedded Ratis + RocksDB store — no external DB).
- Create **cells**, each getting its **own Postgres container** and N wiggle nodes, wired to the
  coordinator and namespace.
- **Reshard**: open a placement epoch with a `shard→cell` ring; opening a new epoch drains the old one
  (the coordinator retires it automatically once instances finish).
- **Scale** a cell up/down, **kill** individual pods, **remove** a whole cell (and its DB).
- Run **client scenarios**: allocate a built-in workflow to a namespace, start instances, and watch
  where they land across cells/epochs.

## Prerequisites

`docker`, `kind`, `kubectl`, and `python3` on your PATH. (The sidebar shows which are present.)

## Setup

```bash
cd wiggle-lab
./setup.sh                     # venv + deps + generate gRPC stubs from ../proto
source .venv/bin/activate
streamlit run app.py           # opens http://localhost:8501
```

`setup.sh` runs `gen_proto.sh`, which compiles `proto/src/main/proto/{wiggle,coordinator}.proto` into
`wigglelab/pb/`. Re-run `./gen_proto.sh` if the protos change.

## Typical flow (in the UI)

1. **Sidebar → Create kind cluster.**
2. **Build image** (compiles the Java dist + dashboard — several minutes; or build `wiggle:local`
   yourself first with `docker build -t wiggle:local ..`), then **Load image → kind**.
3. **Deploy coordinator.**
4. **Cells tab →** create `cellA` in namespace `orders` (1 node). Repeat for `cellB` if you like.
5. **Placement tab →** open an epoch for `orders` with ring `0=cellA`.
6. **Client tests tab →** pick the `sleep` workflow → **Allocate → Start instances → Observe**.
   Watch instances run and complete on `cellA`.
7. **Reshard:** create `cellB`, then open a new epoch `0=cellB`. New instances now land on `cellB`;
   `cellA`'s epoch goes **DRAINING** and retires once its instances finish.
8. **Scale/kill/remove** cells from the Cells tab; **Tear down cluster** from the sidebar when done.

## Record & replay (reproduce an issue)

When something misbehaves, capture the exact sequence and hand it off for a fix:

1. **Sidebar → Recording → ● Start recording** (optionally note what you're testing).
2. Do your thing — every mutating action (create cell, scale, kill, open epoch, allocate, start
   instances, …) is captured with its arguments and outcome. A failing action is recorded as an error.
   For a *behavioural* issue (no crash — e.g. instances stuck), hit **📌 Snapshot into recording** in the
   Client-tests tab to bake the observed state in.
3. **■ Stop recording**, describe the issue, **⬇︎ Download recording JSON**, and send that file over.

Recordings also save to `~/.wiggle-lab/recordings/<id>.json`. To reproduce one:

```bash
python replay.py wiggle-lab-<id>.json
```

It re-runs the steps in order (waiting for the coordinator/cells to be ready between infra steps) and
**stops at the first step that errors — the reproduction point** — leaving the cluster up for
inspection (`kubectl get pods -n wiggle-lab`). `--no-wait` and `--settle N` tune the pacing. Replay is
what makes a bug report actionable: same sequence, same failure, then a fix.

## Notes & limits (v1)

- **No workers yet.** The built-in `sleep` and `instant` flows are advanced by the server itself
  (so they actually complete); the `park` flow holds a task in a queue and stays `RUNNING`, which is
  the clearest way to *see* how work distributes across cells and epochs. Deploying real workers
  (the `example` order workflow) is the natural next iteration.
- Host↔cluster gRPC goes over `kubectl port-forward` (managed automatically): coordinator on
  `127.0.0.1:18099`, each cell on `127.0.0.1:1810x`.
- The coordinator runs a **single-member** Ratis group on an `emptyDir` — ephemeral, fine for a lab.
- Placement policy is cached from `OpenEpoch` responses (the coordinator has no read-policy RPC) and
  persisted to `~/.wiggle-lab/state.json`, so "start into namespace" needs an epoch opened first.

## Config (env vars)

| Var | Default | Meaning |
|-----|---------|---------|
| `WIGGLE_LAB_CLUSTER` | `wiggle-lab` | kind cluster name |
| `WIGGLE_LAB_NAMESPACE` | `wiggle-lab` | Kubernetes namespace for all lab resources |
| `WIGGLE_LAB_IMAGE` | `wiggle:local` | the wiggle server image to deploy |
| `WIGGLE_LAB_HOME` | `~/.wiggle-lab` | where the policy cache is stored |

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
    manifests.py         coordinator / cell(+DB) Kubernetes manifests
    k8s.py               kubectl apply/scale/delete/list
    portforward.py       managed kubectl port-forwards
    coord_client.py      CellCoordinator gRPC (OpenEpoch, RegisterWorkflow, Resolve, …)
    cell_client.py       WiggleControlPlane gRPC (StartInstance, ListInstances, …)
    workflows.py         built-in workflow definitions (JSON-native)
    recorder.py          @record decorator + Recording (capture actions for replay)
    controller.py        orchestration brain (all state + logic; UI-agnostic)
    pb/                  generated gRPC stubs (git-ignored)
```
