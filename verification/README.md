# wiggle-jepsen — Jepsen + Elle harness

A Jepsen test that drives the Wiggle workflow engine + cell coordinator under faults and checks
the recorded history with **Elle** (transactional register anomalies) plus two Wiggle-specific
checkers (single-cell routing, liveness).

> **Status: scaffold — compiles, not yet run against a live cluster.** `lein check` passes: all
> namespaces compile and every Jepsen/Elle require and Wiggle-class import resolves. The structure,
> Elle wiring, checkers, coordinator nemesis, and the client interop against the real
> `WiggleClient`/coordinator stubs are complete. The cluster lifecycle is *external* (Jepsen assumes
> the cluster is already running) and node kill/pause is a documented extension. It has not been
> executed end-to-end against a running cluster yet — treat it as a starting point.

## What it checks, and the honest scope

| Checker | Property | Notes |
|---|---|---|
| `elle` (`elle.rw-register`) | register consistency: lost/stale writes, cycles | Bites hardest at the **storage-tx** layer; at the engine API it mainly catches lost/stale writes **under faults** (partitions, killed leaders). A single instance is serialized by design, so with no faults it is trivially consistent. |
| `single-cell` | an instance is only ever served by one cell (T12) | Uses the `:cell` each op resolved to (coordinator mode). |
| `liveness` | after faults heal, every key is readable again | The cluster recovered. |

Elle is the transactional lens; it does **not** express routing or liveness — hence the two
custom checkers. For a deeper Elle workload, point an `elle.list-append` client at the **storage
`Tx` layer** (multi-key `inTx` transactions), which is where isolation (SKIP LOCKED, LWT,
`claimCompareAndSet`) actually lives — see *Upgrade paths*.

## The workload

A read/write register per workflow instance:

- `[:w k v]` → `signal(instance(k), "set", {"v": v})` — the payload merges into the context.
- `[:r k _]` → `instance(k).context()["v"]`.

One instance is pre-started per key at setup (keys are reused so transactions interfere). The
workflow is a chain of `awaitSignal("set")` nodes — see the `SEAM` note in `client.clj`.

## Prerequisites

1. **Publish the Wiggle client to your local Maven repo** so Leiningen can resolve it:
   ```
   cd .. && ./gradlew publishToMavenLocal        # installs io.github.hadielmougy:client:2.1.5
   ```
2. **A running cluster.** This scaffold does not start Wiggle. Bring one up first — the simplest is
   the existing integration script, which stands up a coordinator + two cell clusters on Postgres:
   ```
   ../scripts/coordinator-integration.sh          # or your own compose / cluster
   ```
   or run cells directly (direct mode): `WIGGLE_PORT=8081 .../wiggle`, etc.
3. **Leiningen** (`brew install leiningen`) and, for `:perf` plots, `gnuplot`.

## Run

Coordinator mode (routes via the coordinator, exercises T12 drain/retire + partitions):
```
lein run test \
  --coordinator 127.0.0.1:8099 \
  --node 127.0.0.1:8081 --node 127.0.0.1:8082 \
  --time-limit 120 --concurrency 20
```

Direct mode (single cluster, no coordinator — partitions only):
```
lein run test --node 127.0.0.1:8081 --node 127.0.0.1:8082 --time-limit 120
```

Results and Elle's anomaly graphs land in `store/`. `lein run serve` browses them.

> `--node` values are Wiggle gRPC `host:port` targets (the client dials them directly). In
> coordinator mode the nodes are only used as the direct fallback; routing goes through
> `--coordinator`.

## Faults (`nemesis.clj`)

- **coordinator-churn** — periodically calls `OpenEpoch`, forcing the current epoch to DRAIN and a
  new one to open. Under load this drives T12's drain/retire and the minter/poll-set shift. Fully
  implemented via the coordinator gRPC stub.
- **partition** — Jepsen's `partition-random-halves` (needs net control on the nodes; see below).

## Upgrade paths (the real work beyond the scaffold)

1. **Let Jepsen own the cluster.** Implement `db/DB` `setup!`/`teardown!` and a node kill/pause
   (`jepsen.nemesis/node-start-stopper`) against your substrate (usually `jepsen.control` over SSH
   to `--nodes`). That unlocks kill/pause faults and reproducible runs. Reuse the `kill -9 leader`
   and `bump→drain→retire` moves already in `../scripts/coordinator-integration.sh`.
2. **Storage-layer Elle (`list-append`).** Add a second client that runs multi-key `[:append k v]`/
   `[:r k]` transactions through `Storage.inTx(...)` (depend on `server` + a storage module). This
   is where Elle's cycle detection earns its keep against real isolation.
3. **More invariants.** Exactly-once dispatch (no task claimed twice) as a `checker/set`-style
   uniqueness check once the workload processes tasks with workers.

## Files

- `core.clj` — CLI, test map, generator, checker composition.
- `client.clj` — Jepsen client → `WiggleClient` (register workload; coordinator-aware routing).
- `nemesis.clj` — coordinator epoch churn + network partition.
- `checkers.clj` — Elle register + single-cell + liveness.
- `db.clj` — external-cluster lifecycle (no-op; the seam to make Jepsen own the cluster).
