# Cell Coordinator — Phase 1 tickets

**Coordinator MVP: a role that mirrors one standalone cell.** After Phase 1 the coordinator exists as a `WIGGLE_ROLE=coordinator` deployment with its own DB and election, tracking one namespace / one epoch / one cell, and nodes register + heartbeat with it. No routing or scale-out yet (Phase 2/3).

Design ref: R8, R10, R14, R25, wire schemas §8. Sequencing: T4 → T5 → T6 → T7.

**Live-DB integration test:** `scripts/coordinator-integration.sh` (requires Docker) stands up a coordinator plus two 2-node cell clusters, each on its own Postgres database, and asserts node registration/roster, per-cluster leader election, role/schema isolation, and coordinator-driven failover expiry.

---

## T4 — `coordinator.proto` + generated stubs

**Files:** new `proto/src/main/proto/coordinator.proto` (package `dev.wiggle.proto`), proto module build (already generates Java stubs for `wiggle.proto`).

**Goal:** the `CellCoordinator` gRPC contract (design §8) as a buildable proto.

**Changes:**
1. Define `service CellCoordinator` with `FetchConfig`, `Register`, `Heartbeat`, `Deregister`, `Resolve`, `ActiveCells`, `OpenEpoch`, `SetRing`, `RegisterWorkflow` and all messages from §8 (`Endpoint`, `Tls`, `StorageSpec`, `Tuning`, `RingSlot`, `EpochRing`, `Policy`, `NodeConfig`, `Health`, …). Use `proto3`, `optional` for the sparse `Tuning` fields, `map<uint64,…>` for epochs/live-by-epoch, `oneof` for `ResolveRequest.by`.
2. Confirm the proto module compiles the new file into the same generated output the server already consumes.

**Acceptance:** `./gradlew :proto:build` generates `CellCoordinatorGrpc` + message classes; a trivial in-process server/stub round-trips one RPC in a unit test.

**Design decision:** separate `coordinator.proto` (recommended — distinct service, clean vendoring for the SDK later) vs appending to `wiggle.proto`.

**Depends on:** —

---

## T5 — `coord_*` persistence

**Files:** new `server/src/main/java/dev/wiggle/server/coord/CoordinatorStore.java` (interface), JDBC impl (in `jdbc` module, reusing `Dialect`/pool like `JdbcStorage`) + an in-memory impl for tests. Tables from T2's `COORDINATOR_MIGRATIONS`.

**Goal:** durable, CAS-guarded coordinator state — policy, node roster, definition registry.

**Changes:**
1. `CoordinatorStore` methods: `getPolicy(ns)`, `putPolicyCas(ns, expectedRevision, newPolicy)`, `upsertNode(...)`, `expireNodes(deadline)`, `getDefinition(ns,name)`, `putDefinition(...)`. Every mutating call is **compare-and-set on a revision/generation** (the `coord_policy` row carries `revision`; append epoch N+1 only if `current==N`).
2. JDBC impl over `coord_policy` / `coord_node` / `coord_definition`; reuse the connection pool + dialect from the jdbc module.

**Acceptance:** CAS put rejects a stale-revision write (returns false, no mutation); roster expiry works; unit tests on H2.

**Depends on:** T2 (tables).

---

## T6 — Coordinator bundle (service + reconcile loop)

**Files:** fill `CoordinatorBundle` (T1 stub); new `server/.../coord/CoordinatorApi.java` (the `CellCoordinator` gRPC service), `server/.../coord/CoordinatorReconciler.java`.

**Goal:** a running coordinator: serves the gRPC surface and runs leader-only reconciliation, safe under the tolerated brief-overlap election. (R8, R10, R14, R25)

**Changes:**
1. `CoordinatorBundle` wires `CoordinatorApi` (on `config.port()`) + `CoordinatorReconciler`, sharing the `storage`-backed `CoordinatorStore` and the `ClusterManager` from `WiggleServer`.
2. `CoordinatorReconciler` runs **only when `cluster.isLeader()`** (mirror `Housekeeper`'s leader gate); all its writes go through `CoordinatorStore` CAS. Reconcile duties for MVP: expire dead nodes, keep the policy consistent. Duties must be idempotent + re-entrant (same discipline as `Housekeeper`).
3. Start with a single namespace / single epoch / single cell (bootstrap policy = the one cell), so the coordinator returns "today's topology".

**Acceptance:** a `coordinator`-role server starts, elects a leader across 3 nodes, and survives leader failover (standby takes over reconcile) without double-applying (CAS proves it); a stale ex-leader's write is rejected.

**Depends on:** T1, T4, T5.

---

## T7 — Node ↔ coordinator lifecycle

**Files:** `server/.../coord/CoordinatorApi.java` (handlers), `dist/coord/HttpCoordinatorLink.java` + `CoordinatorConfigSource.java` (real impls, replacing T3 stubs), the cell node's heartbeat loop.

**Goal:** cells fetch config, register, and heartbeat; the coordinator tracks liveness; change propagates push-free. (R2, R3, R10)

**Changes:**
1. Coordinator side: implement `FetchConfig` (return `NodeConfig` = storage/tuning overlay + `generation` + `expected`), `Register` (record node, return `nodeId` + interval), `Heartbeat` (update roster, return `currentGeneration` + `directives`), `Deregister`.
2. Cell side: `CoordinatorConfigSource.load()` = env baseline **overlaid** with `FetchConfig` (cached to disk; on unreachable coordinator, fall back cached→local; **guardrail:** refuse if the coordinator's storage config diverges from local). `CoordinatorLink` registers after `start()` and heartbeats on a schedule; on a newer `generation` it re-fetches; on `DRAIN` it begins graceful stop. All best-effort — a coordinator outage never blocks boot.

**Acceptance:** a cell with `WIGGLE_COORDINATOR_URL` set registers and heartbeats; killing the coordinator does not stop the cell; bringing it back resyncs; a `generation` bump triggers a re-fetch; config divergence fails safe.

**Depends on:** T3, T6.
