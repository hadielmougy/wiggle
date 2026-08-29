# Cell Coordinator — Phase 3 tickets

**Definitions, scale-out, provisioning.** After Phase 3 a namespace can span multiple cells: definitions are propagated so any cell can start any workflow, new cells are opened via epochs and old ones drain and retire, and cells can be provisioned through a substrate-agnostic seam.

Design ref: R11, R12, R21, R22, R23. Sequencing: T11 → T12 → T13 (T11 and T13 can overlap).

---

## T11 — Definition fan-out (seed before serving)

**Files:** `server/.../coord/CoordinatorApi.java` (`RegisterWorkflow`), `CoordinatorReconciler` (seeding), `CoordinatorStore` (`coord_definition`), the cell register path (`GrpcApi` → `WorkflowEngine`/`DefinitionRegistry`), the SDK `register` call (Java/Go/Python).

**Goal:** every cell that can host a workflow already holds its definition; a joining cell is seeded before it's eligible. (R23)

**Changes:**
1. `RegisterWorkflow(namespace, name, definition)` on the coordinator: record `name → version` (content hash) in `coord_definition`, then **fan out** `register` to every cell in the namespace's active rings. Idempotent — content-hash versioning yields the same version on every cell, so replay is a no-op.
2. **Join invariant:** a new cell is seeded with the namespace's full current definition set **before** it enters the active ring (mirror R22's "migrate before serving"). Byte source: replay from `coord_definition` or copy `wf_graph_*`/`wf_definition` rows from a healthy sibling cell.
3. SDK `register(namespace, blueprint)` targets the coordinator when configured (fan-out); falls back to direct-to-cell when not.

**Acceptance:** register a workflow, open a second cell (T12) → `start` routed to the new cell succeeds immediately (definition present); re-register is a dedup no-op; a cell is never added to a ring before it holds the set.

**Design decision:** coordinator stores definition bytes (self-sufficient, seeds the first cell) vs stores only hashes + seeds from sibling cells (leaner coordinator). Recommended: hashes + sibling-copy, with the registering client as the origin for the first cell.

**Depends on:** T6 (coordinator), T9 (rings).

---

## T12 — Scale-out: epochs, drain, retire

**Files:** `server/.../coord/CoordinatorApi.java` (`OpenEpoch`/`SetRing`), `CoordinatorStore` (epoch history, CAS on `current_epoch`), `CoordinatorReconciler` (retire logic), cell heartbeat live-count reporting, possibly a migration adding an `epoch` column to `wf_instance`.

**Goal:** widen a namespace across cells without moving data; drain old epochs; retire empty cells. (R11, R12, R21)

**Changes:**
1. `OpenEpoch(namespace, ring)` / `SetRing(...)`: append a new epoch (CAS `current == N` → `N+1`), status `OPEN`; mark the prior epoch `DRAINING`. New roots use the current epoch; children inherit the parent's (T8).
2. Cells report `live_by_epoch` on `Heartbeat` (R21). Deriving the epoch per instance: parse it from the id, or add an `epoch` column to `wf_instance` (a new forward-only migration) and index it for a cheap `COUNT ... GROUP BY epoch`. **Recommend the column** for efficiency.
3. `CoordinatorReconciler`: when a draining epoch's cell reports `live == 0`, mark it `RETIRED` and drop it from the ring set (generation bump). Workers reconcile their poll set on the bump (T10).

**Acceptance:** open a wider epoch → new instances spread across cells while existing ones finish in place; old cell drains to zero and is retired; workers stop polling it after the generation bump; no instance ever runs in two cells.

**Depends on:** T8, T9, T10, T11.

---

## T13 — Provisioning state machine + `CellDeployer`

**Files:** new `server/.../coord/CellDeployer.java` (interface) + `EmbeddedCellDeployer` (in-process, uses `StorageFactory.create` + `Storage.migrate` + `new WiggleServer(...)`) + `ProcessCellDeployer` (fork a JVM), `server/.../coord/NamespaceProvisioner.java` (state machine), `CoordinatorStore` as the namespace registry.

**Goal:** stand up a cell (DB + cluster) through a substrate-agnostic seam; no orchestrator assumed. (R22)

**Changes:**
1. `CellDeployer { void migrateSchema(spec); Deployment deploy(spec); void teardown(id); }`. `EmbeddedCellDeployer` runs the calls in-process (single box / tests / a pod entrypoint); `ProcessCellDeployer` forks a JVM per cell. A k8s deployer is a later, separate impl of the same interface.
2. `NamespaceProvisioner.create(spec)`: `REQUESTED → MIGRATING_SCHEMA` (`create` + `migrate` once, before serving) `→ STARTING` (deploy cluster) `→ ACTIVE` (health ok, endpoint recorded). Idempotent; leaves `FAILED` on any step for retry.
3. Storage config is captured into the namespace record; secrets are `secretRef`s resolved at deploy time, never stored.

**Acceptance:** provision a namespace end-to-end with the embedded deployer (fresh DB migrated, cluster up, endpoint resolvable, ACTIVE); a mid-way failure leaves a resumable `FAILED` record; `StorageFactory`/`WiggleServer` are only touched inside the deployer.

**Depends on:** T2, T5, T6.
