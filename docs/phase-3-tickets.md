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

## T12 — Scale-out: epochs, drain, retire ✅ done

Built as three tested increments over an addressing prerequisite (cells had to become distinctly addressable before drain/retire could shift poll sets):

- **Increment 1 — cellId-addressable cells + ring-aware resolution.** `cell_id` added to `RegisteredNode`, `coord_node`, and `CoordNode`; `doResolve(instanceId)` parses the id's epoch+shard, looks up `ring[shard] → cellId`, and returns that cell's live nodes; `doActiveCells` returns one endpoint per cell across OPEN/DRAINING rings (RETIRED excluded). No ring ⇒ falls back to the whole roster (single implicit cell, R1). Tests: `MultiCellResolveTest`.
- **Increment 2 — coordinator-supplied placement.** `Register`/`FetchConfig` return the node's `(epoch, shards)` (the shards its cell owns in the current ring); the cell mints via a live, mutable `CellPlacement` (`ns.e{epoch}.s{shard}.ulid`), re-pointed when a heartbeat reports a new generation. `WIGGLE_CELL_ID` flows through the node link. Tests: `CoordinatorPlacementTest`, `CellPlacementTest`, `EpochAwareIdTest`.
- **Increment 3 — census-driven retire (R21).** Cells report `live_by_epoch` (from `WorkflowEngine.liveCountByEpoch()`) on `Heartbeat`; `LiveCensus` aggregates (max-within-cell, sum-across-cells, staleness-guarded); `CoordinatorReconciler` marks a DRAINING epoch RETIRED once the census confirms a fresh zero, bumping the generation so nodes re-fetch and workers drop it. Tests: `LiveCensusTest`, `EpochRetireTest`.

**Design note:** the epoch is **parsed from the instance id** rather than materialised as an `epoch` column on `wf_instance` — no schema migration on the hot table. The census scans the RUNNING set (capped at `LIVE_CENSUS_CAP`); a draining epoch only shrinks, so this stays cheap. Revisit the column only if a cell's live set makes the scan a bottleneck.

**Files:** `server/.../coord/CoordinatorApi.java` (`OpenEpoch`/`SetRing`/placement/census), `CoordNode`/`CoordinatorStore` (`cell_id`), `CoordinatorReconciler` + new `LiveCensus` (retire), new `CellPlacement` + `CellBundle` minter, `WorkflowEngine.liveCountByEpoch()`, `dist/.../coord/*` (cell_id + placement + health reporting), `proto/.../coordinator.proto` (`cell_id`, `NodeConfig`/`RegisterResponse` epoch+shards).

**Live proof:** `scripts/coordinator-integration.sh` bumps cell A's epoch, shows the minter shift to the new epoch, drains the old one, and watches the reconciler retire it — against real Postgres.

**Acceptance:** ✅ open a wider epoch → new instances spread across cells while existing ones finish in place; old cell drains to zero and is retired; workers stop polling it after the generation bump; no instance ever runs in two cells.

**Depends on:** T8, T9, T10, T11.

---

## T13 — Provisioning state machine + `CellDeployer` ✅ done

**Files:** `server/.../coord/CellDeployer.java` (interface + `Deployment` record) + `EmbeddedCellDeployer` (in-process: `StorageFactory.create` + `Storage.migrate` + `new WiggleServer(...)`) + `ProcessCellDeployer` (forks a JVM per node from a launch command); `NamespaceProvisioner.java` (state machine); `SecretResolver` (ref→secret at deploy time, `ENV` default); domain records `NamespaceSpec`, `StorageConfig`, `CoordNamespace`, enum `ProvisionState`; `CoordinatorStore` namespace registry (`getNamespace`/`namespaces`/`putNamespace`, `coord_namespace` table); `ServerConfig.withStorage`/`withPort` withers.

**Goal:** stand up a cell (DB + cluster) through a substrate-agnostic seam; no orchestrator assumed. (R22)

**Changes:**
1. `CellDeployer { migrateSchema(spec); deploy(spec) -> Deployment; teardown(id); }`. `EmbeddedCellDeployer` runs in-process (single box / tests / a pod entrypoint) and is the **only** place the coordinator side touches `StorageFactory`/`WiggleServer`; `ProcessCellDeployer` forks a JVM per node, configured via `WIGGLE_*` env. Both `deploy` idempotently resume an existing deployment. A k8s deployer is a later impl of the same seam.
2. `NamespaceProvisioner.create(spec)`: `REQUESTED → MIGRATING_SCHEMA` (migrate once, before serving) `→ STARTING` (deploy) `→ ACTIVE` (endpoint recorded). Persists every transition; already-ACTIVE is a no-op; any step that throws leaves a resumable `FAILED` record whose next `create` re-runs the remaining (idempotent) steps.
3. Storage config is captured into the `coord_namespace` record; the credential is a `secretRef` resolved at deploy time by a `SecretResolver` — the coordinator never stores a password.

**Design note:** provisioning is the machinery (deployer seam + state machine + registry); exposing it as a coordinator RPC / admin command is deferred (no `Provision` RPC yet). `EmbeddedCellDeployer` genuinely migrates a shared JDBC schema once before nodes start (idempotent with each node's own start-up migration); `ProcessCellDeployer` relies on the nodes' idempotent start migration (JDBC baseline guard), so its `migrateSchema` is a no-op.

**Acceptance:** ✅ `EmbeddedCellDeployerTest` provisions an in-memory namespace end-to-end (ACTIVE, endpoint serves real register/start with epoch-aware ids, teardown stops it); `NamespaceProvisionerTest` covers happy path, idempotency, and resumable `FAILED`→retry; `ProcessCellDeployerTest` covers fork/idempotent-redeploy/prompt-teardown and bad-command failure; `CoordinatorStoreTest` covers the JDBC namespace registry (secretRef stored, not the secret). `StorageFactory`/`WiggleServer` are touched only inside the deployer.

**Depends on:** T2, T5, T6.
