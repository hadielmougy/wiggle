# Cell Coordinator — Phase 4 tickets

**Later: rare / advanced.** None of these are needed for a working multi-cell system; they add region locality, brownfield adoption, and straggler evacuation. Lowest priority — ship Phases 0–3 first.

Design ref: R24, §7 (adoption), R20. Independent of each other; sequence by need.

---

## T14 — Region as a placement dimension

**Files:** `CoordinatorStore` (region-tagged rings — `RingSlot.region`), `server/.../coord/CoordinatorApi.java` (`Resolve`/`ActiveCells` region filtering + address selection), SDK resolvers (pass `WIGGLE_REGION`), `IdCodec` (region derivable via the epoch's ring).

**Goal:** region-local workers drain region-local cells; addresses are region-appropriate; the authoritative cell never varies by region. (R24)

**Changes:**
1. Rings become region-tagged: `{ region → [cells] }`. Placement picks the region (from the start request's region or a region-bearing routing key), then shards within it.
2. `Resolve` returns the caller-region-appropriate `Endpoint.target` for the **same** cell; `ActiveCells(namespace, region)` returns only that region's cells. Store a per-cell `region → address` map (O(cells × regions), still not per-instance).
3. Workers pass `WIGGLE_REGION`; poll only region-local cells.

**Acceptance:** an instance placed in `eu-west` is served only by `eu-west` workers; a `us-east` client operating on it by id still reaches the one `eu-west` cell (cross-region address, same DB); no split brain.

**Guardrail:** resolution may vary address + poll-scope, never the authoritative cell for a given id.

**Depends on:** T9, T10, T12.

---

## T15 — Adopt a running cell (brownfield)

**Files:** `CoordinatorStore` (genesis-epoch policy), `CoordinatorApi`/resolver (legacy-id fallback route), `dist/coord` (adoption/rolling-restart tooling), runbook in `docs/`.

**Goal:** add the coordinator to a cell that has served standalone, moving no data and stopping no instance. (§7)

**Changes:**
1. **Capture as epoch 0:** write `policy[ns] = { currentEpoch: 0, epochs: {0: {ring:[existing-cell]}}, storage: <from the node's env config> }`. Metadata only — the cell's DB is untouched.
2. **Legacy-id fallback in `route`:** un-parseable (pre-adoption) ids resolve to the genesis epoch's single cell (`IdCodec` already flags legacy ids — T8). Constraint: **never reshard the genesis epoch** (legacy ids have no shard); add capacity only via new epochs.
3. **Rolling adoption:** seed the policy from the node's current config (guardrail on divergence); set `WIGGLE_COORDINATOR_URL` and rolling-restart nodes (each fetches matching config, registers); move clients onto the resolver (endpoint unchanged — soft cutover).

**Acceptance:** a standalone cell keeps serving throughout adoption; pre-adoption instances finish on the genesis cell; new instances get epoch-aware ids; nothing migrates. Self-heals: once epoch 0 drains, its cell can retire.

**Depends on:** T8, T9, T12.

---

## T16 — Live-migration saga (stragglers only)

**Files:** new `server/.../coord/MigrationSaga.java`, cell admin RPCs (quiesce / export / import / cutover / purge — extend `wiggle.proto` or an internal admin service), `CoordinatorReconciler` (drive + resume the saga).

**Goal:** evacuate a long-lived instance from one cell to another so an old epoch's cell can be reclaimed. Rare, complex — the exception, not the norm. (R20)

**Changes:**
1. Saga steps, each idempotent/resumable and CAS-fenced: `QUIESCE` (flip the instance's `wf_token`s out of READY on the source; `wf_instance.status=MIGRATING`) → `SNAPSHOT` (read `wf_instance` + `wf_token` rows; ensure the graph exists on the target — content-hash dedup) → `COPY` (insert on target, still held) → `CUTOVER` (flip instance→cell + bump epoch/`revision` — the single fence) → `ACTIVATE` (target tokens → READY) → `PURGE` (delete source rows).
2. Ordering guarantees exactly-once execution: quiesced on source before active on target; on coordinator crash the instance is *paused in both, active in neither* (resumable, never double-run). In-flight uncommitted steps re-run on the target via Wiggle's existing at-least-once lease recovery.

**Acceptance:** migrate a running instance across cells with no lost/duplicated steps; kill the coordinator mid-saga → instance is recoverable, never active in two cells; the source cell can then be retired.

**Risk:** highest-complexity item; the cutover fence is the critical correctness point. Only build when straggler cells actually block reclamation.

**Depends on:** T8, T9, T12.
