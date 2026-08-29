# Cell Coordinator — Phase 2 tickets

**Routing and the polyglot SDK.** After Phase 2, instances carry epoch-aware ids, the coordinator resolves them to cells, and the Java/Go/Python clients dial the right cell transparently. This is the first *observable* capability and the biggest client-side lift.

Design ref: R9, R13, R16, R17, R18, R19, §6 (id & endpoint formats). Sequencing: T8 → T9 → T10.

---

## T8 — Epoch-aware instance id

**Files:** `server/src/main/java/dev/wiggle/server/engine/WorkflowEngine.java` (id mint at line 74 `inst.id = Ids.next("wfi")`; `start(...)` at line 51), the `Ids` helper, new `core/.../IdCodec.java` (parse/format shared with the Java client).

**Goal:** mint `ns.e{epoch}.s{shard}.{ulid}` at start and provide directory-free `route(id)`. (R13, R16, R17)

**Changes:**
1. Replace `Ids.next("wfi")` with an epoch-aware mint. The cell knows its `namespace` + current `epoch` (from `ServerConfig` / coordinator config) and the ring size. Default hash key = the id's random component: `ulid = ulid()`, `shard = hash(ulid) mod ringSize`, `id = "{ns}.e{epoch}.s{shard}.{ulid}"`. (Standalone/no-coordinator: `ns` = the workflow's namespace, `epoch = 0`, `ringSize = 1` → `shard = 0`, so ids stay well-formed even without a coordinator.)
2. `IdCodec.parse(id) -> (ns, epoch, shard, ulid)` and `format(...)`, tolerant of **legacy ids** (un-parseable → caller uses the genesis-epoch fallback, see §7).
3. Keep `route(id)` a pure function of `IdCodec.parse` + the policy ring (no per-instance lookup).

**Acceptance:** new instances have parseable ids; `route()` returns the right cell from id + policy; a legacy `wfi_*` id parses to "legacy" and is not rejected; standalone mode still starts instances with valid ids.

**Design decision:** shard chosen cell-side at start (recommended — the cell owns `ns`/`epoch`) vs client-side in `placeNew`. Either way the shard is embedded in the id so routing stays directory-free.

**Depends on:** Phase 1 (coordinator config supplies `ns`/`epoch`/ring; degrade gracefully without it).

---

## T9 — `Resolve` / `ActiveCells` on the coordinator

**Files:** `server/.../coord/CoordinatorApi.java` (add the two RPCs), `CoordinatorStore` (policy/ring reads), `core` Endpoint builder.

**Goal:** turn a namespace or an instance id into a cell `Endpoint`, region-aware and cached. (R9, R18, R19, §6)

**Changes:**
1. `Resolve(ResolveRequest)`: `by.instance_id` → parse epoch/shard → ring[shard]; `by.namespace` → current epoch's ring (for a new start). Return the `Endpoint` record (gRPC target + TLS + region + TTL), choosing the address for `caller_region` (never a different cell — R24 groundwork).
2. `ActiveCells(namespace, caller_region)`: the set of cells hosting live work (MVP: just the current epoch's ring), plus a `generation` so callers re-fetch on change.
3. Build `Endpoint.target` from the cell's stable client-facing address (recorded at provisioning/register), **not** a node's self-reported pod IP.

**Acceptance:** `Resolve` by id and by namespace both return the correct cell; `ActiveCells` returns the current ring with a generation; responses carry a TTL.

**Depends on:** T5, T6, T8.

---

## T10 — SDK resolver (Java · Go · Python)

Split per client, since each is its own codebase with its own tests:

- **T10a — Java** (`client/.../CellResolver.java`): ✅ done. The reference implementation + `CellRoutingTest` (real coordinator + cell).
- **T10b — Go** (`wiggle-go`: vendor `coordinator.proto`, new `resolver.go` + id parse): mirrors T10a.
- **T10c — Python** (`wiggle-python`: vendor `coordinator.proto`, new resolver + id parse): mirrors T10a.

**Files:** `client/src/main/java/dev/wiggle/client/WiggleClient.java` (+ a resolver), `wiggle-go` (`client.go`/new `resolver.go`), `wiggle-python` (`client.py`/new resolver). Each needs the coordinator stub (vendor `coordinator.proto` into the Go/Python repos as they already vendor `wiggle.proto`).

**Goal:** clients dial the right cell without the app knowing cells exist. (R9, R18, R19)

**Changes (same shape in all three):**
1. A `Resolver` that, given `WIGGLE_COORDINATOR_URL`, calls `Resolve`/`ActiveCells`, **caches** by TTL, and invalidates on `UNAVAILABLE`/`NOT_FOUND` from a cell (endpoint moved).
2. `start(namespace, ctx)` → `Resolve(by=namespace)` → dial that cell → return the id. Operating by id (`instance`, `signal`, `cancel`, `awaitCompletion`) → `IdCodec.parse` → `Resolve(by=instance_id)` (or straight from the cached ring) → dial.
3. Worker: resolve `ActiveCells(namespace, region)` → long-poll **every** returned cell; re-fetch and reconcile the poll set when the `generation` changes (usually 1 cell, briefly 2 during scale-out).
4. **Fallback:** no coordinator configured → behave exactly as today (dial the static endpoint). (R1)

**Acceptance:** a cross-language run (as in the existing polyglot demo) works with the coordinator in front: clients resolve, start, and operate by id; workers poll the resolved cell(s); with no coordinator URL, everything falls back to direct dialing unchanged.

**Risk:** three codebases; keep the resolver logic identical and the no-coordinator path a true no-op. Biggest single ticket — budget accordingly.

**Depends on:** T4 (proto), T8, T9.
