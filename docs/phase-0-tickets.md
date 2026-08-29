# Cell Coordinator — Phase 0 tickets

**Non-breaking seams. Every task defaults to today's behaviour** (`WIGGLE_ROLE=cell`, `WIGGLE_COORDINATOR_URL` unset ⇒ byte-for-byte identical to the current server). These three land the seams the rest of the work hangs off; nothing here changes what a standalone deployment does.

See the design reference for the full picture (requirements R1–R25, wire schemas §8, deployment topology §9, roadmap §10). Requirement tags below (`R#`) point back to it.

Sequencing: **T1 first** (keystone). Then T2 and T3 can go in parallel. One PR per ticket.

---

## T1 — Role switch + subsystem composition  *(keystone)*

**Files:** `server/src/main/java/dev/wiggle/server/WiggleServer.java`, `server/src/main/java/dev/wiggle/server/ServerConfig.java`, new `server/src/main/java/dev/wiggle/server/ServerRole.java`, new package-private `CellBundle` / `CoordinatorBundle`.

**Goal:** make it possible for the same binary to start as a `cell` or a `coordinator`, without changing the `cell` path. (R25, R1)

**Changes:**
1. Add `enum ServerRole { CELL, COORDINATOR }`.
2. Add `role` to `ServerConfig` (canonical constructor), **defaulting to `CELL`** in the convenience constructors (~lines 55–90) so existing callers/tests compile unchanged. `fromEnvironment()` reads `WIGGLE_ROLE` (default `cell`).
3. Extract a package-private `ServerBundle { void start(); void close(); }`. Move the engine / housekeeper / queue-lag / `api` (`GrpcApi`) / dashboard wiring (currently `WiggleServer` ctor lines 61–76, `start()` 88–92, `close()` 112–115) into `CellBundle`, verbatim. Add a `CoordinatorBundle` **stub** (empty start/close for now — filled in Phase 1/T6).
4. `WiggleServer` keeps only the shared `storage` + `cluster`, selects the bundle by `config.role()`, and delegates `start()`/`close()`. `cluster()` stays; `engine()` is valid only for `CELL` (throw `IllegalStateException("no engine in coordinator role")` otherwise — tests only call it on cell).

**Design decision:** role on `ServerConfig` (one source of truth; `migrate()` in T2 and `Main` in T3 both need it) vs a 4th `WiggleServer` ctor arg. **Recommended: `ServerConfig`.**

**Acceptance:**
- `WIGGLE_ROLE` unset/`cell`: construction, `start()`, `close()`, and all accessors behave identically to today; full `server` + `tests` suites pass with no diffs.
- `WIGGLE_ROLE=coordinator`: constructs with storage + cluster only; no housekeeper/queue-lag thread; `WiggleControlPlane` not served. New test asserts `PollTasks` is unavailable and no `Housekeeper` runs.

**Risk:** record-constructor fan-out; `engine()` nullability. Keep the 1-/2-/3-arg constructors working.

**Depends on:** —

---

## T2 — Base + role schema layering

**Files:** `jdbc/src/main/java/dev/wiggle/jdbc/JdbcStorage.java` (`MIGRATIONS` 71–186, `migrate()` 188–199, `runMigrations(...)` 209–235), `server/src/main/java/dev/wiggle/server/store/Storage.java` (`migrate()` line 13), `server/.../store/InMemoryStorage.java`.

**Goal:** a coordinator DB gets its own tables; a cell DB is untouched. (R22, R25)

**Constraint:** the migration history is **forward-only — never edit or reorder a released migration** (JdbcStorage 63–69). V1 already fuses membership (`wf_node`) and engine (`wf_*`) tables, so it cannot be retroactively split.

**Changes:**
1. Leave `MIGRATIONS` unchanged — it is the **CELL** set (existing cell DBs stay valid).
2. Add `COORDINATOR_MIGRATIONS`: a fresh V1 baseline creating `wf_node` (copy the membership DDL from lines 143–150, `IF NOT EXISTS`, because `ClusterManager` needs it on the coordinator DB) + `coord_policy`, `coord_node`, `coord_definition`. `wf_schema_version` is auto-created by `runMigrations` (it's already generic + `public static` — reuse it).
3. Thread role into migration: `Storage.migrate()` → `migrate(ServerRole role)` (update `JdbcStorage`, `InMemoryStorage` — latter stays a no-op). `WiggleServer` line 60 becomes `storage.migrate(config.role())`. Select `role == COORDINATOR ? COORDINATOR_MIGRATIONS : MIGRATIONS`.
4. **Footgun guard:** before applying `COORDINATOR_MIGRATIONS`, probe for a cell table (`wf_token`) and fail fast if present (and vice versa) — both lineages start at V1 but must live in **separate databases** (R25). Error: *"this database already holds cell tables; a coordinator needs its own DB."*

**Acceptance:**
- Cell DB migrates exactly V1–V6 as today (schema diff = none); storage integration tests pass.
- Fresh coordinator DB gets `wf_node` + `coord_*` + its own `wf_schema_version`.
- Pointing a coordinator at a cell DB (or vice versa) fails fast with the guard message.

**Risk:** shared-DB version-lineage collision → mitigated by the guard + the own-DB rule.

**Depends on:** T1 (`ServerRole`, `config.role()`).

---

## T3 — Dormant client hooks in the distribution

**Files:** `dist/src/main/java/dev/wiggle/dist/Main.java` (lines 17–18, 28), new package `dist/src/main/java/dev/wiggle/dist/coord/`: `ConfigSource`, `EnvConfigSource`, `CoordinatorConfigSource` (stub), `CoordinatorLink`, `NoopCoordinatorLink`, `HttpCoordinatorLink` (stub).

**Goal:** the coordinator client seam exists but is inert unless configured. (R1–R4)

**Changes:**
1. `interface ConfigSource { ServerConfig load(); }`; `EnvConfigSource.load()` = `ServerConfig.fromEnvironment()`.
2. `interface CoordinatorLink extends AutoCloseable { void register(...); void heartbeat(); void close(); }`; `NoopCoordinatorLink` does nothing.
3. In `Main`: read `WIGGLE_COORDINATOR_URL`; `coordinated = url != null && !url.isBlank()`. Choose `configSource = coordinated ? new CoordinatorConfigSource(...) : new EnvConfigSource()` and `link = coordinated ? new HttpCoordinatorLink(url) : new NoopCoordinatorLink()`. **Phase 0 scope: the coordinated impls are stubs** (`CoordinatorConfigSource` returns env config for now; `HttpCoordinatorLink` logs "would register"). Real overlay/register is T7.
4. Build the server from `configSource.load()`, keep the exact banner/`baseUrl()` prints (20–27), call `link.register(...)` after `start()`, and make the shutdown hook `link.close()` then `server.close()` (replaces line 28).

**Acceptance:**
- `WIGGLE_COORDINATOR_URL` unset: `Main` output and behaviour byte-identical to today; zero coordinator calls.
- Set: the seam runs (stub logs a register attempt); server still starts normally.
- No change to `WiggleServer` beyond T1; `dist/coord` is purely additive.

**Risk:** keep the unset path a true no-op; a coordinator-stub failure must never break standalone boot (best-effort, R3).

**Depends on:** T1 (`WIGGLE_ROLE` in `fromEnvironment`).
