# Coordinator module decomposition — plan

Extract the coordinator into a **nested module group** under a single `coordinator/` umbrella, decouple
its persistence + leadership from the engine's storage, and make the backing **pluggable** (reuse-the-DB
today; etcd or Raft later) — without changing behavior or the wire protocol.

## Goals

- All coordinator modules live under one `coordinator/` parent — the whole subsystem (and any future
  licensing boundary) is one directory.
- The coordinator **runtime** is a module, not code inside `server`.
- The coordinator **persistence contract** (`CoordinatorStore` + domain) is a tiny SPI the storage
  adapters implement — so a backend depends on a small contract, not on `server.coord`.
- Durability + leadership become one pluggable seam, so "Raft/etcd instead of a store" is a config/
  adapter choice, not a rewrite. The consensus dependency stays **out of the core engine and cell nodes**.

## Non-goals (now)

- Implementing the etcd/Raft backends (this plan only makes them cheap to add later).
- Changing `coordinator.proto` / `CellCoordinatorGrpc` (wire contract stays put and open).
- Any licensing/premium split (the boundary is set up; not exercised).

## As-is (the coupling to remove)

```
server/  dev.wiggle.server.coord.*   CoordinatorStore, CoordPolicy/CoordNode/CoordDefinition/
                                     CoordNamespace/StorageConfig/ProvisionState/NamespaceSpec,
                                     EpochCodec, InMemoryCoordinatorStore, LiveCensus,
                                     CoordinatorApi, CoordinatorReconciler, CellDeployer(+Embedded/Process),
                                     NamespaceProvisioner, SecretResolver
server/  dev.wiggle.server          CoordinatorBundle, ServerRole, WiggleServer (switch role -> bundle)
jdbc/                               JdbcCoordinatorStore      -> imports server.coord   (jdbc -> server)
cassandra/                         CassandraCoordinatorStore -> imports server.coord   (cassandra -> server)
```

Two smells: (1) storage adapters reach into `server.coord`; (2) `WiggleServer` hard-references
`CoordinatorBundle`, so the engine core compile-depends on the coordinator runtime.

## Target: a nested `coordinator/` group

Directory layout (Gradle subprojects nest by path):

```
coordinator/
  spi/        -> :coordinator:spi       artifact wiggle-coordinator-spi
  runtime/    -> :coordinator:runtime   artifact wiggle-coordinator          (the primary)
  etcd/       -> :coordinator:etcd      artifact wiggle-coordinator-etcd      (later)
  raft/       -> :coordinator:raft      artifact wiggle-coordinator-raft      (later)
```

Dependency graph (no cycles):

```
core
  └─ :coordinator:spi        CoordinatorStore, domain records, EpochCodec, InMemoryCoordinatorStore,
                             CoordinatorBackend (interface)                       deps: core

server (engine)             engine, cluster, gRPC control plane, Storage SPI, ServerBundle(+Factory)
                             deps: core, :coordinator:spi   (only for Storage.coordinatorStore() return)

jdbc      : JdbcCoordinatorStore        deps: server (implements Storage) + :coordinator:spi
cassandra : CassandraCoordinatorStore   deps: server (implements Storage) + :coordinator:spi

:coordinator:runtime        CoordinatorApi, CoordinatorReconciler, LiveCensus, CellDeployer(+impls),
                             NamespaceProvisioner, SecretResolver, CoordinatorBundle,
                             ReuseDbCoordinatorBackend
                             deps: :coordinator:spi, proto, server

:coordinator:etcd  (later)  EtcdCoordinatorStore + lease leadership     deps: :coordinator:spi (+ etcd client)
:coordinator:raft  (later)  RaftCoordinatorStore + raft leadership      deps: :coordinator:spi (+ Ratis)

client    : CellResolver, NamespaceWorker   (unchanged, stays open)
proto     : coordinator.proto                (unchanged)
dist (app)                  composes: picks CELL or COORDINATOR bundle; wires a coordinator backend
                             deps: server, :coordinator:runtime, storage modules, coordinator:etcd/raft (optional)
```

`:coordinator` itself is just a **container directory** (no build.gradle) — the path segment groups the
children. (Optional: make `:coordinator:runtime` re-export a sensible default so app authors add one dep.)

## Gradle mechanics

- **settings.gradle.kts**: `include(":coordinator:spi", ":coordinator:runtime")` (add `:etcd`/`:raft`
  later). The path maps to directory `coordinator/spi`, `coordinator/runtime`, … automatically.
- **Leaf names must not collide with existing top-level modules.** Use `spi`, `runtime`, `etcd`, `raft`
  (NOT `core` — there is already a top-level `:core`). Gradle project *name* is the leaf, so pick these
  deliberately.
- **`subprojects { }` in the root `build.gradle.kts` applies to nested projects too** — the Java
  toolchain, `-Xlint`, test config all still apply. No change needed there.
- **Publishing keys by project name today** (`publishedModules`, `moduleDescriptions`). Nested leaves
  (`spi`, `runtime`) don't fit the current `it.name` map, and the Maven artifactId would wrongly become
  `spi`/`runtime`. Fix: set the artifactId explicitly for the coordinator subprojects (via the
  publishing extension's `coordinates(...)`) → `wiggle-coordinator-spi`, `wiggle-coordinator`, etc., and
  key the publish config off `project.path` for the `:coordinator:*` set instead of `name`.

## What moves where

| From | To | Classes |
|---|---|---|
| `server/.../coord` | **:coordinator:spi** | `CoordinatorStore`, `CoordPolicy` (+nested), `CoordNode`, `CoordDefinition`, `CoordNamespace`, `StorageConfig`, `ProvisionState`, `NamespaceSpec`, `EpochCodec`, `InMemoryCoordinatorStore`, `CoordinatorBackend` (new) |
| `server/.../coord` | **:coordinator:runtime** | `CoordinatorApi`, `CoordinatorReconciler`, `LiveCensus`, `CellDeployer` + `EmbeddedCellDeployer` + `ProcessCellDeployer`, `NamespaceProvisioner`, `SecretResolver`, `ReuseDbCoordinatorBackend` (new) |
| `server/.../CoordinatorBundle` | **:coordinator:runtime** | `CoordinatorBundle` (built by a factory, not by `WiggleServer`) |
| `jdbc` / `cassandra` | stay put | `JdbcCoordinatorStore` / `CassandraCoordinatorStore` — repoint imports to `:coordinator:spi` |
| `client` | stays put | `CellResolver`, `NamespaceWorker` |

**Package naming:** move the SPI to `dev.wiggle.coordinator.spi` and the runtime to
`dev.wiggle.coordinator` (rename from `dev.wiggle.server.coord`). This touches imports in `jdbc`,
`cassandra`, `tests`, and the matrix driver — mechanical but wide. (Cheaper alternative: keep the
`dev.wiggle.server.coord` package name even though the files live under `coordinator/` — Java packages
are independent of Gradle modules — and rename packages in a later pass. Recommend the rename for
coherence, but it's optional.)

## The two inversions

### 1. `WiggleServer` stops knowing the coordinator bundle
Replace `switch(role){ COORDINATOR -> new CoordinatorBundle(...) }` with a `ServerBundle.Factory` seam:
`WiggleServer` builds the `CELL` bundle itself and accepts an optional factory for other roles; with
`COORDINATOR` and no factory it fails fast (*"add the wiggle-coordinator module"*). **`dist` supplies the
factory** — matching the existing "dist composes explicitly, no ServiceLoader" pattern. This flips the
`server -> coordinator` edge so `:coordinator:runtime` can depend on `server` without a cycle.

### 2. Durability + leadership → one pluggable backend
`CoordinatorBackend` (in `:coordinator:spi`):

```
interface CoordinatorBackend extends AutoCloseable {
    CoordinatorStore store();
    BooleanSupplier  isLeader();   // Raft/etcd give strong leadership; DB mode reuses cluster election
    default void start() {}
}
```

- **ReuseDbCoordinatorBackend** (default, in runtime): `store()` = `storage.coordinatorStore()`,
  `isLeader()` = `cluster::isLeader` — today's behavior, unchanged.
- **etcd / Raft backends** (later): both from the consensus group; no engine database.

`CoordinatorBundle` takes a `CoordinatorBackend`; the reconciler keeps its injected `isLeader`. The
backend is chosen by config in `dist`.

## Migration steps (each compiles + full suite green before the next)

1. **Create `coordinator/spi`.** Move the store interface + domain + `EpochCodec` +
   `InMemoryCoordinatorStore`; add `CoordinatorBackend`. Repoint imports (`server`, `jdbc`, `cassandra`,
   `tests`, matrix driver). Add `server -> :coordinator:spi`. Set artifactId `wiggle-coordinator-spi`.
   *Low-risk, mechanical.*
2. **Create `coordinator/runtime`.** Move the runtime + `CoordinatorBundle`. Add the `ServerBundle.Factory`
   seam; delete the coordinator branch from `WiggleServer`; wire the factory in `dist`. Move
   COORDINATOR-role `WiggleServer` tests to the factory/`dist` path. *The meat — the inversion.*
3. **Add `CoordinatorBackend` + `ReuseDbCoordinatorBackend`.** Refactor `CoordinatorBundle` to take a
   backend. Pure refactor, zero behavior change.
4. **(Later, on demand)** Add `coordinator/etcd` and/or `coordinator/raft`.

## Costs / risks

- **Test churn**, mostly import repointing + COORDINATOR-role `WiggleServer` construction going through
  the factory. The `tests` module already depends on everything.
- **The `WiggleServer` inversion** is the only non-mechanical change; keep it to one factory param.
- **Publishing tweaks** (artifactId + keying off `project.path`) are a one-time build change.
- Extracting `:coordinator:spi` removes the `server.coord` reach-in but does **not** remove `jdbc`/
  `cassandra` → `server` (they implement `Storage`, which lives in `server`); that would need a separate
  `storage-spi` extraction — out of scope here.

## Recommendation

Do steps **1–3 now** under the `coordinator/` umbrella — low-to-moderate risk, no behavior change, and a
clean `CoordinatorBackend` seam so a consensus backend is a later adapter, not a rewrite. Keep `proto` +
`CellResolver`/`NamespaceWorker` open. Defer `coordinator/etcd` + `coordinator/raft` until a concrete
DB-free-control-plane need appears.
