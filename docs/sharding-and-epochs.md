# Sharding & epochs — how instances are placed across cells

How the cell coordinator shards a namespace's instances across cells, routes them without a
directory, and reshards (grow / split / rebalance / shrink) with **zero data migration**. This is the
conceptual reference for the code in `core/IdCodec`, `server/CellPlacement`, and
`coordinator/**/CoordinatorService`.

> One-line model: **the instance id carries its own routing.** An instance's cell is a pure function
> of `id + placement policy`, so there is no per-instance directory and an instance never moves.

---

## 1. The three levels: node, cell, namespace

```
namespace  (e.g. "orders")
  └─ cell   (a group of nodes sharing ONE engine database)   ← unit of sharding & storage
       └─ node  (a single running process / JVM)             ← unit of liveness
```

| | **Node** | **Cell** |
|---|---|---|
| What it is | one process | group of nodes sharing a DB |
| Identity | UUID (`nodeId`) | `cellId` (name) + fingerprint (the DB) |
| Registers / heartbeats | **yes, individually** | no — its nodes do |
| Gets shards | no | **yes** (via the ring) |
| Holds instance data | no (it's in the DB) | yes (the shared DB) |

Consequences that surprise people:
- **Every node registers itself** (not a per-cell leader): liveness and endpoints are per-process, so
  the coordinator must know each node to route and fail over. Registration is per-node; *duties*
  (housekeeping, reaping) are per-cell via the intra-cell leader (`ClusterManager`).
- **A cell is defined by its database**, not by its name. Two node groups on two different databases
  are two cells even if they share a `cellId` — see §9 (the fingerprint guard).
- **A coordinated namespace is placed only by an explicit ring.** Every node sets its `cellId`
  (`WIGGLE_CELL_ID`); a node with none is rejected at register. A namespace becomes resolvable only once
  an `OpenEpoch` names its cells — before that, register succeeds but the node is on *standby* (mints
  nothing), and `Resolve` **fails closed** with `NamespaceNotReadyException` (gRPC `FAILED_PRECONDITION`).
  There is no implicit/inferred cell and no whole-roster fallback. To run without a coordinator at all,
  use `WiggleConnection.direct` — a single static target, legacy ids, no cells or sharding.

---

## 2. The self-routing id

`core/IdCodec` — `{namespace}.e{epoch}.s{shard}.{ulid}`:

```
orders.e2.s5.01H8XK9ABCDEF…
└─┬──┘ ┬  ┬  └──── ulid ────┘
 ns   epoch shard
```

- **namespace** — the tenant/workflow family (no `.`; it's the first segment).
- **epoch** — which *version* of the placement map applies to this instance (§4).
- **shard** — which logical slice of the key space this instance belongs to (§3).
- Legacy `wfi_…` ids don't parse and route to the genesis cell (pre-adoption compatibility).

Because epoch and shard are **baked into the id at birth and never recomputed**, resolution is a pure
lookup — no directory, and the id stays valid for the instance's whole life even as topology changes.

---

## 3. Shards: a fixed logical partitioning

A **shard** is a number that appears in a ring; the shard set for an epoch is exactly the shard
numbers its ring names. Shards are a **fixed carving of the key space you choose up front**, then
assign to cells — they are *not* created when nodes or cells join.

At mint time a cell spreads its new ids across the shards **it owns** (`server/CellPlacement.shardFor`
→ `IdCodec.shardFor`):

```java
// IdCodec: well-mixed hash of the ulid, reduced to the range (FNV-1a + murmur3 fmix64)
public static long shardFor(String ulid, int ringSize) {
    return ringSize <= 1 ? 0 : Math.floorMod(hash64(ulid), ringSize);
}
```

The fmix64 finalizer matters: `String.hashCode()` barely mixes and clusters on the shared timestamp
prefix of ULIDs minted close together; the finalizer spreads the random suffix evenly. The value is
mint-time only and baked into the id, so the hash can be changed freely without stranding any
existing instance (only future distribution changes).

### Why not just use cells as shards?

If `cell = shard` then routing is `hash mod cellCount`, and **adding a cell re-hashes everyone**
(`mod 3` → `mod 4` remaps almost every key → mass mis-route / migration). The shard layer decouples
the key space from the topology:

- `key → shard` is a **fixed** hash (baked into the id, permanent).
- `shard → cell` is a **small mutable table** (the ring).

You rebalance by moving *shards* between cells, never by re-hashing *keys*. Extra benefits of
`shards > cells`: uneven load balancing (give a bigger cell more shards), and **splitting** a cell
(hand half its shards to a new one) — impossible if a cell owns one indivisible slice.

**Cost:** pick the shard count up front (raising it later is a reshard) and note it **caps the
maximum number of cells** (a cell needs ≥1 shard). Choose generously — real systems use 256/1024,
the same trick as Elasticsearch shards / Cassandra vnodes / Dynamo virtual nodes.

---

## 4. Epochs: versioned placement

An **epoch** is a numbered generation of the ring, with a lifecycle:

```
OPEN  ──►  DRAINING  ──►  RETIRED
```

- `currentEpoch` — the epoch new instances mint into.
- Old epochs keep their old ring, so their still-running instances keep resolving correctly.

### Why epochs exist

Without them, changing the shard→cell map would mean **migrating live workflows between databases**
(their id is fixed to a shard; move the shard's owner and the instance's data must move too — a racy,
stateful migration of in-flight tokens and timers).

The epoch turns that stateful migration into a **stateless drain**: publish the new layout as a *new*
epoch, let the old one drain, then retire it. Old instances keep using the map they were born under;
new instances use the new one; **nothing moves.**

This is wiggle's answer to consistent hashing: a hash ring works to *minimize* key remaps on topology
change; wiggle **never remaps a key**, because the key remembers which map (epoch) it was born under.

---

## 5. What changes when — epoch vs generation vs roster

| Trigger | epoch number | policy revision (generation) | roster |
|---|---|---|---|
| A **node** joins/leaves | — | — | ✔ |
| A **cell**'s nodes join (new cellId) | — | — | ✔ |
| Operator **`OpenEpoch`** (reshard) | ✔ | ✔ | — |
| Reconciler **retires** a drained epoch | — | ✔ | — |

- Adding nodes or cells does **not** bump the epoch — that's just the roster.
- `OpenEpoch` is the **only** thing that increments the epoch, and it's the **only** way to change a
  ring at all — a published epoch's ring is sealed (§6).
- The **generation** (policy `revision`) is what nodes watch on heartbeat to re-fetch placement.

Every policy write is a **compare-and-set on `revision`** (`casPolicy`), so a stale ex-leader's write
matches zero rows and loses — the tolerated brief-overlap election stays safe.

---

## 6. When to open a new epoch

> **Open a new epoch whenever you change which cell owns a shard that already holds live instances.**
> If the change would relocate existing work, it needs a new epoch. If it wouldn't, it doesn't.

Open a new epoch (reshard) for: **cell split**, **adding a cell to share load**, **draining/removing
a cell**, **rebalancing**, **cross-region moves**.

Do **not** open one for: **scaling a cell's replicas** (adding nodes), **node failure/recovery**, or
anything that leaves the shard→cell map untouched (needless epochs add draining generations every
worker must keep polling).

Prerequisites: provision the target cell **first** (its nodes must be registered, or resolves to it
fail), and prefer letting the previous epoch drain before opening the next (draining epochs stack and
multiply poll targets).

`OpenEpoch` is the **only** way to change a ring: a published epoch's `shard → cell` slots are
**sealed** (immutable), so every reshard bumps the epoch. There is no in-place ring edit — the
`SetRing` RPC was removed — which is what makes the §7 mis-route structurally unreachable. See
[ring-immutability-guard.md](ring-immutability-guard.md).

---

## 7. Adding and removing shards

Both add and remove are **reshards that open a new epoch** — because the ring is sealed (§6), there is
no in-place path. The old epoch keeps its ring (its instances keep resolving) and drains; the new epoch
carries the new shard set.

**Adding a shard** — open a new epoch whose ring includes it. Existing ids keep resolving via their own
epoch's ring; the added shard's cell is handed it for new mints in the new epoch.
Test: `MultiCellResolveTest.addShardViaNewEpochIsSafe`.

**Removing a shard** — open a new epoch that omits it. The old epoch retains the shard (its live
instances keep resolving there), then drains and retires. You drain **epochs, not shards** — there is
no in-place "drain one shard."
Test: `MultiCellResolveTest.removeShardViaNewEpochIsSafe`.

**Why in-place is sealed off.** If a shard could be removed from a live epoch's ring, `cellFor` would
fall through its modulo-wrap for the now-unknown shard and silently route live instances to the wrong
cell:

```java
for (RingSlot s : ring) if (s.shard() == shard) return s.cellId();
return ring.get(Math.floorMod(shard, ring.size())).cellId();   // <-- would wrap to the WRONG cell
```

Since there is no in-place ring edit at all (the `SetRing` RPC was removed), that fall-through can never
be reached for a live epoch's shard — the mis-route is structurally impossible rather than guarded by
operator discipline.
Tests: `MultiCellResolveTest.addShardViaNewEpochIsSafe`, `…removeShardViaNewEpochIsSafe`.

---

## 8. Concurrency & atomicity

"What if an instance is minted at the exact moment the ring changes?" is answered at three layers —
none of which is a distributed lock on the mint path:

1. **Epoch (design level).** A node minting with a stale placement emits an *old-epoch* id, and old
   epochs keep their immutable rings until drained — so it resolves via the map it was born under.
   Concurrent mints are safe *because* reshards go into new epochs, not over old ones.
2. **Coordinator (ring write).** `casPolicy` is a single compare-and-set on `revision`; readers see
   the whole old ring or the whole new ring, never a torn one.
3. **Node (per-mint).** `CellPlacement` holds `(epoch, shards)` as one immutable snapshot behind a
   single `volatile` reference, and a mint takes it atomically via `stampFor(ulid)`. Reading `epoch()`
   and `shardFor()` separately could interleave a re-point between them and stamp one generation's
   epoch with another's shard (→ mis-route after a reshard); `stampFor` closes that window.
   Test: `CellPlacementTest.stampForIsAtomicUnderReconfig`.

---

## 9. Cell identity: the fingerprint guard

`cellId` is operator-chosen free text, so the coordinator cannot tell two situations apart from it
alone: **replicas of one cell** (share a `cellId`, must be allowed) vs **two different cells reusing a
`cellId`** (must be rejected — they'd be silently conflated into one logical cell over two disjoint
databases, and instances would resolve to nodes that don't hold them).

The discriminator is the **fingerprint**: a stable identity of the cell's shared storage
(`Storage.fingerprint()` — hash of the JDBC URL, or the Cassandra keyspace; `null` for in-memory,
which can't be shared across processes). It travels node → coordinator on register
(`RegisteredNode.cell_fingerprint`) and is persisted per node (`CoordNode.cellFingerprint`).

`doRegister` enforces it via `CoordinatorStore.bindCell(namespace, cellId, fingerprint)` — an **atomic
single-key claim** on a `(namespace, cellId) → fingerprint` binding: same `cellId` + **same** fingerprint
→ replica → allow; same `cellId` + **different** fingerprint → two cells → reject; null fingerprint →
exempt. It is a single-key operation on every backend (JDBC unique PK / Cassandra single-partition
`IF NOT EXISTS` LWT / etcd version-fenced txn / in-memory `computeIfAbsent`), so it is race-free — unlike
the earlier check-then-insert over the node roster, which could let two different fingerprints both pass
under a concurrent register, and which could not be made atomic on Cassandra at all.

The binding is reclaimed when a cell fully drains: the leader's reconcile loop calls
`pruneOrphanCellBindings`, deleting any binding no live node references, so a decommissioned cell id
becomes reusable (a bounded delay after the last node leaves; the safe direction — it only ever
over-rejects, never mis-routes).
Tests: `CoordinatorStoreTest.cellBindingScenario` / `bindCellConcurrent` (InMemory + JDBC/H2),
`CoordinatorPlacementTest.rejectsDuplicateCellId`, `…noFingerprintSkipsGuard`.

---

## 10. Worked example: a cell split

Take `orders` from one cell (`cellA`, owning all shards) to two (`cellA` + new `cellB`):

1. **Warm the new cell.** cellB's nodes register; the coordinator seeds every workflow definition onto
   cellB *before* it enters the roster (so it's never resolvable while missing a graph).
2. **Cut over.** `OpenEpoch` with the split ring → epoch N `DRAINING` (ring retained), epoch N+1 `OPEN`
   with `s0,s1→cellA, s2,s3→cellB`; `revision` bumps (CAS-guarded).
3. **Re-point nodes live.** On the next heartbeat each node sees the new generation, re-fetches, and
   swaps its `CellPlacement` — no restart. cellA now mints `[0,1]`, cellB `[2,3]`.
4. **Route old + new.** `orders.eN.s2.…` (born before) still resolves to cellA via epoch N's ring;
   `orders.e{N+1}.s2.…` resolves to cellB. The id's epoch selects the ring.
5. **Keep draining.** `activeCells` returns every cell in any non-retired epoch, so workers poll both
   until epoch N's work finishes.
6. **Retire.** When nodes report zero live instances in epoch N (fresh census), the reconciler flips it
   `RETIRED` and bumps the generation; workers drop it.

No id is ever rewritten and no instance data is moved.

---

## 11. Code map

| Concept | Where |
|---|---|
| id format + `shardFor` hash | `core/src/main/java/com/wiggle/core/IdCodec.java` |
| ring & epoch model (`RingSlot`, `EpochRing`, status) | `coordinator/spi/**/CoordPolicy.java` |
| node's live placement + atomic `stampFor` | `server/src/main/java/com/wiggle/server/CellPlacement.java` |
| minting (uses `stampFor`) | `server/src/main/java/com/wiggle/server/CellBundle.java` |
| register / resolve / openEpoch / fingerprint guard | `coordinator/runtime/**/CoordinatorService.java` (gRPC adapter: `CoordinatorApi.java`) |
| drain → retire lifecycle (census-driven) | `coordinator/runtime/**/CoordinatorReconciler.java` |
| ring persisted as JSON (backend-independent) | `coordinator/spi/**/EpochCodec.java` |
| client-side resolution & caching of the above | `client/**/CoordinatedConnection.java` — see [client-caching-contract.md](client-caching-contract.md) |
| storage fingerprint | `server/**/store/Storage.java`, `jdbc/**/JdbcStorage.java`, `cassandra/**/CassandraStorage.java` |
| node ⇄ coordinator link (applies placement) | `dist/**/coord/HttpCoordinatorLink.java` |
