# Client caching contract — how clients cache coordinator resolutions

What a wiggle client is allowed (and required) to cache from the coordinator, and why each of the three
resolution surfaces caches differently. This is the conceptual reference for `client/WiggleConnection` and a
spec every client implementation must honour (the Java client and the vendored-proto Python client
alike). For the placement model these resolutions read, see [sharding-and-epochs.md](sharding-and-epochs.md).

> One-line model: **the coordinator is control-plane only.** A client resolves *where* a cell is, then
> talks to that cell directly — so resolutions are cached aggressively where the answer is stable and
> not at all where re-resolving is itself the mechanism. The coordinator is never on the data path.

---

## 1. The three resolution surfaces

There is no single "resolve and cache" rule — the three calls have deliberately different policies.

| Surface | `WiggleConnection` entry point | Coordinator RPC | Cached? | Refresh trigger |
|---|---|---|---|---|
| **New start** (by namespace) | `clientForNamespace` → `resolveNamespace` | `Resolve{namespace}` | **No** | every start re-resolves |
| **Route existing instance** (by id) | `clientForInstance` → `resolveInstance` | `Resolve{instance_id}` | **Yes** — by `(ns, epoch, shard)`, TTL'd | TTL expiry or `invalidate()` |
| **Active cells** (worker fan-out) | `activeCellTargets` | `ActiveCells` | **No client cache** | worker re-polls on its reconcile interval (10 s default) |

The rest of this doc is *why* each row is what it is.

---

## 2. New starts are NOT cached — on purpose

`resolveNamespace` hits the coordinator on **every** start and caches nothing. This looks wasteful until
you see what the call *does*: the coordinator spreads new starts across the current epoch's ring
(commit "Spread new starts across the ring"). **Resolving per-start is the load-spreading mechanism** —
so caching the answer would pin one client to one cell and defeat the balancing.

Why this is cheap, not a hot path:
- It is **one small control-plane RPC per *start***, not per operation on a running instance.
- It returns a target; the actual workflow start then goes **straight to the cell**, off the coordinator.
- It is the only place the client needs the *current* epoch, so it is also where a cutover is observed
  with no extra machinery — a fresh resolve after `OpenEpoch` naturally lands on the new epoch.

Cutover safety does **not** depend on a client TTL here: because every start re-resolves, a client can
never mint into a stale epoch. The soft-failure guard (a draining/stray cell rejecting a new start; see
"Contain a stray new cell") is a backstop, not the primary mechanism.

> If you are tempted to add a TTL cache in front of new-start resolution to cut RPCs, don't — you would
> be trading the coordinator's even spread for cell affinity. Batch or pool the coordinator channel
> instead (§6).

---

## 3. Instance routing IS cached — bounded and TTL'd

`resolveInstance` parses the id's baked-in `namespace.epoch.shard` ([IdCodec](sharding-and-epochs.md#2-the-self-routing-id))
and caches the endpoint under the key `namespace|e{epoch}|s{shard}`.

**Why it's safe to cache.** A past epoch's ring is **immutable** — once epoch N is open, its
`shard → cell` map never changes (it only moves OPEN → DRAINING → RETIRED). So the logical answer to
"which cell owns `(epoch, shard)`?" is stable for the life of the instance.

**Why the cache is bounded** (not one entry per instance): every instance on a shard shares one
`(ns, epoch, shard)` cell. The key space is `O(namespaces × epochs × shards)` — the same order as the
coordinator's own policy map — regardless of how many instances are in flight.

**What the TTL actually protects.** The *logical* mapping is immutable, but the *endpoint* behind it is
not: node failover within a cell changes the address list. So the TTL bounds **endpoint staleness**, not
mapping staleness. The client uses `max(1s, Endpoint.ttl_seconds)` — a coordinator-sent `ttl_seconds`
of 0 is floored to 1 s, never treated as "cache forever".

```java
// WiggleConnection.resolveInstance
Cached c = byShard.get(key);
if (c != null && System.nanoTime() < c.expiryNanos()) return c.endpoint();   // hit
ResolveResponse r = coord.resolve(/* by instance_id */);
long ttlNanos = Math.max(1, e.getTtlSeconds()) * 1_000_000_000L;             // 0 -> 1s floor
byShard.put(key, new Cached(e, System.nanoTime() + ttlNanos));
```

---

## 4. Invalidation — the "cell said no" signal

TTL expiry is the passive path; the active path is `invalidate(namespace)`, which drops every cached
instance-resolution for a namespace:

```java
public void invalidate(String namespace) {
    byShard.keySet().removeIf(k -> k.startsWith(namespace + "|"));
}
```

**Callers must invoke it when a cell RPC fails with `UNAVAILABLE` or `NOT_FOUND`**, so the next
operate-by-id re-resolves instead of hammering a dead or wrong endpoint. This is the mechanism that
recovers from: node failover (stale endpoint), an epoch that retired mid-TTL, or a stray/re-homed cell.
Treat a cell's "not here / closed" response as a cache-invalidation event, not a terminal error —
invalidate, re-resolve once, retry.

---

## 5. Active-cell set — interval-driven, not TTL-cached

Workers do not cache `ActiveCells` in the resolver; `NamespaceWorker` re-polls `activeCellTargets` on a
fixed **reconcile interval** (`reconcileEvery`, default 10 s) and reconciles its per-cell worker set —
standing up a worker for each newly-active cell (OPEN or DRAINING) and dropping one when its cell
retires. The interval *is* the refresh cadence; the proto's `generation`/`ttl_seconds` on
`ActiveCellsResponse` are not yet consulted client-side.

Consequence to know: a newly-active cell (e.g. the new epoch's cell after `OpenEpoch`) starts being
polled within one reconcile interval, and a retired cell stops being polled within one interval — so
the reconcile interval bounds how long a worker lags a topology change. Shorten it if faster cutover
pickup matters; lengthen it to cut coordinator load.

---

## 6. Two caches, don't conflate them

`WiggleConnection` holds **two** independent maps:

| Map | Keyed by | Bounds | Lifetime |
|---|---|---|---|
| `byShard` (resolution cache) | `(ns, epoch, shard)` | endpoint TTL (§3) + `invalidate` (§4) | short, refreshed |
| `clients` (connection reuse) | cell **target** | `close()` | long — channels are expensive |

Resolution answers "which target?"; `clientFor(target)` then returns a reused `WiggleClient`
(`computeIfAbsent`), so N instances on M cells open M channels, not N. Flushing the resolution cache
(§4) does **not** tear down channels — a re-resolve that lands on the same target reuses its client.

---

## 7. Direct mode (no coordinator)

With no `coordinatorUrl`, `WiggleConnection` is a pass-through to one `staticTarget`: no resolution, no
caching, no invalidation. This keeps existing single-cell (non-sharded) usage unchanged (R1). Everything
above applies only in coordinator mode.

---

## 8. Conformance checklist (for any client implementation)

A conformant client (Java, Python, …) **MUST**:
- Resolve **every** new start through the coordinator; never cache new-start resolution (§2).
- Route existing instances by the id's own `epoch`/`shard`, cached by `(ns, epoch, shard)` with a TTL
  of `max(1s, ttl_seconds)` (§3) — never one cache entry per instance.
- Invalidate the namespace's instance-resolutions on a cell `UNAVAILABLE`/`NOT_FOUND`, then re-resolve
  once before retrying (§4).
- Reuse one connection/channel per cell target (§6).

A conformant client **SHOULD**:
- Refresh a worker's active-cell set on a bounded interval and reconcile (add new, drop retired) (§5).
- Reuse a single coordinator channel across all resolutions rather than per-call channels.

A conformant client **MUST NOT**:
- Cache new-start resolution behind a TTL (breaks the coordinator's even spread — §2).
- Treat `ttl_seconds == 0` as "cache forever" (it means "1 s floor" — §3).
- Rewrite or recompute an instance's `epoch`/`shard` — they are baked in at birth and permanent.

---

## 9. Code map

| Concept | Where |
|---|---|
| resolution + both caches | `client/src/main/java/com/wiggle/client/WiggleConnection.java` |
| new-start spread (server side of §2) | `coordinator/runtime/**/CoordinatorApi.java` (`resolve` by namespace) |
| id parse (epoch/shard for §3) | `core/src/main/java/com/wiggle/core/IdCodec.java` |
| worker active-cell reconcile (§5) | `client/src/main/java/com/wiggle/client/worker/NamespaceWorker.java` |
| endpoint / ttl on the wire | `proto/src/main/proto/coordinator.proto` (`Endpoint`, `ResolveResponse`) |