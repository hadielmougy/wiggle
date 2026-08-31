# Feature change — seal published rings: no ring change without a new epoch

**Status:** implemented (option A — guarded `SetRing`). **Supersedes:** the in-place `SetRing` path in
[sharding-and-epochs.md](sharding-and-epochs.md) §6–§7, now updated to the sealed-ring rule.

Make a published epoch's `shard → cell` ring **immutable** ("ceil the ring"): once an epoch is written,
its ring slots never change. Every reshard — add, remove, move, rebalance — goes through `OpenEpoch`,
which increments the epoch. `SetRing` is retired as a mutation path.

> One-line invariant: **the epoch is the only thing that changes a ring.** A ring slot is written once,
> at the epoch's birth, and is read-only for the epoch's life.

---

## 1. Why

Today `doSetRing` replaces a live epoch's ring wholesale, with no check that the edit is relocation-safe:

```java
// CoordinatorApi.doSetRing — current: any replacement is accepted
CoordPolicy.EpochRing existing = c.epochs().get(epoch);
epochs.put(epoch, new CoordPolicy.EpochRing(toDomainRing(ring), existing.status()));   // <-- no guard
```

`sharding-and-epochs.md` §7 already documents the trap this opens: **removing a shard in place silently
misroutes** every live instance on that shard, because `cellFor` wraps an unknown shard to the wrong
cell — a mis-route with *no error*. The current mitigation is a **rule an operator must remember**
("`SetRing` is safe only when the edit relocates no live instance; when in doubt, `OpenEpoch`"). That is
a footgun guarded by discipline.

The additive case (adding a shard in place) *is* safe today — but keeping `SetRing` alive for it means
every ring edit still has to be judged relocation-safe-or-not by a human, and the unsafe path stays one
typo away. Sealing the ring **removes the judgment call entirely**: there is exactly one way to change a
ring, and it is always safe, because old instances keep resolving through the immutable epoch they were
born under (the epoch model's whole point — §4).

Cost of the change: a purely-additive edit that used to be a free in-place `SetRing` now costs one new
(briefly draining) epoch. That is the price of deleting a silent-misroute class. We judge it worth it.

---

## 2. What is sealed — and what is not

The invariant is scoped to a ring's **shard → cell slots**, not to everything on an `EpochRing`:

| Field | Mutable after publish? | Why |
|---|---|---|
| ring slots (`shard → cell`, region) | **No** — sealed | changing them relocates live keys |
| epoch **status** (`OPEN → DRAINING → RETIRED`) | **Yes** | a status flip remaps no key; it only gates minting/polling |
| `currentEpoch` pointer | **Yes** (via `OpenEpoch`) | advancing it is how a reshard takes effect |

So the reconciler's drain→retire lifecycle (§10) is untouched — it transitions `status`, never slots.
"Ceil the ring" = the slot mapping is a ceiling, sealed at birth; the status underneath it still moves.

Genesis is not an exception to police: the first `OpenEpoch` on an empty policy *creates* epoch 0's ring
(it isn't changing one), and `doOpenEpoch` already handles that branch.

---

## 3. The change (option A — shipped)

`SetRing` is retired as a mutation. `CoordinatorService.doSetRing` now rejects any call whose ring
differs from the epoch's current slots (a clear, typed `IllegalArgumentException` → gRPC
`INVALID_ARGUMENT`), and tolerates an identical ring as an idempotent no-op that writes nothing:

```java
public Policy doSetRing(String namespace, long epoch, List<RingSlot> ring) {
    CoordPolicy c = store.getPolicy(namespace)
            .orElseThrow(() -> new IllegalArgumentException("no policy for namespace " + namespace));
    CoordPolicy.EpochRing existing = c.epochs().get(epoch);
    if (existing == null) throw new IllegalArgumentException("no epoch " + epoch + " in namespace " + namespace);
    if (!sameSlots(existing.ring(), toDomainRing(ring))) {
        throw new IllegalArgumentException("ring is sealed: epoch " + epoch + " of namespace '" + namespace
                + "' cannot be reshaped in place; open a new epoch instead (OpenEpoch)");
    }
    return toProto(c);   // identical slots -> idempotent no-op, no CAS write
}
```

`sameSlots` compares the `(shard, cellId, region)` set order-independently (`HashSet` equality).
Nothing else in the coordinator changed: `doOpenEpoch` already appends `currentEpoch + 1`, marks the
previous epoch `DRAINING`, and CAS-guards on `revision` — it is now the sole ring-writing path. The
CLI / `CellResolver.openEpoch` remain the sanctioned reshard entry point; no new operator surface.

**Still open (option B — later).** The `SetRing` RPC stays in `coordinator.proto` for now (a guarded
no-op/reject). Deprecate it in the proto and remove it on the next contract bump.

---

## 4. Interaction with the rest of the system

- **Resolution / client cache.** Strengthens the [client caching contract](client-caching-contract.md)
  §3: the "a past epoch's ring is immutable" assumption the instance-routing cache relies on becomes a
  hard, enforced invariant rather than an emergent property — the cache is now provably correct to hold
  a `(ns, epoch, shard) → cell` entry for the epoch's life (endpoint TTL still bounds address drift).
- **Concurrency (§8).** Unchanged and simpler: the "torn ring" concern goes away for edits, since an
  epoch's ring is write-once. `casPolicy` on `revision` still serializes `OpenEpoch` and status flips.
- **Draining stacks.** The honest downside: additive edits that were free now add a draining epoch, and
  draining epochs multiply worker poll targets (§6). Mitigation is unchanged — let an epoch drain before
  opening the next; the reconciler retires drained epochs promptly.

---

## 5. Tests (shipped)

- `MultiCellResolveTest.removeShardInPlaceIsRejected` — the in-place removal that used to silently
  mis-route is now rejected; the instance still resolves to its cell afterward.
- `MultiCellResolveTest.addShardViaNewEpochIsSafe` — the additive case goes through `OpenEpoch`; the
  in-place add is asserted to be rejected.
- `MultiCellResolveTest.setRingIdenticalIsNoOp` — an exact-ring (re-ordered) `SetRing` bumps no revision.
- `CoordinatorApiTest.setRingRejectsSlotChange` / `setRingNoOpIsIdempotent` — guard fires on a slot delta;
  identical slots return the policy without a CAS.
- `MultiCellResolveTest.removeShardViaNewEpochIsSafe` — unchanged (already uses `OpenEpoch`).

---

## 6. Migration

1. ✅ **Done.** Guard (option A) + tests landed; `sharding-and-epochs.md` §6–§7 updated to the
   sealed-ring rule, dropping the "`SetRing` is the lighter alternative" guidance.
2. **Todo.** Mark `SetRing` deprecated in `coordinator.proto` (comment + release note): operators move
   to `OpenEpoch`.
3. **Todo.** On the next proto contract bump, remove the `SetRing` RPC (option B).

No stored data migrates: existing policies are already valid under the stricter rule (their published
rings simply become read-only from here on).

---

## 7. Code map

| Concept | Where |
|---|---|
| ring-write paths (`doOpenEpoch`, `doSetRing`) | `coordinator/runtime/**/CoordinatorApi.java` |
| `SetRing` / `OpenEpoch` RPCs | `proto/src/main/proto/coordinator.proto` |
| the misroute this seals off | [sharding-and-epochs.md](sharding-and-epochs.md) §7 (`cellFor` modulo-wrap) |
| drain → retire (status, still mutable) | `coordinator/runtime/**/CoordinatorReconciler.java` |
| operator entry point (unchanged) | `client/**/CellResolver.openEpoch`, `wiggle open-epoch` CLI |