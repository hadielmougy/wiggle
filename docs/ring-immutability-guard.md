# Feature change — seal published rings: no ring change without a new epoch

**Status:** proposed. **Supersedes:** the in-place `SetRing` path in
[sharding-and-epochs.md](sharding-and-epochs.md) §6–§7 (which stays accurate until this lands).

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

## 3. The change

**Retire `SetRing` as a mutation.** Two implementation options; pick one:

- **(A) Reject slot mutation (keep the RPC).** `doSetRing` rejects any call whose ring differs from the
  epoch's current slots, so old clients get a clear, typed error instead of a silent reshape. A no-op
  (identical slots) is tolerated so retries stay idempotent.
- **(B) Remove the RPC.** Delete `SetRing` from `coordinator.proto` and the API; callers use
  `OpenEpoch`. Cleaner end state, but a wire-contract change — do it as a deprecation, not a hard break.

Recommended: **(A) now, (B) later.** Ship the guard immediately (small, safe, closes the footgun);
deprecate the RPC and remove it on the next contract bump.

Guard sketch for (A):

```java
public Policy doSetRing(String namespace, long epoch, List<RingSlot> ring) {
    // ... load policy, find existing epoch ...
    if (!sameSlots(existing.ring(), toDomainRing(ring))) {
        throw new IllegalArgumentException(
            "ring is sealed: epoch " + epoch + " of '" + namespace + "' cannot be reshaped in place; "
            + "open a new epoch instead (OpenEpoch)");
    }
    return toProto(store.getPolicy(namespace).orElseThrow());   // no-op: nothing to CAS
}
```

`sameSlots` compares the `(shard, cellId, region)` set order-independently. Nothing else in the
coordinator changes: `doOpenEpoch` already appends `currentEpoch + 1`, marks the previous epoch
`DRAINING`, and CAS-guards on `revision` — it is the sole ring-writing path after this.

The CLI/`CellResolver.openEpoch` already exist as the sanctioned reshard entry point; no new surface is
needed for operators.

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

## 5. Tests

- `MultiCellResolveTest.removeShardInPlaceMisroutes` — **invert**: the reshape must now be *rejected*, so
  the misroute is unreachable. Rename to `removeShardInPlaceIsRejected`.
- `MultiCellResolveTest.addShardInPlace` — **replace** with `addShardViaNewEpochIsSafe` (the additive
  case now goes through `OpenEpoch`).
- New: `setRingRejectsSlotChange` (guard fires on any slot delta) and `setRingNoOpIsIdempotent` (identical
  slots return the current policy without a CAS).
- Existing `…removeShardInPlaceViaNewEpochIsSafe` stays green unchanged — it already uses `OpenEpoch`.

---

## 6. Migration

1. Land the guard (option A) + tests; update `sharding-and-epochs.md` §6–§7 to state the sealed-ring
   invariant and drop the "`SetRing` is the lighter alternative" guidance.
2. Mark `SetRing` deprecated in `coordinator.proto` (comment + release note): operators move to
   `OpenEpoch`.
3. On the next proto contract bump, remove the `SetRing` RPC (option B).

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