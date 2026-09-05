# Feature change — seal published rings: no ring change without a new epoch

**Status:** implemented — the `SetRing` RPC was **removed** outright (no clients shipped it yet, so no
deprecation window was needed). **Supersedes:** the in-place `SetRing` path in
[sharding-and-epochs.md](sharding-and-epochs.md) §6–§7, now updated to the sealed-ring rule.

Make a published epoch's `shard → cell` ring **immutable** ("ceil the ring"): once an epoch is written,
its ring slots never change. Every reshard — add, remove, move, rebalance — goes through `OpenEpoch`,
which increments the epoch. There is no in-place ring edit at all: `SetRing` is gone from the proto and
the coordinator, so `OpenEpoch` is the sole ring-writing path.

> One-line invariant: **the epoch is the only thing that changes a ring.** A ring slot is written once,
> at the epoch's birth, and is read-only for the epoch's life.

---

## 1. Why

`doSetRing` used to replace a live epoch's ring wholesale, with no check that the edit was
relocation-safe:

```java
// CoordinatorApi.doSetRing — before: any replacement was accepted
CoordPolicy.EpochRing existing = c.epochs().get(epoch);
epochs.put(epoch, new CoordPolicy.EpochRing(toDomainRing(ring), existing.status()));   // <-- no guard
```

`sharding-and-epochs.md` §7 documents the trap this opened: **removing a shard in place silently
misroutes** every live instance on that shard, because `cellFor` wraps an unknown shard to the wrong
cell — a mis-route with *no error*. The only mitigation was a **rule an operator had to remember**
("`SetRing` is safe only when the edit relocates no live instance; when in doubt, `OpenEpoch`") — a
footgun guarded by discipline.

The additive case (adding a shard in place) *was* safe — but keeping `SetRing` alive for it would mean
every ring edit still had to be judged relocation-safe-or-not by a human, with the unsafe path one typo
away. Sealing the ring **removes the judgment call entirely**: there is exactly one way to change a
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

## 3. The change (RPC removed)

`SetRing` is gone entirely — there is no in-place ring-edit surface to guard:

- **Proto.** The `SetRing` RPC and `SetRingRequest` message are removed from `coordinator.proto`.
- **Coordinator.** `CoordinatorApi.setRing` (the handler) and `CoordinatorService.doSetRing` (plus its
  `sameSlots` helper) are deleted. `doOpenEpoch` is now the only method that writes a ring: it appends
  `currentEpoch + 1`, marks the previous epoch `DRAINING`, and CAS-guards on `revision`.

Because no client ever shipped a `SetRing` call, this is a clean removal rather than a deprecation — no
compatibility window, no guarded no-op to carry. The CLI / `WiggleConnection.openEpoch` remain the sanctioned
reshard entry point; no new operator surface. A published epoch's ring is now immutable *by
construction*: nothing in the wire contract can express "edit this ring."

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
- **Strict placement.** `OpenEpoch` being the sole ring-writer pairs with the strict placement model: a
  coordinated namespace with no ring is *not-ready* (fail closed with `NamespaceNotReadyException`), not
  routed to an inferred implicit cell. The ring is the one source of placement, and it only exists once
  an epoch is opened — see [sharding-and-epochs.md](sharding-and-epochs.md) §1.

---

## 5. Tests (shipped)

The reshards now go through `OpenEpoch`, and the removed in-place paths took their tests with them:

- `MultiCellResolveTest.addShardViaNewEpochIsSafe` — adding a shard opens a new epoch; existing ids keep
  resolving via their own epoch's ring, the new epoch routes the new shard to its cell.
- `MultiCellResolveTest.removeShardViaNewEpochIsSafe` — removing a shard omits it from a new epoch; the
  old epoch retains it (its instances keep resolving) while it drains.

Deleted with the RPC: the `SetRing`-centric cases (`removeShardInPlaceIsRejected`,
`setRingIdenticalIsNoOp`, `CoordinatorApiTest.setRingRejectsSlotChange` / `setRingNoOpIsIdempotent`) —
there is no longer an in-place edit to reject or no-op, so the invariant they asserted is now structural.

---

## 6. Migration

No stored data migrates: existing policies are already valid (their published rings simply become
read-only). No wire-compatibility step is needed either — the `SetRing` RPC had no released callers, so
removing it breaks nothing. Done in one change: proto + coordinator + tests + docs.

---

## 7. Code map

| Concept | Where |
|---|---|
| the sole ring-write path (`doOpenEpoch`) | `coordinator/runtime/**/CoordinatorService.java` |
| coordinator gRPC surface (no `SetRing`) | `proto/src/main/proto/coordinator.proto`, `CoordinatorApi.java` |
| the misroute this seals off | [sharding-and-epochs.md](sharding-and-epochs.md) §7 (`cellFor` modulo-wrap) |
| drain → retire (status, still mutable) | `coordinator/runtime/**/CoordinatorReconciler.java` |
| operator entry point (unchanged) | `client/**/WiggleConnection.openEpoch`, `wiggle open-epoch` CLI |