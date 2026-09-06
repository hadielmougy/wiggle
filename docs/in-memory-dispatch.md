# In-memory dispatch matching

A design for reducing task-dispatch latency (and idle DB load) by matching freshly-produced
continuation tokens to waiting workers **in memory**, before — or instead of — a database claim, while
keeping the database the source of truth.

> **Status.** Layer 1 (§3, wake-on-produce) is **implemented** — see `DispatchNotifier` and the
> `poll` / `tx` / `txVoid` / `parkAtWorkerStep` changes in `WorkflowEngine`. Layer 2 (§4) and the
> active/passive path (§5) are design only.

---

## 1. The problem

When a worker completes a step, the engine settles that token and produces a **continuation** token
for the next node. Today the continuation becomes a normal `READY` row and is only picked up by a
later `poll`, which claims it from the database:

```
complete ──▶ settle prev token (DONE) ──▶ mint continuation ──▶ park READY (DB) ──▶ … ──▶ poll ──▶ claimTasks (DB)
```

Two costs:

- **Latency.** `poll` claims from the DB once, then long-polls by **busy-waiting** — `Thread.sleep(≤100ms)`
  and re-querying the DB in a loop (`WorkflowEngine.java:205-213`). A continuation produced 1 ms after a
  poller parked is not seen until that poller's next re-query. There is no wake-on-produce.
- **DB QPS.** Every poll — and every idle re-poll — is a database round-trip
  (`claimActivations → claimTasks`, `WorkflowEngine.java:222-230`). There is no in-memory ready buffer or
  waiter registry (the idea was considered and shelved — see the note at `WorkflowEngine.java:187-200`).

The goal: match a continuation to a waiting worker **in memory**, deferring or eliminating the claim
round-trip, without weakening durability.

## 2. What the cluster actually is (and why that reshapes the goal)

Confirmed from the code:

| Fact | Where |
|---|---|
| A cell = homogeneous nodes over **one shared database**; **any node drives any instance**. | `WorkflowEngine.java:16-24`, `WiggleServer.java:12-27` |
| The only cross-node serialization is the per-instance row lock (`SELECT … FOR UPDATE`). | `lockInstance`, `Tx.java:20-22`, `JdbcStorage.java:537-556` |
| No per-instance / per-shard node affinity inside a cell. Sharding is a **cell-level** concept (coordinator → cells), never node-level. | `CellPlacement.java:28-59`, `CoordinatorService.java:189-220` |
| Workers **pull**; nothing is pushed. Client resolution stops at a **cell**, not a node (`Endpoint.target = nodes.get(0)`, `// TODO stable cell DNS name`), but the response already carries **every** node address. | `GrpcApi.java:260-288`, `CoordinatorService.java:447-477` |
| A same-worker handback already exists (`leaseBack`) and bypasses poll/claim, but only in `LOCAL_SYNC` / `LOCAL_ASYNC`. | `WorkflowEngine.java:346-422` |
| Correctness rests **entirely** on the DB row lock + atomic claim — not on which node handles a request. | §5 of this doc |

**Governing principle.** Because any node can already claim any token via the DB, in-memory matching
buys **latency and DB-QPS only** — never correctness or new capability. So the DB claim stays the
source of truth and the fallback; every fast path must degrade to `claimTasks` safely.

**The DNS reframe.** It is tempting to think the continuation "lives on the node that produced it," so
a poll must be routed there. It does not — the token lives in the **shared DB**, visible to every node,
and the producing node is *unknowable* (any node may have run the completing step). So the goal is not
"route the poll to the producing node." It is "co-locate producer and consumer on a **deterministic**
node." The only deterministic key that matches how workers subscribe is the **queue**. Route by queue,
not by node.

## 3. Layer 1 — Wake-on-produce (no routing; do this first) — *implemented*

Replace the busy-poll with a **per-node waiter registry**: a map `queue → parked poll waiters`
(condition variables). When a continuation is parked `READY`, signal local waiters for that queue;
a woken poller runs **one** `claimTasks` (unchanged, still the atomic arbiter).

**As built**

- `DispatchNotifier` — the per-node registry: `signal(queues)` bumps a per-queue version and wakes
  waiters; `snapshot(queues)` + `awaitChange(queues, since, timeout)` block a poller until one of *its*
  queues advances past the snapshot, or the timeout elapses. Per-queue versioning avoids spurious
  cross-queue wakeups; snapshot-before-claim closes the lost-wakeup race.
- `WorkflowEngine.poll` — snapshots signal counts, claims, and if empty `awaitChange`s (capped by a
  `FALLBACK_POLL_MILLIS` = 100 ms safety net) instead of the old `Thread.sleep(≤100ms)` busy-loop.
- `parkAtWorkerStep` records the parked queue in a per-transaction thread-local; the `tx` / `txVoid`
  wrappers around each mutating entry point (`start`, `complete`, `advance`, `signal`, timer/signal/
  reclaim sweeps, `fail`, schedule fire) flush those queues to `notifier.signal(...)` **after the
  transaction commits**, so a woken poller always sees the committed `READY` row. Nesting defers to the
  outermost scope so sub-workflow/parent resumes signal once, post-commit.
- `poll` skips the claim if the caller's request is **cancelled** (`GrpcApi` passes the gRPC
  `Context::isCancelled`). Instant wake can otherwise deliver a freshly-produced token to a poll whose
  worker is mid-shutdown (its own drain woke it) — the token would be claimed then stranded until lease
  expiry. Declining to claim for a gone caller leaves the work `READY` for a live worker.

**Effect:** same-node dispatch latency drops from up to `FALLBACK_POLL_MILLIS` to ~0; the fallback still
catches cross-node production and any missed signal. No correctness change — the DB claim stays the
arbiter. (Raising `FALLBACK_POLL_MILLIS` would further cut idle-poll QPS at the cost of cross-node
latency; left at 100 ms to strictly not regress today's cadence.)

**Batching (linger).** Wake-on-produce is edge-triggered, so on its own it claims eagerly — as little as
one token per poll — which under load means more, smaller `claimTasks` round trips than the old
accumulate-then-batch busy-poll (a latency-for-throughput trade). To recover batching, after a wake the
poll lingers up to `DISPATCH_LINGER_MILLIS` (default 5 ms, bounded by the poll deadline) so a burst of
concurrently-produced tokens is drained in one claim of up to `max`. It only lingers when the worker
asked for more than one task (it has spare capacity to batch) and only after a real signal (not the
fallback timeout); `0` disables it. This does **not** reduce the *number* of steps that round-trip in
SERVER mode — that is what LOCAL_ASYNC chaining (§4 handback / local batch) is for; linger only makes
each poll carry more work.

**Properties**

- No node affinity, no routing, no protocol change.
- Correctness unchanged — the woken poller still claims from the DB.
- Only wakes waiters **on the same node** as the completion. A cross-node waiter still catches the token
  on its fallback poll. That residual is **latency, not loss**.

**Why first.** Removes the ~100 ms tail and most idle-poll QPS for the common case where one node serves
both the producing and consuming worker (already frequent under load-balancer stickiness). For many
workloads this, plus a tuned fallback interval, is enough — measure before building Layer 2.

## 4. Layer 2 — Skip the claim via queue-partitioned matching

Only if Layer 1's cross-node latency, or the synchronous claim write, still matters. This is the
"match before the DB claim" design, and it is the classic partitioned matching-service shape
(producers and consumers for a task queue both funnel to that queue's owner node; match in RAM).

### 4.1 Partition & routing (this is the DNS fix)

- Add intra-cell **node membership** + a **consistent-hash ring** over `(namespace, queue)` → owner node.
- **Consumer:** a worker polling queue `Q` dials `Q`'s **owner node** directly — resolved the same way
  `clientForInstance` resolves an instance to its cell today, using the node `addresses` already present
  in the resolve response (`CoordinatorService.java:447-477`). Concretely: `resolve(queue) → node`, dial
  it, bypass the load balancer. This is the answer to "which node do I call?" — deterministic by queue.
- **Producer:** a `complete` on any node produces a continuation on queue `Q`; the producing node
  **forwards the offer** to `Q`'s owner node (one intra-cluster RPC), where `Q`'s waiters live. The
  sync-match happens there in memory.

*Alternative (keep workers on the LB):* each node gossips "I hold waiters for queues {…}", and the
producer forwards offers to a node that has one. Simpler clients, more chatter, needs a waiter
directory. The ring approach is cleaner and reuses existing coordinator resolution.

### 4.2 Durability rule — the line you must not cross

Wiggle recovery is **token-based, not event-replay**. An instance resumes only because `complete`
committed *prev token DONE + continuation persisted* under `lockInstance`. Therefore:

1. **Persist the continuation as `READY` inside the `complete` transaction** (as today).
2. Then do the in-memory match and hand the activation to the waiter.
3. Make the **claim** (`RUNNING` + lease) write **lazy / async / batched**.
4. Crash before the lazy claim flush → the token is still `READY` in the DB → the existing
   lease-expiry / `SKIP LOCKED` reclaim redelivers it. **At-least-once is preserved** — which the engine
   already guarantees.

What this removes from the hot path: the synchronous **claim `UPDATE`** and the **poll round-trip**.

What it must **not** do: hold the continuation *only* in memory and persist later. A crash there stalls
the instance forever — prev is `DONE`, no continuation row exists, and the reclaimer has nothing to find.
That variant requires an outbox / event log so recovery can recompute the step — a much larger change
that trades away wiggle's simple token-based recovery. Out of scope.

### 4.3 Ordering under the ring

The match on `Q`'s owner is **dispatch, not state mutation** — it happens *after* the `complete`
transaction already committed under `lockInstance`, so per-instance ordering remains the DB's job. The
in-memory step only assigns a lease, which is lazy and at-least-once. So a stale ring during a
membership change merely means a token is matched on the "wrong" node → it falls back to the DB claim.
No correctness impact; only a missed fast-path.

## 5. Alternative to Layer 2 — Layer 1 + active/passive (collapsing co-location)

Layer 2 exists only because a cell has **many active nodes**, so a completion on one node cannot wake a
waiter parked on another (§3). An entirely different way to close that gap is to remove the multiple
active nodes: run each cell **active/passive** — one node serves all API/dispatch/engine traffic, the
rest are warm standbys promoted on failure.

With a single active node per cell, every `poll` and every `complete` land on the same node **by
construction**, so Layer 1's wake-on-produce covers 100% of dispatches. Better still, the full
"skip the synchronous claim" payoff of Layer 2 becomes reachable **without any of Layer 2's machinery**
— no ring, no membership routing, no offer-forwarding — because the waiter registry and match table are
entirely local. The DNS problem disappears: a worker dials the cell endpoint, which is always the active
node.

This fits the grain of the system. Horizontal scale in wiggle is already **cellular** (a namespace = a
cell = its own DB + cluster; you scale by adding cells and resharding via the coordinator). Active/passive
says "HA *within* a cell, scale *by adding* cells" — consistent with that thesis. The plumbing is
half-built: `ClusterManager` already elects a DB-derived leader (longest-running alive node) that runs
housekeeping (`ClusterManager.java:90-123`); you would promote that same leader to be the sole dispatcher.

### 5.1 Trade-offs

| Trade-off | Detail |
|---|---|
| **Per-cell throughput ceiling = one node.** | You give up active-active scale-out *within* a cell; all cell work funnels through one JVM and passive nodes are idle capacity. The relief valve is coordinator resharding (`openEpoch`). Fine for most workloads; a real limit for a hot, un-shardable namespace. |
| **Failover availability blip.** | Active-active keeps serving from survivors; active/passive has a detection + promotion window where *nobody* serves the cell (bounded by the heartbeat timeout). Workers polling the dead node error out and must **re-resolve to the new active node** — needs a cell endpoint that tracks the active (the existing `// TODO stable cell DNS name`) or coordinator-driven active discovery with client retry. |
| **In-memory state lost on promotion (safely).** | The new active starts with an empty registry. Because continuations are persisted `READY` before hand-out (§4.2 / §6), nothing is lost — unflushed lazy-claims are reclaimed by lease expiry. Cost: a recovery blip up to the lease duration, so the lazy-claim flush window trades against failover recovery time. |
| **Deploys become controlled failovers.** | Upgrading the active *is* a failover (a blip); passives upgrade free. The deploy playbook changes from rolling-drain to promote-then-upgrade. |
| **Warm standbys.** | For fast promotion, passives should be DB-connected with graphs cached. Small cost; cold standbys promote slower. |

### 5.2 The correctness caveat — keep the DB claim as the fence

Active/passive failover is **not clean**: a slow old active (GC pause, network blip) may still believe it
is active while a new one is promoted — two actives briefly. Active-active tolerates this because the
atomic DB claim arbitrates. But if the fast path **fully skips** the claim and matches purely in memory,
both actives can match the same `READY` token and **double-dispatch** it, breaking exactly-once dispatch.

The fix is the same persist-then-**lazy**-claim rule from §4.2: the lazy claim is still the atomic
`SKIP LOCKED` / CAS `UPDATE`, just deferred. Under split-brain both actives flush, one wins the `UPDATE`,
and the loser's in-memory dispatch is retracted (its worker gets a lost-lease rejection at `complete` via
`requireLease`, `WorkflowEngine.java:285`). The worst case collapses to *at-least-once execution* — which
the engine already guarantees — not double state commit.

This matters because wiggle's leader election is **DB-derived with no consensus** ("leader-only duties are
idempotent", `ClusterManager.java:15-28`). That is fine for idempotent housekeeping but **not** for
claim-free dispatch. So: **lazy claim yes, no-claim no.** The atomic claim is what survives imperfect
failover.

### 5.3 Middle ground — partitioned active-active

If the single-node ceiling is a concern but full Layer 2 is too much: run multiple active nodes but have
each **own disjoint queues** (or shard ranges). Within a queue there is still one owner, so Layer 1
co-location holds. This is Layer 2's ring at node granularity — scale-out without a separate matching
service (producers still forward cross-queue offers, but there is no offer-matching subsystem). It is the
natural next step if a cell outgrows one node.

## 6. Correctness invariants (must hold in every layer)

- **State transitions still commit under `lockInstance`.** In-memory matching never mutates instance
  state; it only assigns dispatch. `complete`/`advance` keep taking the instance lock first
  (`WorkflowEngine.java:260-265`).
- **The DB claim stays the arbiter and the fallback.** A cross-node poll, a cold poll, or ring churn all
  degrade to `claimTasks` (`FOR UPDATE SKIP LOCKED` / CAS, `JdbcStorage.java:657-736`). A token is never
  dispatched to two workers because the lazy claim still goes through the atomic path; an in-memory
  hand-off that races a DB claim loses to whichever writes `RUNNING` first, and the loser re-parks.
- **No token is lost.** Every dispatched token is already `READY` in the DB before it is handed out, so
  lease expiry always recovers it.

These are exactly the invariants the existing `leaseBack` handback already respects (it pre-computes the
next token in the ack path and keeps it as a DB-backed lease, `WorkflowEngine.java:411-422`) — Layer 2
generalizes that from "same worker" to "any waiting worker for the queue."

## 7. Scope reducers

- **Same-worker is already free.** `leaseBack` hands the next step to the *same* worker with no
  poll/claim in `LOCAL_*` modes. Cross-node matching only needs the **cross-worker** case. A cheap
  intermediate: extend stickiness so a worker that just finished, and also serves the continuation's
  queue, keeps it even in `SERVER` mode.
- **Layer 1 is independent** and independently shippable — it stands alone, precedes Layer 2, and is a
  prerequisite of the §5 active/passive path.

## 8. Recommendation

Two coherent roads, both starting from Layer 1:

- **Road A — active/passive (§5).** Layer 1 + one active node per cell. Co-location holds by
  construction, the full skip-the-claim payoff is reachable with **no** ring/routing machinery, and the
  DNS problem disappears. Cost: per-cell throughput ceiling of one node and a failover blip. Best fit if
  you are happy scaling by adding cells (the cellular thesis) rather than nodes-within-a-cell.
- **Road B — active-active + Layer 2 (§4).** Keep multiple active nodes; add queue-partitioned matching
  with client routing to the partition owner. Preserves intra-cell scale-out, at the cost of a matching
  subsystem (membership, hash ring, offer-forwarding, ring-change behavior). §5.3 (partitioned
  active-active) is the lighter middle of this road.

Invariants for **either** road:

1. **Persist the continuation `READY` first; make the claim lazy.** Never match-before-persist.
2. **Keep the atomic DB claim as the fence** — it is what survives imperfect failover / split-brain.
3. **The DB claim stays the fallback** — cross-node, cold poll, ring churn, and failover all degrade to
   `claimTasks`.

**Cost honesty.** Because any node can already claim any token via the DB, none of this buys correctness
or capability — only latency and DB-QPS. Do Layer 1 and **measure** before committing to Road A or B.

## 9. Open questions

- **Partition count & rebalancing.** Fixed partitions per `(namespace, queue)` vs. a ring directly over
  nodes; how partitions move on membership change without stranding waiters.
- **Waiter fairness.** With multiple waiters for a queue on the owner node, FIFO vs. capacity-weighted
  hand-off (mirror `WorkerOptions.concurrency`).
- **Backpressure.** How the waiter registry interacts with `MemoryGuard.rejectPoll()`
  (`GrpcApi.java:266-275`) under heap pressure.
- **Lazy-claim batching window.** How long `RUNNING`/lease writes may lag before they must flush, and its
  effect on lease-expiry reclaim timing.
- **Cross-region.** Whether queue-owner routing should be region-aware (the coordinator already carries
  `callerRegion`).
