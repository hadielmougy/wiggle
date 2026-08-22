# Design: Worker-side local execution (step chaining)

Status: **Phase 0–2 implemented + `.checkpoint()` + graceful-shutdown drain** · Target: post-2.0 · Owner: TBD

> **Benchmark (linear 20-step pipeline, 1000 instances, 4 workers × 16, Postgres):**
> SERVER 15 inst/s · LOCAL_SYNC 102 inst/s · **LOCAL_ASYNC 213 inst/s**. Async is ~2× sync on a
> real DB because a 20-step run commits once (≈20× fewer WAL fsyncs) rather than per step; on the
> in-memory store, where commits are ~free, async ≈ sync. Reproduce with `./gradlew :example:bench`
> (set `WIGGLE_EXECUTION_MODE`, `WIGGLE_JDBC_URL`, `WIGGLE_BENCH_*`).

> **Implemented:** the `GraphTraversal` seam (`core`), `ExecutionMode` on the definition (in the
> content hash) with the `.execution(...)` DSL flag, the `AdvanceRun` wire RPC + `execution_mode`
> on `TaskActivation`, `WorkflowEngine.advanceRun` (which already applies multi-step batches
> atomically), and the worker's unified local loop: `LOCAL_SYNC` flushes every step, `LOCAL_ASYNC`
> buffers up to `WorkerOptions.localBatchSize` (default 64) and flushes the run in one call. A
> mid-run failure flushes the successful prefix then fails the offending step's token.
> The `WIGGLE_EXECUTION_MODE` server default is still deferred — `DEFAULT` resolves to `SERVER`;
> set the mode per-workflow via `.execution(...)`.

## 1. Summary

Today the server drives the state machine one node at a time: every worker step is a
poll → execute → complete round-trip plus a committed DB transaction, with the context
re-shipped both ways. This proposal lets a worker, once it has claimed a token, **execute a
run of consecutive nodes locally** — traversing the graph it already holds and mutating the
context in memory — reporting progress to the server (synchronously or asynchronously) until
it reaches a node that needs server coordination (a "boundary"), then handing control back.

The feature is opt-in and configurable per workflow definition, with a server-wide default.

### Goals
- Collapse a linear run of *K* same-queue steps from ~2K RPCs + O(K) commits into one claim,
  K local executions, and 1 (async) or K (sync) status writes.
- Preserve Wiggle's core properties: pull-based workers (no inbound connectivity), at-least-once
  execution, immutable per-version definitions, multi-node correctness.
- Make the durability/throughput tradeoff an explicit, per-workflow choice.

### Non-goals
- No change to `SERVER` mode semantics (it remains the default and the reference behaviour).
- No per-step boundary control in v1 (only automatic boundaries; see §4).
- No exactly-once guarantee — this stays at-least-once (see §10).

## 2. Current model (recap)

`WorkflowEngine.drive()` advances a token until it parks on something needing the outside
world (`READY` for a worker, `WAITING` for a timer, `AWAITING` for a user, `JOINED` for a
sibling). A worker claims a `READY` token via `PollTasks`, executes exactly one activity, and
calls `CompleteTask`/`FailTask`; the server advances one node and the cycle repeats. The worker
is "dumb": it only knows the single `TaskActivation` it was handed.

Key enabler for this proposal: **the worker already holds the full compiled graph.** When it
calls `register(Blueprint)` it keeps the `WorkflowDefinition` (nodes + edges) and the handler
table, so it can locally resolve "what runs next" without asking the server.

## 3. Execution modes

A new enum, part of the definition and resolved per dispatch:

| Mode | Who drives | Status writes | Crash blast radius | Use for |
|---|---|---|---|---|
| `SERVER` (default) | server, one node at a time | per step (today) | one step | payments, anything non-idempotent |
| `LOCAL_SYNC` | worker chains locally | per step, before continuing | one step (same as today) | most workflows — safe speedup |
| `LOCAL_ASYNC` | worker chains locally | batched at handback | whole local run | idempotent, high-volume pipelines |

`LOCAL_SYNC` is the sweet spot: it removes the re-poll / re-claim / re-dispatch round-trip and
the context re-shipping between steps while committing each step, so it is **strictly as durable
as today** and still meaningfully faster. `LOCAL_ASYNC` trades durability for maximum throughput.

## 4. The handback boundary

A worker in a `LOCAL_*` mode, after executing a `TASK`/`PREDICATE`, continues locally **iff**
the next node is a `TASK`/`PREDICATE` on a queue this worker serves and is available now.
Otherwise it hands back. Boundaries (all derived from the existing node model):

1. next node is `SLEEP` (server timer),
2. next node is `FORK` (fan-out, possibly to other workers),
3. next node is `JOIN` (barrier across siblings),
4. next node is `USER_TASK` (external),
5. next node is `TASK`/`PREDICATE` on a **different queue**,
6. the step **failed and needs a delayed retry** (backoff timer),
7. next node is `END` (terminal — report and stop),
8. **lease budget** is nearly exhausted (hand back before being reclaimed),
9. the server reports the instance is **no longer RUNNING** (cancelled/failed) on a status write.

The rule is pure and local: *"continue while the next node is a same-queue TASK/PREDICATE I can
run now and I have lease budget."*

## 5. Config surface

### 5.1 Per-workflow-definition flag (primary)

```java
Workflow.define("name", codec)
        .execution(ExecutionMode.LOCAL_SYNC)   // SERVER | LOCAL_SYNC | LOCAL_ASYNC | DEFAULT
        .step(...) ...
```

- New field on `WorkflowDefinition`: `ExecutionMode executionMode` (default `DEFAULT`).
- Serialized in `WorkflowDefinition.toJson()` **and included in `contentVersion()`**, so the mode
  is part of the immutable version hash — an in-flight instance can never switch modes under you,
  and changing the mode mints a new version like any other topology change.
- `DEFAULT` is a stable, hashable sentinel meaning "defer to the server's configured default".

### 5.2 Server default

```
WIGGLE_EXECUTION_MODE = SERVER | LOCAL_SYNC | LOCAL_ASYNC   (default SERVER)
```

Resolution: a definition's `DEFAULT` resolves to `WIGGLE_EXECUTION_MODE` at dispatch time. An
explicit mode on the definition always wins. **The server resolves `DEFAULT` and stamps the
concrete mode onto each `TaskActivation`** (§6), so the worker never needs to know the server's
env or re-derive the default — it just obeys what it was handed.

### 5.3 Optional future knobs (not v1)
- `WIGGLE_LOCAL_MAX_STEPS` / `.execution(mode, maxSteps)` — cap a local run length for fairness.
- `WIGGLE_LOCAL_ASYNC_FLUSH_MILLIS` — periodic flush cadence for `LOCAL_ASYNC`.
- Per-step `.step(...).checkpoint()` — force a commit boundary even in async mode.

## 6. Wire-protocol changes (`proto/wiggle.proto`)

One new RPC, one new field, two new messages. `CompleteTask`/`FailTask` stay (they are `SERVER`
mode and the K=1 building block).

```proto
service WiggleControlPlane {
    // ... existing ...
    // Applies an ordered run of locally-executed steps atomically, keeps the lease, and tells
    // the worker whether to keep going. Used by LOCAL_SYNC (one step per call) and LOCAL_ASYNC
    // (whole run at handback).
    rpc AdvanceRun(AdvanceRunRequest) returns (AdvanceRunResult);
}

message StepResult {
    string node_id = 1;                       // node the worker executed (server validates the path)
    oneof outcome {
        google.protobuf.Value merge = 2;      // TASK: shallow-diff to merge into context
        bool predicate_value = 3;             // PREDICATE: branch selector
        StepFailure failure = 4;              // the step threw
    }
}

message StepFailure { string message = 1; bool retryable = 2; }

message AdvanceRunRequest {
    string task_id = 1;                       // the claimed starting token
    string lease_owner = 2;
    repeated StepResult steps = 3;            // ordered, may be a single step (sync) or many (async)
    bool final = 4;                           // true = worker reached a boundary/terminal and is handing back
}

message AdvanceRunResult {
    string instance_status = 1;               // RUNNING | COMPLETED | FAILED | CANCELLED
    int64  lease_expires_at = 2;              // renewed lease covering continued local execution
    string parked_task_id = 3;                // token now parked at the boundary (observability/continuity)
}
```

Add to the existing `TaskActivation` so the worker knows the resolved mode without guessing:

```proto
message TaskActivation {
    // ... existing fields 1..12 ...
    string execution_mode = 13;               // SERVER | LOCAL_SYNC | LOCAL_ASYNC (already resolved)
}
```

Server behaviour for `AdvanceRun` (under the instance lock, one transaction per call):
- For each `StepResult` in order, replay the same transition `complete()`/`fail()` would have
  done for that node (merge context or select predicate branch; on failure, apply retry policy).
- Validate each `node_id` against the actual graph path; reject a bogus/mismatched path with a
  conflict (defensive — the worker is untrusted about topology).
- If `final=true`, drive the boundary node normally (SLEEP→WAITING, FORK→spawn, JOIN→barrier,
  USER_TASK→AWAITING, END→terminal), releasing the token; else leave the continuation token
  `RUNNING` and leased to this worker.
- Return the instance status (so the worker stops if it was cancelled) and a renewed lease.

## 7. The shared traversal seam (`core`)

To avoid two divergent drivers, extract the *pure* per-node decision into `core`, used by both
`WorkflowEngine.drive()` (server) and the new client driver:

```java
// core: pure functions over the graph model, no DB, no I/O.
final class GraphTraversal {
    /** The successor of a completed worker step: task -> next; predicate -> next/altNext. */
    static String successor(Node node, boolean predicateValue);

    /** Why a worker must hand back at {@code next}, or null if it can keep running it locally. */
    static Handback classify(Node next, Set<String> workerQueues);
}

enum Handback { SLEEP, FORK, JOIN, USER_TASK, OTHER_QUEUE, TERMINAL /* END */ }
```

- The server's `drive()` is refactored so its TASK/PREDICATE edge resolution calls
  `successor(...)`; the classification of "is the next node worker-runnable" is shared.
- The client driver (§8) uses both to decide when to stop. The server does *more* (fork spawn,
  join barrier, DB writes) — that stays server-only; only the narrow "advance one worker node and
  decide continue-vs-handback" logic is shared, which is exactly the part that must never diverge.
- Unit-test `GraphTraversal` directly (pure, fast) as the single source of truth.

## 8. Client changes (`Worker`)

`Worker.execute(TaskActivation)` gains a mode switch:

- `SERVER` (or unknown/unregistered version): unchanged — execute one activity, `CompleteTask`/`FailTask`.
- `LOCAL_SYNC` / `LOCAL_ASYNC`: run the local driver loop:
  1. Look up the blueprint for `(workflow, version)` (already registered); if absent, fall back to `SERVER`.
  2. Execute the current node's handler; accumulate the result into a local context copy.
  3. Buffer a `StepResult`. In `LOCAL_SYNC`, `AdvanceRun([step], final=false)` now; in `LOCAL_ASYNC`, keep buffering.
  4. Compute `successor(...)`; `classify(...)` the next node.
     - runnable locally and lease budget remains → loop to step 2.
     - boundary → `AdvanceRun(buffered, final=true)` and stop.
  5. If any `AdvanceRunResult.instance_status != RUNNING`, abandon the run (cancellation/failure).
- Heartbeats (`Heartbeat`, the existing lease guard) continue during the local run so the lease
  covers the whole chain; `AdvanceRun` also renews it.
- The existing `Step.attempt()` / `Step` ambient context is set per local step as today.

The worker stays pull-based: it still initiates the claim and pushes every `AdvanceRun`; the
server never calls the worker.

## 9. Server changes (`WorkflowEngine` / `GrpcApi`)

- `GrpcApi.advanceRun(...)` → `engine.advanceRun(taskId, leaseOwner, steps, final)`.
- `engine.advanceRun(...)`: instance-locked transaction that folds the batch through the existing
  transition logic (reuse `mergeContext`, predicate routing, retry policy, and `drive()` for the
  final boundary). This is "apply N completes atomically, keep the lease, report status".
- `poll(...)`/`TaskActivation`: stamp the resolved `executionMode`.
- Reclaim (`reclaimExpiredLeases`): unchanged in mechanism — it always redispatches from the
  **last committed** token. That token is the last synced step (sync) or the last batch boundary
  (async), which is precisely what defines the crash blast radius per mode.

## 10. Durability & crash-replay contract (the important part)

Everything stays **at-least-once**. What changes per mode is how much re-executes after a worker
crash, and this MUST be documented loudly for users:

- `SERVER` / `LOCAL_SYNC`: each step is committed before the next runs. A crash re-runs **at most
  one** step (identical to today).
- `LOCAL_ASYNC`: an **unclean** death (kill -9, OOM, node loss) after executing steps *i..j* but
  before the batch `AdvanceRun` rewinds the server's view to *i-1*; on lease expiry, steps *i..j*
  **re-execute**. Blast radius = the whole local run. A **graceful** shutdown does not pay this
  cost: `Worker.close()` flips its running flag, and the in-flight `LocalRun` sees it at its next
  between-steps check and drains the buffer (`AdvanceRun` with a forced handback) before
  returning, instead of continuing to chain -- so a rolling deploy or scale-down loses nothing
  already computed (see `Worker.LocalRun.drainOnShutdown()`, implemented).

Implication: `LOCAL_ASYNC` steps must still be idempotent for the unclean-death case. Non-idempotent
side effects (charge a card, send an email) belong in `SERVER`/`LOCAL_SYNC`, or behind a
step-level idempotency key, or after a `.checkpoint()`. Documented in the DSL javadoc and README.

## 11. Feature interactions

- **Fork/join:** always boundaries → parallelism (the reason to distribute work) still fans out
  across workers via the server. A local run only ever spans a sequential region, so context merge
  stays trivial (no concurrent writers within a local run).
- **Retry/backoff:** a failed step that needs a delayed retry is a boundary (timer). An immediate
  in-line retry is out of scope for v1.
- **Sleep / user tasks:** boundaries by definition.
- **Cancellation:** observed at the next `AdvanceRun` via `instance_status` — immediate in
  `LOCAL_SYNC` (every step), best-effort mid-run in `LOCAL_ASYNC`. Document the weaker guarantee.
- **Multi-node fairness:** a worker grabs a whole linear run instead of single steps — coarser but
  fine, since a linear chain is inherently sequential; fork remains the distribution point.
- **Queues (`onQueue`):** a queue change is a boundary, so per-queue worker specialization is preserved.

## 12. Observability impact

In `LOCAL_ASYNC`, intermediate steps aren't in the DB until the batch lands, so the dashboard and
`QueueLagMonitor` see the instance "parked" mid-run. `LOCAL_SYNC` preserves near-real-time
visibility. Mitigations: document it; optionally have the worker emit a lightweight "in local run"
marker on the parked token; keep the default `SERVER` for anyone who needs step-level visibility.

## 13. Versioning & backward compatibility

- The mode is in `contentVersion()`, so adding/changing it mints a new version; running instances
  keep their pinned mode.
- **Old workers** (pre-feature) ignore `execution_mode` and use `CompleteTask` per step — they
  simply run any workflow in effective `SERVER` mode. A new worker that lacks the pinned version's
  blueprint falls back to `SERVER`. So mixed-version fleets stay correct, just not uniformly fast.
- **Old servers** don't implement `AdvanceRun`; a new worker detects the unimplemented RPC and
  falls back to `SERVER`. (gRPC returns `UNIMPLEMENTED`.)

## 14. Rollout plan

1. **Phase 0 — seam.** Extract `GraphTraversal` in `core`; refactor `drive()` to use it. No
   behaviour change; pure-function tests. (Low risk, valuable on its own.)
2. **Phase 1 — `LOCAL_SYNC`.** Add the enum, definition flag (+hash), `AdvanceRun` RPC, the
   `TaskActivation.execution_mode` field, and the client driver limited to sync. Zero durability
   regression, so this is the safe high-value slice.
3. **Phase 2 — `LOCAL_ASYNC`.** Add batching, cancellation-on-flush, chain-lease heartbeating, and
   the idempotency documentation. Ship behind the per-definition flag.
4. **Phase 3 — knobs.** ~~per-step `checkpoint()`~~ (done: forces an async flush after a step,
   committing it before the next; part of the content hash), plus still-to-do `maxSteps` /
   async flush cadence.

## 15. Testing plan

- `GraphTraversal` unit tests (successor resolution; every handback classification).
- Conformance parity: run the existing suite under `LOCAL_SYNC` and assert identical outcomes to
  `SERVER` (a linear pipeline, filter/gate, retry, fork/join boundaries, cancel).
- Crash-replay tests: kill a worker mid-run under each mode; assert `LOCAL_SYNC` re-runs one step
  and `LOCAL_ASYNC` re-runs the batch, both converging to the same final context (idempotent steps).
- Throughput check: linear N-step workflow, compare RPC/commit counts across the three modes.
- Backward-compat: new worker vs old server (`UNIMPLEMENTED` → fallback), old worker vs new server.

## 16. Open questions

- Should `FORK` be locally executable (run branches in-worker) when all branches share the
  worker's queue? Tempting for throughput but complicates merge and lease ownership — defer.
- Do we validate the reported path strictly (reject mismatches) or trust the worker? Proposal:
  validate, since the graph is cheap to consult and it guards against a buggy/rogue worker.
- `LOCAL_ASYNC` flush policy: at handback only, or also time/size-based mid-run? Start with
  handback-only; add cadence if runs get long.
- Metrics: expose local-run length / handback-reason counters for tuning `maxSteps`.
</content>
