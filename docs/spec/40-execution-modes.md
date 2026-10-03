# 40 — Execution modes

← [Engine semantics](30-engine.md) · [Index](00-index.md) · Next: [Sagas](50-sagas.md)

How a workflow's steps are driven. The mode is part of the definition — and of its fingerprint — so an
in-flight instance keeps the mode it started on. Three modes have the server dispatch work to a
worker; the fourth inverts it: the steps run inside the application and are reported after the fact.

## 1. The modes

**WGL-MODE-001** (MUST) The modes MUST be exactly `SERVER`, `LOCAL_SYNC`, `LOCAL_ASYNC`, `OBSERVED`
and `DEFAULT`.

| Mode | Who drives | Status writes | Crash blast radius | For |
|---|---|---|---|---|
| `SERVER` | server, one node per claim | per step | one step | anything non-idempotent |
| `LOCAL_SYNC` | worker chains locally | per step, before the next runs | one step (as `SERVER`) | most workflows — a safe speedup |
| `LOCAL_ASYNC` | worker chains and buffers | batched at handback | the whole batch re-runs | idempotent, throughput-critical |
| `OBSERVED` | the application itself | one append per report | n/a — nothing is dispatched | governing steps you already run |
| `DEFAULT` | resolves to `SERVER` | — | — | saying nothing, and taking the default |

**WGL-MODE-002** (MUST) `DEFAULT` MUST be a stable, hashable sentinel and MUST resolve to `SERVER`.
There is no server-wide execution-mode setting today ([WGL-OPS-004](90-ops.md)), so a workflow that
wants another mode says so with `executeIn*()`.

**WGL-MODE-003** (MUST) The server MUST resolve the effective mode and stamp the concrete value onto
every task activation. A worker MUST NOT re-derive it.

**WGL-MODE-004** (MUST) The mode MUST be part of the definition fingerprint, so changing it mints a new
version ([WGL-AUTH-102](10-authoring.md)).

**WGL-MODE-005** (MUST) The DSL MUST offer `executeInServer()`, `executeInLocalSync()` and
`executeInLocalAsync()` and nothing else: `OBSERVED` is stamped by an observer when it publishes, so a
spec cannot be handed to a worker in a mode no worker serves.

**WGL-MODE-006** (MUST) All four modes MUST be at-least-once. What differs is how much re-executes
after an unclean worker death.

## 2. `SERVER`

**WGL-MODE-010** (MUST) The server advances one node per claim: the worker executes exactly one step
and reports it, and the continuation is parked for the next poll rather than leased back.

**WGL-MODE-011** (MUST) A worker with no compiled graph for the task's `(workflow, version)` MUST fall
back to `SERVER` behaviour whatever the stamped mode says, because local chaining needs the graph to
traverse.

## 3. Local chaining (`LOCAL_SYNC`, `LOCAL_ASYNC`)

**WGL-MODE-020** (MUST) After executing a `TASK`/`PREDICATE`, a chaining worker MUST continue locally
**iff** the next node is a `TASK` or `PREDICATE` whose queue it serves; otherwise it MUST hand back.

**WGL-MODE-021** (MUST) The continue-or-hand-back decision MUST be the one pure function shared by the
server's state machine and the worker's driver (`GraphTraversal.classify`), with handback reasons
exactly `SLEEP`, `FORK` (fork or runtime fan-out), `JOIN`, `SIGNAL`, `SUB_WORKFLOW`, `OTHER_QUEUE`,
`TERMINAL`.

**WGL-MODE-022** (MUST) A worker MUST also hand back when: the step failed and needs a delayed retry;
its lease budget is nearly exhausted; a report says the instance is no longer `RUNNING`; or it is
draining for shutdown.

**WGL-MODE-023** (MUST) `LOCAL_SYNC` MUST commit every step before running the next, so its durability
is identical to `SERVER` while removing the re-poll round trip and the context re-shipping between
steps.

**WGL-MODE-024** (MUST) `LOCAL_ASYNC` MUST buffer up to `WorkerOptions.localBatchSize` steps (default
64) and report them in one call, and its steps MUST therefore be idempotent: an unclean death rewinds
the server's view to the last committed step and the whole buffered run re-executes.

**WGL-MODE-025** (MUST) `.checkpoint()` on a step MUST force a `LOCAL_ASYNC` flush after it, committing
it before the chain continues. The checkpoint set is part of the fingerprint.

**WGL-MODE-026** (MUST) A mid-run failure MUST flush the successful prefix and then fail the offending
step's token.

**WGL-MODE-027** (MUST) A graceful `close()` MUST drain the buffer rather than continue chaining, so a
rolling deploy or scale-down loses nothing already computed.

**WGL-MODE-028** (MUST) Cancellation is observed at the next report: immediately under `LOCAL_SYNC`
(every step), best-effort mid-run under `LOCAL_ASYNC`.

**WGL-MODE-029** (MUST) Under `LOCAL_ASYNC`, intermediate steps are not in the database until the batch
lands, so the console and the lag monitor see the instance parked mid-run. `LOCAL_SYNC` preserves
step-level visibility.

*Verified by:* `tests/LocalSyncTest`, `tests/LocalBoundaryTest`, `tests/CheckpointTest`,
`tests/GraphTraversalTest`, `tests/GracefulShutdownTest`, `server/engine/LocalBatchBench`.

### 3.1 Batched reports across instances

**WGL-MODE-030** (MUST) A report carrying several runs MUST be admitted only where batching is sound.
Each of these MUST refuse **that run alone**, leaving the rest of the batch to apply:

| Refusal | Status |
|---|---|
| the task id is unknown | 404 |
| another run in the same batch already advances that instance | 409 |
| the instance is held by a concurrent operation, or gone | 409 |
| the lease owner does not match | 409 |
| the first reported step does not name the token's node | 409 |
| the instance is a sub-workflow of another instance | 409 |
| the workflow's mode is not `LOCAL_ASYNC` | 409 |

**WGL-MODE-031** (MUST) An instance that is no longer `RUNNING` MUST NOT be a refusal: the run reports
the instance's status so the worker stops.

**WGL-MODE-032** (MUST) Surviving runs in one call MUST commit together, and the answers MUST come back
one per submitted run, in submission order.

**WGL-MODE-033** (MUST) A single-run report MUST carry no batching restriction — there is nothing to
batch it with.

*Verified by:* `server/engine/AdvanceManyTest`, `tests/CrossInstanceBatchTest`,
`server/engine/AdvanceManyBench`.

## 4. `OBSERVED`

### 4.1 What it is

**WGL-OBS-001** (MUST) In `OBSERVED` mode the server MUST dispatch nothing, lease nothing to a worker,
and never block the reporting application. The steps run on the application's own threads and are
reported afterwards with their own timings.

**WGL-OBS-002** (MUST) An observed graph MAY contain only `TASK`, `PREDICATE`, static `FORK`, `JOIN`
and `END` nodes, and MUST NOT contain compensable steps. Registration MUST refuse anything else with
400: a sleep, signal, sub-workflow or runtime fan-out needs the server to run it, and there is no
server-side run for something that already happened.

**WGL-OBS-003** (MUST) Reporting against a workflow whose resolved mode is not `OBSERVED` MUST be
refused with 400, and observing MUST be refused for a mode whose steps workers report.

### 4.2 Run identity

**WGL-OBS-010** (MUST) A run is identified by a **correlation key**. The instance id MUST be derived
from `(workflow, key)`, so every reporter of a run lands on the same instance whichever reports first.

**WGL-OBS-011** (MUST) Two reporters creating one run concurrently MUST collide on the primary key, and
the loser MUST read the winner's row. When the winner's row is not yet visible, the call MUST be
refused as a conflict the reporter may retry.

**WGL-OBS-012** (MUST) A blank key MUST mint a random id; such a run is single-reporter by construction.

**WGL-OBS-013** (MUST) A run's first report MUST create the instance and MUST append a `wf.started`
event.

### 4.3 Appending reports

**WGL-OBS-020** (MUST) A report MUST only append. Each reported step becomes a **settled, timed token**
carrying its reporter as lease owner and a monotonic arrival sequence, so arrival order breaks ties
between steps whose clocks agree to the millisecond.

**WGL-OBS-021** (MUST) A reported step naming a node the graph does not have (or one that is not
worker-dispatched) MUST be recorded as `UNKNOWN_NODE` at once and skipped.

**WGL-OBS-022** (MUST) Steps reported once the instance is terminal MUST be recorded as `AFTER_END`,
and their tokens MUST still be kept for their timings.

**WGL-OBS-023** (MUST) A step reported with an error MUST be settled `FAILED` and MUST mark the run
**closing**, but MUST NOT fail the run at once — another service's earlier steps may still be in
flight and belong to this run rather than after it.

**WGL-OBS-024** (MUST) A step whose successor (by its predicate value, where applicable) is `END` MUST
also write the `END` token and mark the run closing.

**WGL-OBS-025** (MUST) A task report's `merge` MUST be applied to the run's context.

### 4.4 Settling

**WGL-OBS-030** (MUST) Every report MUST push the run's settle time out by the stall threshold
(`WIGGLE_OBSERVE_STALL_MILLIS`, default 10 minutes).

**WGL-OBS-031** (MUST) Reaching `END`, or a report marked `final`, MUST pull the settle time in to a
short grace (`WIGGLE_OBSERVE_SETTLE_MILLIS`, default 5 s) so stragglers from other services still
land.

**WGL-OBS-032** (MUST) Closing MUST be sticky: once `END` was seen or a report said `final`, a
straggler keeps the short grace rather than pushing the run back out to the stall threshold.

**WGL-OBS-033** (MUST) The leader's housekeeping tick MUST judge runs whose settle time has passed.

### 4.5 Judgement

**WGL-OBS-040** (MUST) Judgement MUST be a pure function of the run's steps and the graph, so the same
verdict can be replayed offline over any day of runs.

**WGL-OBS-041** (MUST) The judge MUST sort the run's steps by the reporter's clock and walk a
**frontier** — the set of steps the graph expects next — where: a step in the frontier consumes it and
releases its successors; a predicate's value picks the branch; a fork releases every branch head; a
join releases its successor once every branch has arrived; `END` closes the run.

**WGL-OBS-042** (MUST) Two branches whose steps interleave in time MUST both be in the frontier, so
fan-out across services raises nothing.

**WGL-OBS-043** (MUST) A step outside the frontier MUST be `DUPLICATE` when it was already consumed and
lies on no cycle, and `OUT_OF_ORDER` otherwise; after an `OUT_OF_ORDER` finding the frontier MUST
resynchronise to that step's successors so the rest of the run still judges.

**WGL-OBS-044** (MUST) The anomaly kinds MUST be exactly:

| Kind | Raised when | Effect |
|---|---|---|
| `UNKNOWN_NODE` | the graph has no such step (at arrival) | step skipped |
| `AFTER_END` | steps arrive once the instance is terminal (at arrival) | tokens kept for their timings |
| `DUPLICATE` | a consumed step runs again and lies on no cycle | ignored; at-least-once delivery |
| `OUT_OF_ORDER` | a step outside the frontier, not a duplicate | frontier resynchronised there |
| `INCOMPLETE` | judged without reaching `END` | instance `FAILED` ("run ended before END, at …") |
| `STALLED` | judged because the run went quiet, not because `END` or `final` arrived | recorded with `INCOMPLETE` |

**WGL-OBS-045** (MUST) An anomaly MUST be a **finding, not a refusal**: a departure is recorded and the
report still applies.

**WGL-OBS-046** (MUST) The verdict MUST be: a successful `END` reached ⇒ `COMPLETED`, whatever was
recorded along the way; a failing `END` ⇒ `FAILED` with its reason; a step that threw ⇒ `FAILED` with
`"<step>: <error>"`; no `END` ⇒ `FAILED` as incomplete.

**WGL-OBS-047** (MUST) An anomaly record MUST carry instance, workflow, version, kind, the expected
node, the reported node, a detail string and a timestamp, and MUST be listable newest first, narrowed
by workflow and/or instance.

*Verified by:* `server/engine/ConformanceTest`, `tests/ObservedModeTest`, `tests/ObserveApiTest`.

### 4.6 Timing statistics

**WGL-OBS-050** (MUST) Every execution mode MUST feed the same per-step statistics: the handler's own
`startedAt`/`finishedAt` as reported. The server's claimed-to-settled stamps MUST stand in only for a
worker that reports none.

**WGL-OBS-051** (MUST) Statistics MUST be per node: count, mean, p50, p95 and max duration, computed in
the server over the newest N timed settled tokens of one workflow version (default 10 000), so no
percentile SQL has to be portable.

**WGL-OBS-052** (MUST) Worker-run steps MUST additionally carry **queue wait** (ready → claimed,
measured by the server) as p50/p95, so a slow step is distinguishable from a starved one. A step a
local worker chained without a round trip, and an observed step, were never queued and MUST report
zero wait.

**WGL-OBS-053** (MUST) Graph order MUST NOT be implied by the statistics response; a bottleneck view
sorts by p95.

*Verified by:* `tests/StepTimingTest`.

### 4.7 The reporting module (`sh.wiggle:wiggle-observe`)

**WGL-OBS-060** (MUST) The reporter MUST be a separate module from the client: an observed service
publishes a topology and reports against it, and needs neither a worker nor the instance API.

**WGL-OBS-061** (MUST) `Observer.publish(spec)` MUST stamp `OBSERVED` on the definition, register it,
index its worker-dispatched nodes by name, and refuse a spec that named a worker mode.

**WGL-OBS-062** (MUST) The handle MUST offer `record(key, step, startedAt, finishedAt)`,
`recordPredicate(key, step, value, startedAt, finishedAt)`,
`recordError(key, step, error, startedAt, finishedAt)`, `end(key)` and
`start(key, step)` returning a timer that measures for the caller.

**WGL-OBS-063** (MUST) Step names MUST be validated against the published spec at the call site, so a
typo fails in the service rather than arriving as an anomaly. The wire report carries node ids.

**WGL-OBS-064** (MUST) Reporting MUST NOT block the caller: reports queue and travel in batches on one
flusher thread. A full queue or a failed call MUST drop the report and count it in `dropped()`.

**WGL-OBS-065** (MUST) Several services MAY report steps of one run under the same key, in any order.

**WGL-OBS-066** (MUST) `ObserverOptions` MUST configure the reporter name (`host@pid` by default), the
linger, the queue capacity, and TLS with the client's semantics.

**WGL-OBS-067** (MUST) `end(key)` states that the originator is done; a run still short of `END` is then
judged incomplete.

*Verified by:* `tests/ObserveApiTest`, `tests/ObservedModeTest`.
