# 30 — Engine semantics

← [Worker contract](20-worker.md) · [Index](00-index.md) · Next: [Execution modes](40-execution-modes.md)

The engine is one instance state machine with many concurrent token state machines beneath it. The
instance owns "is this workflow still going"; each token owns one unit of execution moving over the
graph. This chapter specifies both machines, what each node kind does when a token reaches it, how
context scoping works under fan-out, and every path that fails an instance.

The tables in §2 and §3 mirror `docs/state-machines.md`, which is generated from the two state enums
and held to them by `StateChartTest`.

## 1. Model

**WGL-ENG-001** (MUST) An instance MUST be persisted state, not a call stack: one instance row plus
token rows marking where execution is on the compiled graph.

**WGL-ENG-002** (MUST) Handler code MUST NOT be re-executed to reconstruct state. There is no replay,
so there is no determinism discipline imposed on handlers.

**WGL-ENG-003** (MUST) Every mutation for one instance MUST be serialised by an instance write-lock
held for the remainder of its transaction (`Tx.lockInstance`), which is what lets several server
nodes drive the same instance concurrently.

**WGL-ENG-004** (MUST) A state **answers** and a lifecycle **writes**: `InstanceState` and
`TokenState` take no transaction and perform no write; `Instances` and `Tokens` perform every write
and MUST ask the state first. A transition no state permits MUST throw rather than be persisted.

**WGL-ENG-005** (MUST) The parent MUST NOT recompute itself from its children: a token *arriving* at a
node fires the instance transition, and the aggregate ("no token still active") appears only as a
guard on it.

**WGL-ENG-006** (MUST) `READY → RUNNING` is the one transition performed by the store, because a
claim must be atomic with the select that finds the token.

*Verified by:* `server/engine/StateChartTest`, `server/store/StatusRulesTest`.

## 2. Instance states

**WGL-ENG-010** (MUST) The instance statuses and their classification MUST be exactly:

| Status | Class | Meaning |
|---|---|---|
| `RUNNING` | live, forward | Born here, with one token at the start node. |
| `COMPENSATING` | live, reverse | The saga reverse pass owns it; undo tasks are still dispatched. |
| `COMPLETED` | terminal | A token reached a successful END and nothing was left running. |
| `FAILED` | terminal | Unrecoverable, with nothing recorded to undo. |
| `CANCELLED` | terminal | Cancelled by a caller. Never compensates. |
| `COMPENSATED` | terminal | The reverse pass undid every recorded step. |
| `COMPENSATION_FAILED` | terminal | A compensator ran out of retries. Stuck, and deliberately loud. |

**WGL-ENG-011** (MUST) `live()` is "not terminal"; `running()` is `RUNNING` only; `compensating()` is
`COMPENSATING` only. A status name this build does not recognise MUST NOT be treated as live.

**WGL-ENG-012** (MUST) The instance transitions MUST be exactly:

| From | Event | To | Guard |
|---|---|---|---|
| — | `START` | `RUNNING` | the workflow version resolves |
| — | `START_SUB_WORKFLOW` | `RUNNING` | a `SUB_WORKFLOW` token spawned it; `parentTokenId` links them |
| — | `SCHEDULE_DUE` | `RUNNING` | the fire-time compare-and-set won |
| — | `TRIGGER_FIRED` | `RUNNING` | the dispatch-position compare-and-set won |
| `RUNNING` | `TOKEN_REACHED_SUCCESSFUL_END` | `COMPLETED` | no token of the instance is still active |
| `RUNNING` | `UNRECOVERABLE_FAILURE` | `FAILED` | the comp-log holds no uncompensated entry |
| `RUNNING` | `UNRECOVERABLE_FAILURE` | `COMPENSATING` | the comp-log holds an uncompensated entry |
| `RUNNING` | `CANCEL_REQUESTED` | `CANCELLED` | still `RUNNING`; a terminal instance ignores it |
| `COMPENSATING` | `COMPENSATOR_COMPLETED` | `COMPENSATING` | another uncompensated entry remains; the next undo is dispatched |
| `COMPENSATING` | `COMPENSATOR_COMPLETED` | `COMPENSATED` | no uncompensated entry remains |
| `COMPENSATING` | `COMPENSATOR_EXHAUSTED` | `COMPENSATION_FAILED` | a compensator ran out of retries |

## 3. Token states

**WGL-ENG-020** (MUST) The token statuses MUST be exactly:

| Status | Class | Meaning |
|---|---|---|
| `READY` | active | Dispatchable. A retry waits here too, behind `availableAt`. |
| `RUNNING` | active | Leased; implies a non-null lease owner and a future expiry. |
| `WAITING` | active | Parked on the clock until `availableAt`. |
| `AWAITING` | active | Parked on an external actor: a signal, or a child instance. |
| `JOINED` | active | Parked at a join barrier, waiting on its siblings. |
| `DONE` | settled | Consumed: a completed step, a spent fork, a satisfied barrier. |
| `FAILED` | settled | Out of retries, or waiting on something that can no longer arrive. |
| `CANCELLED` | settled | Abandoned because the instance stopped running. |

**WGL-ENG-021** (MUST) The token transitions MUST be exactly:

| From | Event | To | Guard |
|---|---|---|---|
| — | `MINT` | `READY` | a continuation, a fork branch, or the start node |
| — | `CHAIN` | `RUNNING` | the reported run continues locally; leased straight back, never polled |
| `READY` | `DRIVE_TASK` | `READY` | `TASK`/`PREDICATE`; sets the queue and wakes pollers post-commit |
| `READY` | `DRIVE_SLEEP` | `WAITING` | `SLEEP` |
| `READY` | `DRIVE_SIGNAL` | `AWAITING` | `SIGNAL`; `availableAt` carries the optional deadline |
| `READY` | `DRIVE_SUB_WORKFLOW` | `AWAITING` | `SUB_WORKFLOW`; the child starts in the same transaction |
| `READY` | `DRIVE_JOIN` | `JOINED` | `JOIN`, barrier not yet satisfied |
| `READY` | `DRIVE_FORK` | `DONE` | `FORK`/`DYN_FORK`; branches minted, this token spent |
| `READY` | `DRIVE_END` | `DONE` | `END` |
| `READY` | `POLL_CLAIMED` | `RUNNING` | `availableAt` passed and the instance is `RUNNING` (`COMPENSATING` for a compensator) |
| `RUNNING` | `STEP_REPORTED` | `DONE` | the lease matches; the continuation is minted and driven |
| `RUNNING` | `TASK_FAILED` | `READY` | retryable and `attempt < maxAttempts`; `availableAt = now + backoff` |
| `RUNNING` | `TASK_FAILED` | `FAILED` | not retryable, or attempts exhausted |
| `RUNNING` | `LEASE_EXPIRED` | `READY` | same retry policy as an explicit failure; the attempt is spent |
| `RUNNING` | `LEASE_EXPIRED` | `FAILED` | attempts exhausted |
| `WAITING` | `TIMER_DUE` | `DONE` | `availableAt` passed and the instance is `RUNNING` |
| `AWAITING` | `SIGNAL_DELIVERED` | `DONE` | the name matches and the instance is `RUNNING` |
| `AWAITING` | `SIGNAL_DEADLINE` | `DONE` | the deadline passed; continues at `altNext`, or the instance fails |
| `AWAITING` | `SUB_WORKFLOW_TERMINAL` | `DONE` | the child `COMPLETED`; its context merges back into the parent's scope |
| `AWAITING` | `SUB_WORKFLOW_TERMINAL` | `FAILED` | the child ended any other way; the parent fails with it |
| `JOINED` | `BARRIER_SATISFIED` | `DONE` | every expected sibling arrived; one continuation is minted for them all |
| `READY`/`RUNNING`/`WAITING`/`AWAITING`/`JOINED` | `CANCEL_PROPAGATED` | `CANCELLED` | the instance stopped running |

**WGL-ENG-022** (MUST) A retry MUST NOT be a distinct state: it is `READY` with `availableAt` in the
future, indistinguishable from a freshly minted token except in time.

**WGL-ENG-023** (MUST) Retries MUST be internal to the token; only exhaustion crosses the boundary to
the instance.

## 4. The drive pump

**WGL-ENG-030** (MUST) Starting an instance MUST resolve the version (explicit, else latest — a
missing workflow is a 404), write the instance row, mint one token at the start node, and drive it to
its first parking place, all in one transaction.

**WGL-ENG-031** (MUST) The drive pump MUST advance each token over server-side nodes until it parks or
the instance ends.

**WGL-ENG-032** (MUST) The pump MUST cap chain **depth**, not fan-out breadth: the budget starts at
10 000 advances and grows by one for every extra child a fan-out enqueues, so any fan-out width
advances while a runaway chain of server-side nodes still trips the cap with
`"drive budget exceeded"`.

**WGL-ENG-033** (MUST) The same pump MUST serve the engine's own sweeps and every execution mode, so
the runaway guard has one definition.

## 5. Per-node behaviour

**WGL-ENG-040** (MUST) `TASK` / `PREDICATE` MUST park the token `READY`, stamped with the node's
queue, and MUST wake local pollers after commit. On report, a task applies the result to the token's
current scope and continues at `next`; a predicate continues at `next` or `altNext` by the reported
boolean.

**WGL-ENG-041** (MUST) `SLEEP` MUST park the token `WAITING` until `now + sleepMillis`. No worker is
held.

**WGL-ENG-042** (MUST) `FORK` MUST spend its own token, push a join-stack frame naming the group (the
fork token's id), and mint one token per branch start, each carrying an `ARM` frame whose view is a
copy of the pre-fork view and whose index is the arm's position.

**WGL-ENG-043** (MUST) `DYN_FORK` MUST read the collection at `itemsKey` from the token's current
view and:

- fail the instance when the value is neither a list, a map, nor absent;
- iterate a map's values, remembering each entry's key as the item's map key;
- mint one token per element, each carrying an `ITEM` frame whose view **is the element** and whose
  index is its position, all sharing a group of `"<forkTokenId>#<width>"`;
- when the collection is empty or absent, skip the whole construct — continue at the combine's
  successor if the join is followed by a combine, else at the join's successor.

**WGL-ENG-044** (MUST) `JOIN` MUST mark the arriving token `JOINED`, count the tokens parked at that
barrier in the same group, and do nothing further until the expected width arrives. The width is the
node's `expected` when positive, else the `#n` suffix parsed from the group.

**WGL-ENG-045** (MUST) On satisfaction the join MUST settle every token at the barrier and mint **one**
continuation whose payload is restored from the fork token's own payload — which never held the frame
— so the frame is popped by restoration rather than mutation.

**WGL-ENG-046** (MUST) When the join's successor is a combine, the continuation's payload MUST carry
the staged inputs: for a fork, each arm's final frame view under `__arm__<armName>` keyed by arm
position; for a forEach, the item views ordered by item index as a list, or keyed by source map key as
a map, under the node's `collectKey`.

**WGL-ENG-047** (MUST) `SIGNAL` MUST park the token `AWAITING` under the signal's name, with the
optional deadline in `availableAt`.

**WGL-ENG-048** (MUST) `SUB_WORKFLOW` MUST park the parent token `AWAITING` and start the child in the
**same transaction**, passing the parent token's dispatch context, a correlation id of
`"sub:<parentTokenId>"`, and the parent token id. An unregistered child workflow MUST fail the parent.

**WGL-ENG-049** (MUST) `END` MUST spend its token; an unsuccessful END fails the instance with its
reason, and a successful one completes the instance only when no token of the instance is still active
and no work remains queued in this drive pass.

*Verified by:* `tests/ForkIsolationTest`, `tests/ForkJoinContextMergeTest`,
`tests/DynamicConstructsTest`, `tests/SubFlowTest`, `tests/SignalTest`,
`tests/JdbcForkJoinStressTest`, `server/engine/ForkWidthBench`.

## 6. Context scoping

**WGL-ENG-050** (MUST) A token carries a nesting stack that mirrors its join stack frame for frame: a
fork pushes an `ARM` frame, a runtime fan-out an `ITEM` frame.

**WGL-ENG-051** (MUST) The **top** frame's view is the token's complete current context; with an empty
stack, the shared instance context is that view.

**WGL-ENG-052** (MUST) Only the top frame is ever written. Frames beneath it are copies frozen at
spawn time, which is what makes an item's base context stable.

**WGL-ENG-053** (MUST) Fork, forEach and `repeatWhile` MUST nest to any depth in any order, because
depth is a list.

**WGL-ENG-054** (MUST) A branch's writes MUST be invisible to its siblings and MUST NOT touch the
shared context. A combine is the only path by which a branch result reaches the flow.

**WGL-ENG-055** (MUST) A step result MUST **replace** the token's current scope view, with top-level
nulls dropped; a null return leaves the context untouched.

**WGL-ENG-056** (MUST) A combine's return MUST be taken verbatim (an arm name may legitimately double
as a data key), with only the engine's reserved keys removed.

**WGL-ENG-057** (MUST) The staged combine inputs MUST be stripped from the payload once the combine
node has run.

**WGL-ENG-058** (MUST) The only merges in the engine are external inputs folding back: a delivered
signal's payload and a completed sub-workflow's final context. A map merges key by key (a null value
deletes its key); any other value replaces the view wholesale.

**WGL-ENG-059** (MUST) The context a worker is dispatched is the token's current scope view overlaid
with any staged combine inputs. When that view is not an object (a scalar item) and staged inputs
exist, the staged inputs are dispatched alone and the view rides on the activation's base context.

*Verified by:* `tests/NestedScopesTest`, `tests/ForkIsolationTest`,
`client/flow/DeepNestingTest`, `server/store/PayloadCodecTest`.

## 7. Dispatch and leases

**WGL-ENG-060** (MUST) A poll MUST claim the **oldest** `READY` tokens first, filtered by the worker's
served queues and — when the worker is version-scoped — by `(workflow, version)`, and MUST NOT claim a
token whose `availableAt` is in the future.

**WGL-ENG-061** (MUST) A token MUST only be claimable while its instance is `RUNNING`, or
`COMPENSATING` for a compensation token.

**WGL-ENG-062** (MUST) A claim MUST stamp a lease owner and expiry. Only the lease owner may report,
fail, or heartbeat that token.

**WGL-ENG-063** (MUST) Dispatch MUST be exactly-once and execution at-least-once: a lost worker's lease
expires and the token becomes claimable again, with the attempt spent.

**WGL-ENG-064** (MUST) Repeated delivery MUST be conflict-safe rather than idempotent: a second report
or failure for the same token fails its lease check and is rejected as a conflict (409).

**WGL-ENG-065** (MUST) A report MUST validate each reported node id against the token's position and
refuse a mismatch.

**WGL-ENG-066** (MUST) A heartbeat MUST extend the lease of a `RUNNING` token owned by the caller and
MUST return the new expiry.

**WGL-ENG-067** (MUST) A long poll MUST be woken by same-node production (wake-on-produce) and MUST
also fall back to periodic re-claims so that cross-node production is discovered within a bounded
delay.

**WGL-ENG-068** (MAY) After a wake-on-produce signal the server MAY linger briefly before claiming, so
a burst is drained in one batched claim; the linger applies only when the worker asked for more than
one task.

**WGL-ENG-069** (MUST) The server MUST record which queues and `(workflow, version)` pairs its current
pollers serve, for backlog coverage ([WGL-OPS-070](90-ops.md)). That registry is in memory, per node,
and MUST NOT be durable.

*Verified by:* `tests/CompetingConsumersTest`, `server/engine/WakeOnProduceTest`,
`server/engine/DispatchNotifierTest`, `postgres/PostgresClaimTest`, `tests/BacklogCoverageTest`.

## 8. Signals

**WGL-ENG-070** (MUST) A signal is delivered by `(instanceId, name)`. The instance MUST already be
waiting on that name; there is no buffering, and an early delivery MUST be refused as a conflict
(409) the sender may retry.

**WGL-ENG-071** (MUST) A delivered payload MUST merge into the waiting token's current scope, and the
flow MUST continue down the signal node's `next` edge.

**WGL-ENG-072** (MUST) A signal deadline that passes MUST continue at `altNext` when one exists
(escalation), and MUST otherwise fail the instance with `"signal '<name>' timed out"`.

**WGL-ENG-073** (MUST) Signal delivery MUST be refused when the instance is not `RUNNING`.

*Verified by:* `tests/SignalTest`.

## 9. Schedules and triggers

**WGL-ENG-080** (MUST) A schedule fires a workflow either on a fixed interval or on a five-field cron
expression.

**WGL-ENG-081** (MUST) Cron MUST support `*`, values, lists, ranges and steps; day-of-week accepts
0–7 with both 0 and 7 as Sunday; when day-of-month and day-of-week are both restricted a day matches
if **either** does (vixie-cron rule).

**WGL-ENG-082** (MUST) Cron fire times MUST be evaluated in **UTC**, so every node in a cluster
computes the same schedule regardless of its local zone.

**WGL-ENG-083** (MUST) A malformed cron expression MUST be rejected with 400; a non-positive interval
MUST be rejected with 400.

**WGL-ENG-084** (MUST) Schedule creation MUST be an **upsert keyed on workflow name**: a workflow has
at most one schedule, so repeated creation from any number of clients updates it in place.

**WGL-ENG-085** (MUST) Firing MUST be leader-driven and exactly-once per due time, guarded by a
compare-and-set on the next fire time, so a failover cannot double-fire.

**WGL-ENG-086** (MUST) A fired schedule MUST start an ordinary instance, correlated to its schedule.

*Verified by:* `tests/ScheduleTest`, `tests/CronTest`, `tests/ScheduleClientTest`.

A trigger starts a workflow on another instance's event: when an instance of the trigger's source
appends one of its event types to the [event log](60-event-log.md), the target workflow starts.

**WGL-ENG-130** (MUST) A trigger MUST name a target workflow, a source workflow or `*`, at least one
event type, and whether the new instance begins with the source's context. Creation MUST be an
**upsert keyed on (workflow, source)**. An unregistered target MUST be refused with 404. A `wf.` type
that is not one of the lifecycle types ([WGL-EVT-002](60-event-log.md#1-what-is-written)), an empty
list, a type holding `,` or longer than 32 characters, and a source equal to the target MUST be
refused with 400. Any other type names a handler-emitted event.

**WGL-ENG-131** (MUST) A trigger MUST fire only on events appended after it was first created. `*`
MUST match every workflow except the trigger's own target.

**WGL-ENG-132** (MUST) Dispatch MUST be leader-driven and read each instance shard's event log behind
that shard's own dispatch position, held on that shard, honouring the feed's visibility window
([WGL-EVT-026](60-event-log.md#3-reading-pull-and-ack)). The position MUST exist on every instance
shard before the trigger row is written, and MUST be dropped when the last trigger is deleted.

**WGL-ENG-133** (MUST) Moving a shard's position and starting the instances its events owe MUST
share one transaction on that shard, guarded by a compare-and-set on the position, so each event
fires each matching trigger exactly once, even across a two-leader overlap.

**WGL-ENG-134** (MUST) A triggered instance MUST start on its source's shard, correlated
`"trigger:<triggerId>:<sourceInstanceId>"`. Its context MUST be the source's context as it stands at
dispatch when the trigger asks for it, else empty, with a `trigger` object added naming the trigger,
the event type, its seq and payload, the source instance, workflow, version and correlation id, and
the chain depth.

**WGL-ENG-135** (MUST) The chain depth is the number of triggered starts leading to an instance,
followed back through the correlation ids. A start that would exceed 16 MUST be skipped and logged,
so a cycle of triggers ends.

**WGL-ENG-136** (MUST) An event whose start throws, or whose target is no longer registered, MUST be
logged and skipped; it MUST NOT hold back the shard's other events.

*Verified by:* `tests/TriggerTest`, `tests/TriggerJdbcTest`, `tests/TriggerClientTest`,
`server/engine/ShardedEngineTest`.

## 10. Cancellation

**WGL-ENG-090** (MUST) `cancel(instanceId, reason)` MUST take the instance write-lock, settle every
active token `CANCELLED`, and set the instance `CANCELLED` with that reason. A terminal instance MUST
ignore it.

**WGL-ENG-091** (MUST) Cancellation MUST cascade to sub-workflow children, each in its own transaction
so lock ordering stays one-way (child → parent only). A failed cascade MUST be logged and MUST NOT
fail the parent's cancellation.

**WGL-ENG-092** (MUST) Cancellation MUST NOT run compensators.

**WGL-ENG-093** (MUST) Cancellation MUST be structural rather than a priority rule: every other
transition re-reads under the same instance lock and bails, so a cancelled token is no longer
`RUNNING` and a racing lease renewal fails its lease check.

*Verified by:* `tests/SubFlowTest`, `server/engine/TerminalInvariantTest`.

## 11. Failure paths

**WGL-ENG-100** (MUST) Exactly these paths MUST raise the instance failure event:

- a token exhausted its retry policy, whether reported or through an expired lease;
- a loop guard exceeded its `repeatWhile` budget;
- a token reached an `END` marked unsuccessful;
- a runtime fan-out found something other than a list or map at its `itemsKey`;
- a `SUB_WORKFLOW` node named a workflow that is not registered;
- a sub-workflow instance ended in any state but `COMPLETED`;
- a signal deadline passed on a node with no `altNext`.

**WGL-ENG-101** (MUST) A gate returning `false` MUST NOT be a failure: the instance ends
`COMPLETED` (or the branch short-circuits to its join), and nothing is recorded as an error.

**WGL-ENG-102** (MUST) A `repeatWhile` guard MUST have a budget: the node's own when declared, else
the server default (`WIGGLE_LOOP_MAX_ITERATIONS`, default 10 000). Exceeding it MUST fail the instance
with a message naming the loop and how to raise the budget.

**WGL-ENG-103** (MUST) A lease expiring and a step being dispatched twice MUST NOT be treated as
errors; they are the recovery and at-least-once guarantees working.

**WGL-ENG-104** (MUST) When a failing instance has uncompensated comp-log entries, it MUST enter
`COMPENSATING` instead of `FAILED` ([chapter 50](50-sagas.md)).

*Verified by:* `tests/ErrorHandlingTest`, `tests/LoopBudgetTest`, `server/cluster/HousekeeperTest`.

## 12. Invariants

**WGL-ENG-110** (MUST) A terminal instance MUST have no active token.

**WGL-ENG-111** (MUST) A `RUNNING` token MUST imply a non-null lease owner and a future expiry;
settling, retrying and cancelling all clear both.

**WGL-ENG-112** (MUST) "Terminal" MUST mean "neither `RUNNING` nor `COMPENSATING`", and every place
that encodes it (the view type, the JDBC purge query, the in-memory purge) MUST agree.

**WGL-ENG-113** (MUST) `LOCAL_SYNC` and `LOCAL_ASYNC` MUST NOT be a third state machine: every worker
reports through the same entry point and a run of steps walks the same transitions one at a time.

*Verified by:* `server/engine/TerminalInvariantTest`, `tests/ManyWorkflowsStateSweepTest`,
`server/engine/StateChartTest`.

## 13. Leader duties

**WGL-ENG-120** (MUST) Exactly one node per cell MUST run the clock-driven duties, each bounded by a
batch size per tick: fire due timers, reclaim expired leases, fire due signal deadlines, fire due
schedules, dispatch triggers.

**WGL-ENG-121** (MUST) A separate, slower sweep MUST purge terminal instances older than the retention
window and trim the event log.

**WGL-ENG-122** (MAY) Adaptive housekeeping MAY re-run a sweep immediately when it filled its batch
(drain mode), which removes the batch ÷ tick promotion ceiling under backlog. Because the signal is
batch fullness, an idle system MUST NOT loop.

**WGL-ENG-123** (MUST) Leader duties MUST be idempotent and re-entrant, and leader-guarded writes MUST
be compare-and-set, so a brief two-leader overlap during failover duplicates work but cannot corrupt
state.

*Verified by:* `server/cluster/HousekeeperTest`, `election/LeaderElectionTest`.
