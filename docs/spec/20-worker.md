# 20 — Worker contract

← [Authoring](10-authoring.md) · [Index](00-index.md) · Next: [Engine semantics](30-engine.md)

A worker is the data plane: it binds handlers, pulls work it has capacity for, runs it, and reports.
It holds no durable state and needs no inbound connectivity. This chapter specifies how handlers are
bound to graph nodes, how a task is claimed and reported, and what a worker must do with each answer.

## 1. Binding handlers

### 1.1 Declaration

**WGL-WRK-001** (MUST) A handler set is a class annotated `@ForFlow("<workflow name>")`; its public
instance methods implement that workflow's steps. `Worker.registerHandler(flowName, object)` MAY
supply the workflow name instead, and MUST be required when the annotation is absent.

**WGL-WRK-002** (MUST) A method binds the node whose name matches its own under canonical folding
(`inStock` ↔ `in-stock`), unless `@Handles("node-name")` names a node explicitly, which MUST win.

**WGL-WRK-003** (MUST) Two methods that fold to the same name MUST be rejected at scan time as
ambiguous.

**WGL-WRK-004** (MUST) A method's **signature defines the step kind**:

| Signature | Step kind | Semantics |
|---|---|---|
| one parameter, `boolean` return | gate / guard (`PREDICATE`) | selects `next` (true) or `altNext` (false) |
| one parameter, `void` return | effect (`TASK`) | context flows on untouched |
| one parameter, any other return | task (`TASK`) | the return **replaces** the context |
| named like a fork's combine node | fork combine | each parameter found by type ([WGL-DYN-083](35-dynamic-flows.md#8-parameters-by-type)) |
| named like a forEach's combine node | forEach combine | collections of results, `List` ordered by item index or `Map` keyed as the input was, and the base, found by type ([WGL-DYN-084](35-dynamic-flows.md#8-parameters-by-type)) |
| zero arguments returning `Activity`/`GateActivity`/`EffectActivity`/`CompensableActivity` | typed activity factory | the factory's name is the step it serves |

**WGL-WRK-005** *Withdrawn with `@Context`.* A combine takes the pre-fork (or pre-forEach) context as
a parameter no result matches ([WGL-DYN-083](35-dynamic-flows.md#8-parameters-by-type)), and any
other step reads it through `Step.base()`.

**WGL-WRK-006** (MUST) A combine's return is the **complete post-join context**. There is no default
fold, so every combine node MUST have an explicit handler on some worker.

**WGL-WRK-007** (MUST) A method annotated `@Decode` is not a step but a decoder for its return type:
it takes the raw persisted JSON (a `Map`) and returns the typed object, and MUST run instead of the
default reflective mapping wherever a parameter of that type is bound.

**WGL-WRK-008** (MUST) The default decode MUST be lenient by field name: a missing field arrives as
null and an unknown field is dropped.

*Verified by:* `client/…/HandlerBinderTest`, `client/…/TypedActivityTest`,
`tests/RegisterHandlersTest`, `tests/RecordContextTest`, `tests/HandleBindingTest`.

### 1.2 Compensable steps

**WGL-WRK-010** (MUST) A step carries an undo only when the topology declares it through a
`CompensableActivity` factory ([WGL-AUTH-060](10-authoring.md) `thenApplyCompensable`).

**WGL-WRK-011** (MUST) The binder MUST verify the pairing both ways: a declared undo whose handler is
not `Compensable`, and a `Compensable` handler on an undeclared step, MUST both refuse to bind.

**WGL-WRK-012** (MUST) `compensate(Compensation<A,B>)` receives both snapshots — what the step was
given and what it returned.

*Verified by:* `client/…/CompensationDeclarationTest`, `server/engine/SagaCompensationTest`.

### 1.3 Reconciling against the graph

**WGL-WRK-020** (MUST) A worker does **not** publish topology. At `start()` it MUST fetch the
registered graph for each bound workflow and validate its handlers against it.

**WGL-WRK-021** (MUST) A signature that clashes with the matched node's kind MUST fail `start()` fast.

**WGL-WRK-022** (MUST) A graph step with no matching method MUST NOT fail startup; it is simply served
by no handler on this worker, and MUST be logged.

**WGL-WRK-023** (MUST) `registerHandler(object)` (unversioned) MUST bind against the **latest**
registered version and MUST claim tasks of every version of that workflow.

**WGL-WRK-024** (MUST) `registerHandler(object, version)` MUST bind against that exact graph and MUST
restrict the worker's claims to that `(workflow, version)`. Binding a version that was never
registered MUST fail `start()`.

**WGL-WRK-025** (MUST) A worker with any unversioned registration claims every version: the scoping
is only as narrow as the least specific binding.

**WGL-WRK-026** (MUST) `WorkerOptions.withAwaitRegistration(d)` makes `start()` wait up to `d` for a
graph to appear; the default of zero fails fast.

*Verified by:* `tests/VersionScopedWorkerTest`, `tests/VersioningTest`.

### 1.4 Queues served

**WGL-WRK-030** (MUST) A worker's served queue set is `WorkerOptions.queues()` when non-empty, and
otherwise every queue mentioned by its bound graphs.

**WGL-WRK-031** (MUST) A worker MUST NOT be able to run a step off its served queues: queue filtering
is enforced server-side on the claim regardless of what the worker attempts.

*Verified by:* `tests/LocalBoundaryTest`, `tests/CompetingConsumersTest`.

## 2. The poll loop

**WGL-WRK-040** (MUST) A worker MUST poll for at most `concurrency − inFlight` tasks, so backpressure
is a property of the protocol rather than a setting.

**WGL-WRK-041** (MUST) When saturated, the worker MUST wait for a slot to free rather than sleeping a
fixed interval (a fixed nap caps throughput at one concurrency-sized wave per nap).

**WGL-WRK-042** (MUST) A poll MUST carry the worker id, the served queues, the claimed
`(workflow, version)` pairs (empty = every version), the max tasks, the requested lease, and the
long-poll wait.

**WGL-WRK-043** (MUST) On an empty poll the worker MUST: honour `retryAfterMillis` when positive by
holding off that long; else back off `idleBackoff` if the server answered faster than the long-poll
floor (5 ms); else re-poll immediately, because the server-side long poll *is* the idle wait.

**WGL-WRK-044** (MUST) A poll error MUST be logged and followed by `errorBackoff`.

**WGL-WRK-045** (MUST) Each task MUST run on its own thread (virtual threads by default) with the
ambient `Step` info published for its duration.

*Verified by:* `client/…/WorkerPacingTest`, `tests/MemoryPollTest`.

## 3. Executing one task

**WGL-WRK-050** (MUST) A task activation carries: task id, instance id, workflow, version, node id,
step name, activity, kind, attempt, lease expiry, lease owner, context, the server-resolved execution
mode, and — for a forEach item step — the frozen base context, the item index and the item's source
map key.

**WGL-WRK-051** (MUST) For a forEach item step the activation's `context` is the **item's** current
value (any JSON value, scalars included), and `baseContext` is the frozen pre-forEach context.

**WGL-WRK-052** (MUST) An activation whose activity has no bound handler MUST be failed
non-retryably with `"no handler registered for activity '<activity>'"`.

**WGL-WRK-053** (MUST) While a handler runs, the worker MUST keep the lease alive by heartbeating, and
MUST guarantee no extension is sent after the task is settled.

**WGL-WRK-054** (MUST) A handler that throws `PermanentActivityException` MUST be reported as
**not retryable**; any other exception MUST be reported as **retryable**; an `Error` MUST be reported
non-retryably and rethrown.

**WGL-WRK-055** (MUST) A predicate handler that returns a non-`Boolean` MUST be failed non-retryably.

**WGL-WRK-056** (MUST) Every report MUST carry the handler's own `startedAt`/`finishedAt`, measured
around the invocation, so the recorded duration is the handler's and not the round trip's.

**WGL-WRK-057** (MUST) Events the handler emitted via `Step.emit` MUST be drained and shipped with
that step's report, in emission order, and MUST NOT be sent for an attempt that threw.

*Verified by:* `tests/ErrorHandlingTest`, `tests/StepTimingTest`, `tests/EmittedEventTest`,
`client/…/HeartbeatTest`.

## 4. The ambient `Step` API

**WGL-WRK-060** (MUST) `Step` exposes, and only inside an activity body: `attempt()` (1 on the first
try, incremented per retry), `name()`, `instanceId()`, `base()` (the frozen base context where one
exists), `itemIndex()`, `itemMapKey()`, and `emit(type, payload)`. Calling any of them outside an
activity MUST throw.

**WGL-WRK-061** (MUST) `Step.base(Class<T>)` MUST decode the frozen base context into the given type.

**WGL-WRK-062** (MUST) `Step.emit` MUST refuse a type starting with `wf.` and MUST refuse a payload
that is not an object (record or map).

**WGL-WRK-063** (MUST) A forEach item step MUST NOT be able to write the base context; only the
combine can.

*Verified by:* `tests/EmittedEventTest`, `client/…/ForEachAccessorTest`, `tests/NestedScopesTest`.

## 5. Reporting

**WGL-WRK-070** (MUST) All finished work MUST be reported through the single `ReportSteps` RPC. A
worker MUST NOT choose its reporting call by execution mode.

**WGL-WRK-071** (MUST) A reported run is `(taskId, leaseOwner, ordered steps, final)`. `final` states
that **this worker** will take no continuation — it hit a boundary, filled its batch, or is draining.
It says nothing about the workflow.

**WGL-WRK-072** (MUST) A step result MUST carry the node id and exactly one outcome: `merge` (task),
`predicateValue` (predicate) or `error`.

**WGL-WRK-073** (MUST) Whether a continuation is leased back to this worker is the **server's**
decision, derived from the workflow's mode; the client MUST NOT express it.

**WGL-WRK-074** (MUST) A worker MUST stop a run when a reported outcome's instance status is anything
other than `RUNNING`.

**WGL-WRK-075** (MUST) A refused run wrote nothing and MAY be reported again on its own. A client
reporting a single run MUST surface the refusal as an error carrying that status.

**WGL-WRK-076** (MAY) Several runs for **different** instances MAY be reported in one call; see
[WGL-MODE-030](40-execution-modes.md) for when the server admits a batch.

*Verified by:* `server/engine/AdvanceManyTest`, `tests/CrossInstanceBatchTest`,
`tests/LocalSyncTest`.

## 6. Lifecycle and shutdown

**WGL-WRK-080** (MUST) `start()` MUST be idempotent and MUST reconcile registrations before polling
begins.

**WGL-WRK-081** (MUST) `close()` MUST stop polling, let in-flight steps finish, and MUST NOT interrupt
a running handler.

**WGL-WRK-082** (MUST) `close()` MUST cause an in-flight local run to **drain** — flush its buffered
steps with a forced handback — instead of continuing to chain, so a graceful shutdown loses nothing
already computed.

**WGL-WRK-083** (MUST) A worker crash MUST lose at most the current step (`SERVER`, `LOCAL_SYNC`) or
the current local batch (`LOCAL_ASYNC`), recovered by lease expiry.

*Verified by:* `tests/GracefulShutdownTest`, `tests/ServerShutdownTest`.

## 7. Tuning

**WGL-WRK-090** (MUST) `WorkerOptions` defaults MUST be: `concurrency` = available processors,
`lease` = 30 s, `longPollWait` = 10 s, `idleBackoff` = 200 ms, `errorBackoff` = 2 s,
`localBatchSize` = 64, `queues` = empty (serve everything registered), `awaitRegistration` = 0,
`crossInstanceBatching` = false.

**WGL-WRK-091** (MUST) `localBatchSize` MUST be at least 1 and MUST be ignored by `SERVER` and
`LOCAL_SYNC`.

**WGL-WRK-092** (SHOULD) `crossInstanceBatching` SHOULD stay off unless the server is a real network
hop away: it collapses N report RPCs into one but couples instances into shared transactions.

## 8. RPC resilience

**WGL-WRK-100** (MUST) Every client and worker RPC MUST retry on `UNAVAILABLE` only — the call almost
certainly never ran, so the retry is safe even for a non-idempotent operation.

**WGL-WRK-101** (MUST) `DEADLINE_EXCEEDED` and permanent errors MUST NOT be retried.

**WGL-WRK-102** (MUST) Retry is tunable per JVM by `wiggle.rpc.maxAttempts` /
`WIGGLE_RPC_MAX_ATTEMPTS` (default 5; 1 disables) and `wiggle.rpc.retryDelayMillis` /
`WIGGLE_RPC_RETRY_DELAY_MILLIS` (default 200, exponential).

**WGL-WRK-103** (SHOULD) A submitter that must not double-start across a retried failover SHOULD pass
a correlation id to `start`.

*Verified by:* `tests/RpcRetryFailoverTest`.
