# Design: one reporting API

Status: **implemented** · Supersedes: the `CompleteTask` / `AdvanceRun` split

## 1. The problem

This section describes the surface as it was before the change. A worker reporting finished work
had to choose between two RPCs, and it chose by execution mode:

```text
Worker.execute()
    task.executionMode() != SERVER && graph is registered  ->  LocalRun  ->  AdvanceRun
    otherwise                                              ->  ServerRun ->  CompleteTask
```

The server then resolved the mode again: both engine entry points called
`definitions.executionMode(tx, ...)` and `modeFactory.create(mode)` before doing anything. So the
mode reaches the worker only because the server stamped it on the `TaskActivation`, and the
worker's RPC choice told the server nothing it does not independently recompute.

That was duplicated knowledge across a network boundary, and it could disagree. `AdvanceRun` was
keyed by task id, not by mode: nothing stopped a client from calling it against a task whose
workflow runs `SERVER`, and the engine had no mode guard -- it resolved whatever mode the
definition declared and dispatched. That landed in `BaseRunningMode.chainSteps`, which threw
`UnsupportedOperationException` because `ServerRunningMode` had no override. The batch path
answers the same question deliberately (`LocalAsyncRunningMode.validate` rejects a non-LOCAL_ASYNC
run with `EngineException.conflict`); the single-run path answered it by accident.

**The fix is not a better refusal.** It is removing the client's ability to get it wrong.

## 2. What is genuinely the client's decision, and what is not

Worth separating, because the mode does not stop mattering to the worker:

| Decision | Whose | Why |
|---|---|---|
| How many steps to run locally before reporting | **Worker** | Only the worker knows it holds the graph, its batch budget, and whether it is draining for shutdown |
| Whether the continuation is leased back or released | **Server** | It is a property of the workflow's declared mode, which the server owns |
| Which RPC carries the report | **Neither — there should only be one** | The choice encodes nothing the server does not already know |

So `TaskActivation.execution_mode` stays. A worker still needs to know whether it may chain. What
goes away is using that same field to pick a wire call.

## 3. The one RPC

```proto
// Reports finished work: one step, or an ordered run of locally-chained steps. The server
// decides from the workflow's mode whether the continuation is leased back to this worker or
// released; the client never chooses that.
rpc ReportSteps(ReportStepsRequest) returns (ReportStepsResult);

message ReportStepsRequest {
    string task_id = 1;                 // the currently-leased token these steps start at
    string lease_owner = 2;
    repeated StepResult steps = 3;      // ordered, at least one; one step is the ordinary case
    // The worker will not take a continuation: it reached a boundary, filled its batch, or is
    // draining. A hint about this worker, not a protocol variant -- a mode that never chains
    // ignores it.
    bool final = 4;
}

message ReportStepsResult {
    string instance_status = 1;         // any InstanceStatus; anything but RUNNING means stop
    int64 lease_expires_at = 2;         // renewed lease, when the continuation was leased back
    string next_task_id = 3;            // blank when nothing was leased back to this worker
}
```

`StepResult` is reused unchanged, and that is an improvement in itself. It carries `node_id` and a
proper `oneof outcome { merge | predicate_value | error }`. `TaskResultRequest` carried neither: it
had one untyped `result`, so a predicate had to be smuggled through it as a map --
`ServerRun.settle` literally wrapped it (`Map.of("value", result)`) and the engine documented the
convention ("for PREDICATE nodes it must carry a boolean under `value`"). Unifying on `StepResult`
deletes that wart and gains a check: the server verifies the reported `node_id` against the token
in `requireMatchingNode`, for every report, which the single-step path could not do at all.

## 4. Server-side routing

`ReportSteps` lands in one engine entry point, which resolves the mode once and builds the context
the mode implies:

```text
ReportSteps(task, steps, final)
    lock the instance, resolve the mode
    SERVER      ->  apply each step as a completion; never lease the continuation back
    LOCAL_SYNC  ->  apply the run; lease the continuation back unless final
    LOCAL_ASYNC ->  apply the run; lease back unless final
    OBSERVED    ->  refuse: an observed run is reported through ObserveRun, not by a worker
```

The important consequence: there is now one context shape, `ReportStepsContext`, and one procedure
that applies it. The hole in §1 was a mode receiving a context it had no override for; with one
context and one shared procedure there is no such pairing left to get wrong. It stopped being
reachable rather than being reported, which is why this is better than adding a guard.

A `SERVER`-mode workflow reporting several steps at once is then well defined rather than
undefined, and the forgiving reading is the one that landed: the server applies every step in
order and hands the continuation back after the last one. The modes share one procedure,
`BaseRunningMode.chainSteps`, and differ in a single boolean, `chainsBack()`. `SERVER` answers
no, so its handback branch is taken at the last step; the local modes answer yes and keep the
continuation leased to the reporting worker.

## 5. What it changes on the client

`Worker.execute()` keeps branching on mode, but only to choose an execution strategy, not a wire
call:

```text
before:  mode != SERVER && graph  ->  LocalRun (AdvanceRun)   else  ServerRun (CompleteTask)
after:   mode != SERVER && graph  ->  chain locally           else  one step at a time
                                      ... both report through ReportSteps
```

`ServerRun` and `LocalRun` converge on one reporting call. `ServerRun` stops wrapping predicate
results in a map and reports its single step with `final = true`, which it is by construction: it
holds no graph and takes no continuation. `WiggleClient` has one `reportSteps(...)` and no
`complete(...)` at all.

## 6. Migration

None. The old RPCs were deleted outright rather than shimmed and deprecated: the platform has no
users yet, and a compatibility window would have bought a second code path that nobody needs and
that would then have to be removed anyway.

What went with them: `CompleteTask`, `AdvanceRun` and `TaskResultRequest` on the wire;
`WorkflowEngine.complete` (three overloads) and `WiggleClient.complete` (three overloads) in
Java; `CompleteExecutionContext` and `BaseRunningMode.completeStep`, whose whole job was to apply
a single step the way `chainSteps` already applies each one.

Two things fell out of the deletion rather than being designed:

- `StepReport` had two shapes, one per report path -- a `Completion` holding a worker's raw
  handler return and a `Reported` holding a pre-split `StepInput`. With one path it is one record,
  and the `{"value": <boolean>}` decoding the completion shape needed is gone.
- A predicate step reported without a branch is now a 400 rather than a silent `false`. The old
  `Reported.predicate()` returned `predicateValue != null && predicateValue`, which could only be
  reached by a chaining worker that always sets the field; as the single path it has to say so.

Out-of-tree clients (Go, Python, the vendored stubs under `wiggle-lab/`) regenerate from the
proto and move their reporting call.

## 7. Scope

**In:** `CompleteTask` and `AdvanceRun` collapse into `ReportSteps`.

**Out, deliberately:**

- `AdvanceMany` stays its own RPC. It is not "report N steps", it is "report N runs across N
  instances in one commit", with per-run rejection (`RunResult`) and cross-instance lock ordering.
  That is genuinely different semantics, not a wider arity.
- `FailTask` stays for now. A thrown step is already expressible as `StepResult.error`, so folding
  it in is possible, but it carries `retryable`, which has no `StepResult` equivalent, and retry
  policy is a separate concern from reporting. Worth revisiting once `ReportSteps` has landed.
- `HeartbeatTask` is unrelated.

## 8. Decisions taken

- **A multi-step report against a non-chaining mode is applied in order, not refused.** The steps
  ran; refusing them would discard work the worker already did.
- **`OBSERVED` refuses `ReportSteps`.** An observed run is reported through `ObserveRun`, by an
  instrumented application rather than a worker, and the refusal is a conflict naming that.
- **`ReportStepsResult` still does not distinguish "final" from "this mode never chains".** A
  blank `next_task_id` covers both, and a worker that set `final` already knows which it asked
  for.
