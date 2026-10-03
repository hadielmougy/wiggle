# 50 — Sagas: compensation and the reverse pass

← [Execution modes](40-execution-modes.md) · [Index](00-index.md) · Next: [Event log](60-event-log.md)

A failed instance stops where it is unless the topology declares undos. When it does, failure instead
unwinds them newest-first as ordinary durable tasks. This chapter specifies how a step is declared
compensable, what is captured when it completes, how the reverse pass runs, and what each terminal
state claims.

## 1. Declaring an undo

**WGL-SAGA-001** (MUST) A step is compensable **only** when the topology says so, by naming the step
through a `CompensableActivity` factory and `thenApplyCompensable(...)`. The declaration and the
node's `compensable` flag MUST be the same fact.

**WGL-SAGA-002** (MUST) `thenApplyCompensable` MUST be the only operator that accepts a
`CompensableActivity`, so the signature is the single place an undo is said to exist.

**WGL-SAGA-003** (MUST) The worker binder MUST verify the pairing in both directions: a declared undo
whose handler is not `Compensable`, and a `Compensable` handler on an undeclared step, MUST both refuse
to bind.

**WGL-SAGA-004** (MUST) The undo MUST be implemented on the same object as the step
(`Activity.execute` + `Compensable.compensate`), so the do and the undo travel together.

**WGL-SAGA-005** (MUST) A compensable step MAY change the context type like any other task.

**WGL-SAGA-006** (MUST) An `OBSERVED` graph MUST NOT contain compensable steps
([WGL-OBS-002](40-execution-modes.md)).

*Verified by:* `client/…/CompensationDeclarationTest`.

## 2. Capture: the compensation log

**WGL-SAGA-010** (MUST) When a compensable step completes, the engine MUST append one comp-log entry
**in the same transaction as the completion**, carrying: the instance id, a per-instance monotonic
`seq`, the node id, the activity, the queue, and two snapshots — the context the step was dispatched
with (`input`) and the context as the step left it (`result`).

**WGL-SAGA-011** (MUST) Both snapshots MUST be captured, because a step's return *replaces* the context:
the identifier an undo needs may be on either side — the reference the step produced, or the id it
consumed and did not carry forward.

**WGL-SAGA-012** (MUST) Snapshots MUST be taken at completion time, not re-read from the current
context later, because a later step may have replaced it.

**WGL-SAGA-013** (MUST) For a branch-scoped step the branch's own overlay MUST be captured, so branch
compensation needs no special case.

**WGL-SAGA-014** (MUST) The log MUST be bounded by the number of compensable steps actually run, and
MUST NOT be written at all for a workflow that declares no undo.

## 3. The reverse pass

**WGL-SAGA-020** (MUST) On instance failure, the engine MUST check the comp-log: with no uncompensated
entry it MUST set `FAILED`; with one it MUST set `COMPENSATING`, keep the original error on the
instance, and mint the first undo.

**WGL-SAGA-021** (MUST) Forward progress MUST stop first: the instance's active tokens are settled
before the reverse pass begins.

**WGL-SAGA-022** (MUST) The next undo MUST be the **highest-`seq` entry not yet compensated**, so the
order is strictly reverse completion order.

**WGL-SAGA-023** (MUST) Undos MUST run **sequentially**: one compensation token exists at a time.

**WGL-SAGA-024** (MUST) A compensation token MUST be a real durable task: minted at the forward step's
node, dispatched to the forward step's **queue**, under activity `"<activity>#compensate"`, claimed
and leased and retried through the ordinary claim path — not a callback in the failing step's thread.

**WGL-SAGA-025** (MUST) A compensation task's context MUST be the staged `{input, result}` snapshot
pair, which the worker's compensator wrapper splits into `Compensation.input()` and
`Compensation.result()`.

**WGL-SAGA-026** (MUST) A compensation task MUST always be dispatched in `SERVER` mode: the reverse pass
never chains.

**WGL-SAGA-027** (MUST) A compensation token MUST be claimable while the instance is `COMPENSATING`, and
that status MUST be the only one in which its comp-log entry may be settled.

**WGL-SAGA-028** (MUST) When an undo completes, the engine MUST settle its token, mark its entry
compensated, and mint the next-highest uncompensated entry. With none left the instance MUST become
`COMPENSATED`.

**WGL-SAGA-029** (MUST) The cursor MUST be **derived** ("next = highest seq not yet compensated"), never
stored, so a crash mid-compensation resumes correctly with no extra mutable state and lease reclaim
redelivers an in-flight compensator exactly as it does a forward step.

**WGL-SAGA-030** (MUST) A compensator that exhausts its own retry policy MUST stop the pass and set
`COMPENSATION_FAILED`, recording which entry failed and why.

*Verified by:* `server/engine/SagaCompensationTest`.

## 4. Terminal states

**WGL-SAGA-040** (MUST) The three saga states MUST mean exactly:

| State | Claim |
|---|---|
| `COMPENSATING` | non-terminal: the forward flow failed and the undos are running |
| `COMPENSATED` | terminal: the forward flow failed **and every declared undo ran cleanly** |
| `COMPENSATION_FAILED` | terminal: an undo exhausted its retries — a human must intervene |

**WGL-SAGA-041** (MUST) `COMPENSATED` MUST NOT be reported as success, and `COMPENSATION_FAILED` MUST
NOT be reported as either success or a plain failure. The original failure MUST be preserved on the
instance.

**WGL-SAGA-042** (MUST) A `COMPENSATING` instance MUST count as live for purge, for the coordinator's
retire census, and for every other "is this finished" test.

## 5. Interaction with other features

**WGL-SAGA-050** (MUST) Cancellation MUST NOT compensate: `cancel(id, reason)` stops in place and ends
`CANCELLED`. There is no compensate-on-cancel flag on the wire or in the client.
*(The variant described in `docs/saga-compensation.md` §7 is not implemented — see
[drift](00-index.md#6-known-documentation-drift).)*

**WGL-SAGA-051** (MUST) Each instance MUST own its own saga: a child sub-workflow that fails runs its
**own** reverse pass to `COMPENSATED`/`COMPENSATION_FAILED` and only then reports its outcome upward,
at which point the parent compensates its own completed steps.

**WGL-SAGA-052** (MUST) Compensation MUST compose across sub-workflow boundaries with no cross-instance
logic, because it is per-instance token state throughout.

**WGL-SAGA-053** (MUST) A compensator MAY emit events; its completion takes the same reporting path, so
an undo can announce itself on the event log ([chapter 60](60-event-log.md)).

**WGL-SAGA-054** (MUST) A lifecycle event MUST be appended for each of `wf.compensating`,
`wf.compensated` and `wf.compensation_failed`, in the transaction that made the transition.
