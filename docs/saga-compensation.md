# Design: Saga / compensation (backward recovery)

**Status:** proposal. Today a failed instance stops in place — sibling tokens are cancelled and the
instance goes `FAILED` (`WorkflowEngine.failInstance`), with no undo. This design adds **declared,
explicit compensation**: a step may name a compensating activity; when the instance fails, the
engine runs the compensators of the already-completed steps, in reverse completion order, as real
durable tokens.

The design is shaped by one hard fact about Wiggle and one principle.

- **The fact — replace semantics.** A step's return *replaces* the context; it does not accumulate
  (`applyStepResult`, `WorkflowEngine.java:881`). So the identifier a compensator needs (a
  `paymentRef` produced by `authorise`) may have been dropped by a later step that returned a
  context without it. A compensator therefore **cannot** be handed "the current context" — it must
  see the context **as its forward step left it**. That single constraint drives most of the design.
- **The principle — nothing implicit.** Wiggle's identity is explicitness: returns replace, combines
  are mandatory, no auto-merge. Compensation follows suit — only steps that *declare* a compensator
  are ever compensated, the undo order is defined, and the terminal state distinguishes "cleanly
  rolled back" from "stopped in place."

---

## 1. Author's surface (DSL)

A step optionally declares a compensating activity, attached to the just-added step exactly the way
`checkpoint()` attaches today (`WorkflowBuilder.java:418`):

```java
Blueprint orders = Workflow.define("order-fulfilment")
        .step("validate")
        .step("authorise").compensate("void-authorisation")
        .step("capture").compensate("refund", RetryPolicy.exponential(5, Duration.ofMillis(200)))
        .step("reserve-stock").compensate("release-stock")
        .step("print-label")                       // no compensator — nothing to undo
        .step("confirm")
        .build();
```

- `compensate(String activity)` — names the compensator for the preceding step.
- `compensate(String activity, RetryPolicy retry)` — compensators get their own retry policy
  (a refund is worth retrying hard; default = the workflow's default retry).
- `compensate(String activity, RetryPolicy retry, String queue)` — a compensator can run on a
  different worker pool than its forward step.

A step with no `.compensate(...)` is simply not compensated — its effect is either irreversible
(an email already sent) or immaterial (a read). That is a deliberate, visible choice in the graph,
not a default.

Works uniformly inside `fork` branches, `forEach` bodies, `choose` cases, and `doWhile` bodies —
any TASK node can carry a compensator.

---

## 2. Handler's surface

A compensator is an ordinary handler matched by name in the same `@Handlers` class — no new
registration path. It receives the **snapshot context its forward step returned** and is an
**effect** (`void`): a compensator undoes an external effect; it does not steer the forward flow
(which has already failed).

```java
@Handlers("order-fulfilment")
class OrderHandlers {
    public Order authorise(Order o) { return o.withAuthRef(gateway.auth(o)); }
    public Order capture(Order o)   { return o.withPaymentRef(gateway.capture(o.authRef())); }

    // compensators — receive the context AS THAT STEP LEFT IT, so authRef/paymentRef are present
    public void voidAuthorisation(Order o) { gateway.voidAuth(o.authRef()); }
    public void refund(Order o)            { gateway.refund(o.paymentRef()); }   // idempotent!
    public void releaseStock(Order o)      { wms.release(o.shipmentRef()); }
}
```

Compensators are **at-least-once**, like every handler (a lease can expire and redeliver), so they
must be idempotent — refund by the payment's idempotency key, not blindly. Same contract as forward
steps; stated loudly because a double refund is worse than a double read.

---

## 3. Graph / data-model changes

**One field on `Node`** (`core/.../Node.java:12`) — the compensator's activity name (+ its own
queue and retry). Since compensator dispatch reuses the existing worker-claim path, the cleanest
encoding is a nested descriptor:

```java
record Node(..., String itemKey, Compensation compensation)   // compensation nullable
record Compensation(String activity, String queue, RetryPolicy retry)
```

The compensator is **metadata on the forward node, not a node in the forward graph**. It has no
`next`/`altNext` edges — it never participates in forward routing, so the forward topology and its
validation (`Pipeline.validate`, `Pipeline.java:189`) are untouched. It rides along in the content
hash (`WorkflowDefinition.contentVersion`), so adding/removing a compensator is a new version, and
in-flight instances keep the version they started on — versioning is free here too.

This keeps the invariant that **the graph is data**: the compensator is a declared fact attached to
a node, and the reverse run is orchestrated by the engine over runtime token state (below), not by
replayed code.

---

## 4. Runtime: the compensation log

The problem replace-semantics creates: to hand a compensator the right context, we must capture
what its forward step returned, *at completion time*, before a later step can replace it.

**When a compensatable step completes** (`applyStepResult` path, `WorkflowEngine.java:881`), the
engine writes a **compensation-log entry** for the instance:

```
CompLogEntry(instanceId, seq, activity, queue, retry, contextSnapshotJson)
```

- `seq` — a per-instance monotonic counter, defining the reverse order.
- `contextSnapshotJson` — the context the step returned (for a branch-scoped step, the branch
  overlay it produced — captured the same way, so branch compensation "just works").

The log is bounded by the number of *compensatable* steps actually run — small by construction
(you compensate the few steps with external side effects, not reads). Storage: a new
`comp_log` table (JDBC) / map (in-memory), mirroring the token store's shape, added behind the
`Tx` seam (`server/.../store/Tx.java`) — never on the hot path of non-compensating workflows.

> **Why snapshot and not re-read current context?** Because under replace semantics the current
> context is not guaranteed to still contain `paymentRef`. Snapshotting at completion is the only
> correct choice, and it is honest about the cost: you pay a bounded per-compensatable-step context
> write. An optional `compensate("refund", projecting("paymentRef"))` overload can snapshot only the
> declared keys when contexts are large — a later refinement.

---

## 5. Runtime: the compensation phase

The single failure choke point is `failInstance(tx, inst, error, now)` (`WorkflowEngine.java:1366`).
Today it cancels active tokens and sets `FAILED`. New behaviour:

```
failInstance(inst, error):
    cancelActiveTokens(inst)                       # stop forward progress (unchanged)
    if compLog(inst).isEmpty():
        inst.status = FAILED                       # nothing to undo — today's behaviour, preserved
    else:
        inst.status = COMPENSATING
        inst.error  = error                        # remember why we're rolling back
        driveCompensation(inst)                    # mint the first compensator token
    notifyParent(inst)
```

`driveCompensation` picks the **highest-`seq` comp-log entry not yet compensated** and mints a
compensation token: a real `Rows.Token` with `activity = entry.activity`, `queue = entry.queue`,
its own retry policy, and `payloadJson = entry.contextSnapshotJson`. It is claimed, leased, and
executed by a worker through the **existing** dispatch path — compensators are just activities.

When a compensation token completes, the engine marks that comp-log entry compensated and drives
the next-highest `seq`. When none remain:

```
inst.status = COMPENSATED       # forward failed, but every declared undo ran cleanly
```

The cursor is **derived**, not stored — "next = highest `seq` whose comp token isn't `DONE`" — so a
crash mid-compensation resumes correctly on restart with no extra mutable state, and lease reclaim
(`reclaimOrphan`, `WorkflowEngine.java:642`) redelivers an in-flight compensator exactly as it does
a forward step.

**Order:** strictly reverse completion order (descending `seq`), run **sequentially** by default —
predictable, and the common case (refund before releasing the authorisation hold). Parallel
compensation of independent branches is a possible option later; sequential is the safe default.

---

## 6. Terminal states — telling the truth

Today: `RUNNING, COMPLETED, FAILED, CANCELLED` (`Rows.InstanceStatus`). Add:

| state | meaning |
|---|---|
| `COMPENSATING` | non-terminal: forward failed, compensators are running |
| `COMPENSATED` | terminal: forward failed **and every declared compensator ran cleanly** — the system was left consistent |
| `COMPENSATION_FAILED` | terminal: a compensator exhausted its retries — **a human must intervene** |

`COMPENSATED` vs `FAILED` is the whole point: `FAILED` means "stopped in place, some effects linger";
`COMPENSATED` means "we undid what we'd done." And `COMPENSATION_FAILED` refuses to pretend — the
one thing worse than a stuck saga is a stuck saga reported as success. A compensator that exhausts
its retry policy stops the phase, sets `COMPENSATION_FAILED`, records which entry failed, and the
console surfaces it for manual resolution (retry the compensator, or resolve out-of-band).

---

## 7. Cancellation with compensation

`client.cancel` is a deliberate operator action that today stops in place. Compensating on cancel is
desirable (a cancelled order should refund) but must be **explicit**:

- `cancel(id, reason)` — unchanged: stop in place, `CANCELLED`.
- `cancel(id, reason, compensate=true)` — cancel active tokens, then run the compensation phase,
  ending `COMPENSATED` / `COMPENSATION_FAILED`. (Proto: one bool field on `CancelInstanceRequest`.)

Same engine machinery as the failure path; only the trigger differs.

---

## 8. Composition with sub-workflows

Each instance owns its own saga. A child sub-workflow that fails runs **its own** compensation phase
(its own `failInstance`), reaches `COMPENSATED`/`COMPENSATION_FAILED`, and only then does
`notifyParent` (`WorkflowEngine.java:1075`) report the child's outcome upward — at which point the
**parent** compensates its own completed steps. Compensation composes across sub-workflow boundaries
with no special cross-instance logic, because it is per-instance token state throughout. (A parent
may treat a child's `COMPENSATION_FAILED` as its own compensation failure — configurable.)

---

## 9. What changes, concretely

| Area | File(s) | Change |
|---|---|---|
| DSL | `client/.../dsl/WorkflowBuilder.java`, `Pipeline.java` | `compensate(...)` overloads attaching a `Compensation` to the just-added TASK node |
| Graph model | `core/.../Node.java`, `NodeDraft.java` | nullable `Compensation` field; JSON round-trip; folds into `contentVersion` |
| Comp-log store | `server/.../store/Tx.java`, `InMemoryStorage.java`, `jdbc/…` | a bounded `comp_log` (append on compensatable completion, read/mark in the phase) |
| Capture | `WorkflowEngine.applyStepResult` (`:881`) | on a compensatable step's completion, append a comp-log entry with the returned-context snapshot |
| Phase | `WorkflowEngine.failInstance` (`:1366`) + new `driveCompensation` | branch into `COMPENSATING`; mint/settle compensator tokens over the existing dispatch path; derive the cursor; land `COMPENSATED`/`COMPENSATION_FAILED` |
| States | `Rows.InstanceStatus` (`store/Rows.java:9`) | add the three states; teach terminal checks and the purge/retention path |
| Cancel | `WorkflowEngine.cancel` (`:158`), `WiggleClient.cancel`, proto | optional `compensate` flag |
| Console | dashboard SPA + `DashboardData` | show `COMPENSATING`/`COMPENSATED`/`COMPENSATION_FAILED`, the comp-log, and a "retry compensator" action for the failed case |
| Docs | `docs/dsl-cookbook.md`, a new pattern | a worked saga; retire the "no rollback" caveat in `README.md:497` and `patterns/retries` |

No change to forward routing, join/combine, or validation — compensators are node metadata plus an
engine-orchestrated reverse pass, both additive.

---

## 10. Phasing

1. **Phase 1 — linear sagas.** `compensate(...)` DSL, `Node.compensation`, comp-log, the phase in
   `failInstance`, the three states, sequential reverse compensation, console read-out. Covers the
   overwhelming majority of real sagas (a linear pipeline of external effects). Ship this.
2. **Phase 2 — cancel-with-compensation** and the `COMPENSATION_FAILED` **retry-compensator**
   operator action.
3. **Phase 3 — refinements:** key-projected snapshots for large contexts; optional parallel
   compensation of independent branches; parent policy on child `COMPENSATION_FAILED`.

## 11. Alternatives considered

- **"Just write it in workflow code" (Temporal-style saga).** Not available and not wanted — Wiggle
  has no imperative workflow code by design. Compensation must live in the graph-as-data model.
- **Hand-wired reverse edges** (author draws the undo path with `choose`/`gate`). Rejected: it makes
  the author maintain reverse order by hand and re-derive it whenever the forward path changes; the
  declared-compensator approach derives the exact reverse order from *runtime* completion for free,
  including inside dynamic `forEach` fan-out where the author cannot know the width in advance.
- **Compensate against current context.** Rejected — unsound under replace semantics (§ the fact).

---

## 12. Worked example — what actually happens

`authorise ✓ → capture ✓ → reserve-stock ✓ → print-label ✗ (retries exhausted)`

1. `print-label` fails; `failToken` exhausts its retry and calls `failInstance`.
2. Comp-log holds three entries (seq 1 `void-authorisation`, seq 2 `refund`, seq 3 `release-stock`),
   each with the snapshot its forward step returned. Instance → `COMPENSATING`.
3. Engine mints the seq-3 compensator token → a worker runs `releaseStock(o)` with the context
   `reserve-stock` returned (so `shipmentRef` is present). Done.
4. seq-2 `refund(o)` with `capture`'s context (`paymentRef` present). Done.
5. seq-1 `voidAuthorisation(o)` with `authorise`'s context (`authRef` present). Done.
6. No entries remain → instance → `COMPENSATED`. `print-label` had no compensator and needed none;
   nothing was shipped. The order was cleanly unwound, and the console shows exactly which undos ran.
