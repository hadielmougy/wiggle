# DSL cookbook

Eight small workflows, each pairing operators that don't otherwise appear together in the
`order-fulfilment` example. Read the source at
[`example/src/main/java/com/wiggle/cookbook/Cookbook.java`](../example/src/main/java/com/wiggle/cookbook/Cookbook.java)
(the topologies) and
[`CookbookHandlers.java`](../example/src/main/java/com/wiggle/cookbook/CookbookHandlers.java)
(the step logic) alongside this page; run all eight end to end with:

```bash
./gradlew :example:runCookbook
```

`CookbookDemo` starts an embedded server and one worker, registers every blueprint below and binds
its handlers, runs one instance of each, and prints the resulting context.

> **Topology and logic are separate.** A `Workflow.define(...)` blueprint is pure topology — named
> nodes and their wiring, no logic and no context type. The step logic lives in a class annotated
> `@Handlers("<workflow-name>")`, one per workflow, whose methods are matched to the graph by name
> (case/style-insensitive, so `isLarge` serves `is-large`). Each method's signature defines its step:
> a `Map<String, Object>` in and out is a task, a `boolean` return is a gate, `void` is an effect.
>
> **A task returns the whole document, not just what it changed.** The engine shallow-diffs a
> method's return value against the context it was given and merges only the keys that changed — so a
> handler must return the **full** context (see `with(...)` in the cookbook source), not just the
> fields it touched. Returning a bare `Map.of("k", v)` tells the engine every other key was
> deliberately cleared, and they come back as `null`.

---

## 1. `step` + `then` + `effect` + `gate`

The smallest useful pipeline: two transforms, a side effect, and a filter.

```java
Workflow.define("cb-linear-gate")
    .step("normalise")
    .then("classify")
    .gate("eligible")
    .effect("welcome")
    .build();
```

```java
@Handlers("cb-linear-gate")
class LinearGate {
    public Map<String, Object> normalise(Map<String, Object> ctx) {
        return with(ctx, "email", String.valueOf(ctx.get("email")).toLowerCase());
    }
    public Map<String, Object> classify(Map<String, Object> ctx) {
        return with(ctx, "vip", "hadi@wiggle.dev".equals(ctx.get("email")));
    }
    public boolean eligible(Map<String, Object> ctx) {          // gate
        return Boolean.TRUE.equals(ctx.get("vip"));
    }
    public void welcome(Map<String, Object> ctx) {              // effect
        System.out.println("welcome email -> " + ctx.get("email"));
    }
}
```

`then` is just `step` under another name for when "do this, then that" reads better. `gate`'s
false path ends the instance successfully as `gated:eligible` — not a failure, the workflow
equivalent of an empty stream. `effect` runs for its side effect only (a `void` method); the
context is unchanged.

## 2. `choose` + `fork` + retry

An exclusive switch/case whose matched branch itself fans out in parallel.

```java
Workflow.define("cb-choose-fork")
    .choose(
        Case.when("is-large", b -> b
            .fork(
                Branch.of("fraud-check", s -> s.step("fraud-check",
                        RetryPolicy.exponential(3, Duration.ofMillis(50)))),
                Branch.of("manager-notice", s -> s.effect("manager-notice")))
            .combine("large-merge")),
        Case.otherwise("standard", b -> b.step("fast-path")))
    .step("settle")
    .build();
```

```java
@Handlers("cb-choose-fork")
class ChooseFork {
    public boolean isLarge(Map<String, Object> ctx) {          // guard for "is-large"
        return ((Number) ctx.get("amount")).doubleValue() >= 1000;
    }
    public Map<String, Object> fraudCheck(Map<String, Object> ctx) {
        return with(ctx, "fraudChecked", true);
    }
    public void managerNotice(Map<String, Object> ctx) {
        System.out.println("large txn: " + ctx.get("amount"));
    }
    public Map<String, Object> fastPath(Map<String, Object> ctx) {
        return with(ctx, "fraudChecked", false);
    }
    public Map<String, Object> settle(Map<String, Object> ctx) {
        return with(ctx, "settled", true);
    }
}
```

A case's guard is a `boolean` handler named for the case (`isLarge` ↔ `is-large`); the topology only
names it. `choose` costs at most one guard evaluation per case, short-circuiting at the first match.
A `fork` always ends in a `combine` — here `large-merge`, which has no handler method, so the two
branches (a task and an effect, touching disjoint fields) fold with the default union.

## 3. `forkEach` + `defaultQueue` + a per-step queue

Runtime fan-out over a list, with one step in the branch pinned to a different worker pool.

```java
Workflow.define("cb-foreach-queues").defaultQueue("cpu")
    .forkEach("charge-items", "items", "item", b -> b
        // forkEach branches share one context, so a plain "priced" key would race across
        // items (last write wins) -- namespace by itemIndex instead.
        .step("price")
        .step("render-thumbnail", "gpu"))   // the queue arg pins just this step
    .step("summarise")
    .build();
```

```java
@Handlers("cb-foreach-queues")
class ForeachQueues {
    public Map<String, Object> price(Map<String, Object> item) {
        return with(item, "priced-" + item.get("itemIndex"), true);
    }
    public Map<String, Object> renderThumbnail(Map<String, Object> item) {
        return with(item, "thumbnail-" + item.get("itemIndex"), "thumb-" + item.get("itemIndex"));
    }
    public Map<String, Object> summarise(Map<String, Object> ctx) {
        return with(ctx, "done", true);
    }
}
```

One branch spawns per element of `items`; each handler sees its element as the whole context, with
its position under `itemIndex`. The `queue` argument affects only that one step — `render-thumbnail`
moves to `gpu`, `price` stays on the workflow's `cpu` default. An empty or missing list skips the
fan-out entirely.

## 4. `doWhile` + `gate`

A retry-until-ready loop, with an inner gate that can end the whole instance from inside the loop body.

```java
Workflow.define("cb-poll-until-ready")
    .doWhile("still-pending", b -> b
        .gate("not-cancelled")
        .step("poll"))
    .step("finish")
    .build();
```

```java
@Handlers("cb-poll-until-ready")
class PollUntilReady {
    public boolean stillPending(Map<String, Object> ctx) {     // loop condition
        return !Boolean.TRUE.equals(ctx.get("ready"));
    }
    public boolean notCancelled(Map<String, Object> ctx) {     // inner gate
        return !Boolean.TRUE.equals(ctx.get("cancelled"));
    }
    public Map<String, Object> poll(Map<String, Object> ctx) {
        int n = ((Number) ctx.getOrDefault("polls", 0)).intValue() + 1;
        return with(with(ctx, "polls", n), "ready", n >= 3);
    }
    public Map<String, Object> finish(Map<String, Object> ctx) {
        return with(ctx, "finishedAfter", ctx.get("polls"));
    }
}
```

`doWhile` names a guard handler (`stillPending`) that runs after each pass of the body; while it
holds, the body runs again. The inner `gate`'s false path short-circuits to the loop's exit, not just
the next iteration — a cancellation ends the instance immediately rather than looping forever.
`doWhile` compiles to a plain cycle in the graph, so it behaves identically under every
[execution mode](#7-executionlocal_async--checkpoint--dowhile).

## 5. `awaitSignal` (timeout + escalation) + `choose`

Wait for a human, escalate if nobody acts, then branch on which one happened.

```java
Workflow.define("cb-approval-escalation")
    .step("submit")
    .awaitSignal("manager-approval", Duration.ofMillis(200),
        esc -> esc.step("auto-escalate"))
    .choose(
        Case.when("was-escalated", b -> b.effect("notify-director")),
        Case.otherwise("was-approved", b -> b.effect("notify-submitter")))
    .build();
```

```java
@Handlers("cb-approval-escalation")
class ApprovalEscalation {
    public Map<String, Object> submit(Map<String, Object> ctx) {
        return with(ctx, "submitted", true);
    }
    public Map<String, Object> autoEscalate(Map<String, Object> ctx) {
        return with(with(ctx, "escalated", true), "approved", false);
    }
    public boolean wasEscalated(Map<String, Object> ctx) {     // guard for "was-escalated"
        return Boolean.TRUE.equals(ctx.get("escalated"));
    }
    public void notifyDirector(Map<String, Object> ctx) {
        System.out.println("escalated to director");
    }
    public void notifySubmitter(Map<String, Object> ctx) {
        System.out.println("approved directly");
    }
}
```

Exactly one of delivery or escalation happens; either way the flow rejoins after the wait, so
`choose` downstream can read whichever field the branch that ran actually set. (The escalation branch
here is a short deadline for the demo; in production it might be `Duration.ofHours(48)`.)

## 6. `subWorkflow` + `gate` + `fork`

Composing a *registered* workflow as a reusable child.

```java
Workflow.define("cb-parent")
    .subWorkflow("run-eligibility", "cb-linear-gate")   // example 1, reused as a child
    .gate("child-passed")
    .fork(
        Branch.of("provision", s -> s.step("provision")),
        Branch.of("audit", s -> s.effect("audit")))
    .combine("merge")
    .build();
```

```java
@Handlers("cb-parent")
class Parent {
    public boolean childPassed(Map<String, Object> ctx) {
        return Boolean.TRUE.equals(ctx.get("vip"));   // "vip" was set by the child workflow
    }
    public Map<String, Object> provision(Map<String, Object> ctx) {
        return with(ctx, "provisioned", true);
    }
    public void audit(Map<String, Object> ctx) {
        System.out.println("provisioning audited");
    }
}
```

The child starts with the parent's current context and its final context merges back on completion;
a failed or cancelled child fails the parent. The child workflow (`cb-linear-gate` here) must already
be registered on the server — `CookbookDemo` registers all eight blueprints before starting any
instance for exactly this reason. The parent's own `merge` combine has no handler, so its two
disjoint branches fold by union.

## 7. `execution(LOCAL_ASYNC)` + `checkpoint` + `doWhile`

Batched local execution, with an explicit commit point inside a loop.

```java
Workflow.define("cb-batched-loop").execution(ExecutionMode.LOCAL_ASYNC)
    .doWhile("more-batches", b -> b
        .step("process-batch")
        .checkpoint())   // flush the buffer before the next iteration
    .step("finalise")
    .build();
```

```java
@Handlers("cb-batched-loop")
class BatchedLoop {
    public boolean moreBatches(Map<String, Object> ctx) {
        return ((Number) ctx.getOrDefault("batch", 0)).intValue() < 3;
    }
    public Map<String, Object> processBatch(Map<String, Object> ctx) {
        int n = ((Number) ctx.getOrDefault("batch", 0)).intValue() + 1;
        return with(ctx, "batch", n);
    }
    public Map<String, Object> finalise(Map<String, Object> ctx) {
        return with(ctx, "batchesDone", ctx.get("batch"));
    }
}
```

Under `LOCAL_ASYNC` a worker buffers several steps and reports them in one call — fewer commits,
higher throughput, but a killed worker re-runs the whole unflushed batch. `checkpoint()` forces
a flush right after the step it follows, narrowing that replay window to the current loop
iteration instead of the whole run. It's a no-op under `SERVER` and `LOCAL_SYNC`, which already
commit every step.

## 8. Kitchen sink

`step`, `gate`, `choose`, `fork` (with a retried, queue-pinned branch), `forkEach`, `sleep`,
`awaitSignal` with escalation, `subWorkflow`, `doWhile` with a `checkpoint`, and
`defaultQueue` — one graph, every operator except the `fixed`/`forever` retry variants:

```java
Workflow.define("cb-kitchen-sink").defaultQueue("default").execution(ExecutionMode.LOCAL_SYNC)
    .step("intake")
    .gate("has-items")
    .subWorkflow("run-eligibility", "cb-linear-gate")
    .choose(
        Case.when("is-vip", b -> b
            .fork(
                Branch.of("priority-pack", s -> s
                    .step("pack", RetryPolicy.fixed(2, Duration.ofMillis(20)), "packing")),
                Branch.of("priority-notice", s -> s
                    .sleep("brief-hold", Duration.ofMillis(50))
                    .effect("notice")))
            .combine("large-merge")),
        Case.otherwise("standard", b -> b
            .forkEach("pack-items", "items", "item", body -> body
                .step("pack-item"))))
    .awaitSignal("dock-clear", Duration.ofMillis(150),
        esc -> esc.effect("auto-clear"))
    .doWhile("more-checks", b -> b
        .step("run-check")
        .checkpoint())
    .step("ship")
    .build();
```

```java
@Handlers("cb-kitchen-sink")
class KitchenSink {
    public Map<String, Object> intake(Map<String, Object> ctx) { return with(ctx, "stage", "intake"); }
    public boolean hasItems(Map<String, Object> ctx) {
        return ctx.get("items") != null && !((List<?>) ctx.get("items")).isEmpty();
    }
    public boolean isVip(Map<String, Object> ctx) { return Boolean.TRUE.equals(ctx.get("vip")); }
    public Map<String, Object> pack(Map<String, Object> ctx) { return with(ctx, "packed", true); }
    public void notice(Map<String, Object> ctx) { System.out.println("VIP order held briefly"); }
    public Map<String, Object> packItem(Map<String, Object> item) {
        return with(item, "packed-" + item.get("itemIndex"), true);
    }
    public void autoClear(Map<String, Object> ctx) { System.out.println("dock auto-cleared"); }
    public boolean moreChecks(Map<String, Object> ctx) {
        return ((Number) ctx.getOrDefault("checks", 0)).intValue() < 2;
    }
    public Map<String, Object> runCheck(Map<String, Object> ctx) {
        int n = ((Number) ctx.getOrDefault("checks", 0)).intValue() + 1;
        return with(ctx, "checks", n);
    }
    public Map<String, Object> ship(Map<String, Object> ctx) { return with(ctx, "stage", "shipped"); }
}
```

This one is deliberately not idiomatic — a real workflow wouldn't cram every operator into a
single graph. It exists as a stress test of the combination space and a single place to see how
`choose`, `fork`, `forkEach`, `subWorkflow`, `awaitSignal`, and `doWhile` all wire together and
still merge context correctly.

---

## Reference: what's covered where

| Operator | Examples |
|---|---|
| `step` / `then` | 1, 2, 3, 4, 5, 6, 7, 8 |
| `effect` | 1, 2, 5, 6, 8 |
| `gate` | 1, 4, 8 |
| `choose` / `Case.when` / `Case.otherwise` | 2, 5, 8 |
| `fork` / `Branch` / `combine` | 2, 6, 8 |
| `forkEach` | 3, 8 |
| `doWhile` | 4, 7, 8 |
| `sleep` | 8 |
| `awaitSignal` (+ timeout, + escalation) | 5, 8 |
| `subWorkflow` | 6, 8 |
| per-step `queue` / `defaultQueue` | 3, 8 |
| `RetryPolicy.exponential` / `.fixed` | 2, 8 |
| `execution(LOCAL_ASYNC)` / `execution(LOCAL_SYNC)` + `checkpoint` | 7, 8 |

Not covered here: `RetryPolicy.forever()` / `.none()` (trivial variants of `.fixed`/`.exponential`)
and `awaitSignal` without a timeout (see the README's [Signals](../README.md#signals-human--external-input) section).
