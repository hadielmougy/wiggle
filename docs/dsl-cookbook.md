# DSL cookbook

Eight small workflows, each pairing operators that don't otherwise appear together in the
`order-fulfilment` example. Read the source at
[`example/src/main/java/dev/wiggle/cookbook/Cookbook.java`](../example/src/main/java/dev/wiggle/cookbook/Cookbook.java)
alongside this page; run all eight end to end with:

```bash
./gradlew :example:runCookbook
```

`CookbookDemo` starts an embedded server and one worker, registers every blueprint below, runs
one instance of each, and prints the resulting context.

> **JSON-map contexts must return the whole document.** Every example here uses
> `Workflow.defineJson`, whose steps take and return `Map<String, Object>`. The engine
> shallow-diffs a step's return value against the context it was given and merges only the
> keys that changed -- so a step must return the **full** context (see `with(...)` in the
> cookbook source), not just the fields it touched. Returning a bare `Map.of("k", v)` tells the
> engine every other key was deliberately cleared, and they come back as `null`.

---

## 1. `step` + `then` + `effect` + `gate`

The smallest useful pipeline: two transforms, a side effect, and a filter.

```java
Workflow.defineJson("cb-linear-gate")
    .step("normalise", ctx -> with(ctx, "email", String.valueOf(ctx.get("email")).toLowerCase()))
    .then("classify",  ctx -> with(ctx, "vip", "hadi@wiggle.dev".equals(ctx.get("email"))))
    .gate("eligible",  ctx -> Boolean.TRUE.equals(ctx.get("vip")))
    .effect("welcome", ctx -> sendWelcomeEmail(ctx.get("email")))
    .build();
```

`then` is just `step` under another name for when "do this, then that" reads better. `gate`'s
false path ends the instance successfully as `gated:eligible` — not a failure, the workflow
equivalent of an empty stream. `effect` runs for its side effect only; the context is unchanged.

## 2. `choose` + `fork` + retry

An exclusive switch/case whose matched branch itself fans out in parallel.

```java
Workflow.defineJson("cb-choose-fork")
    .choose(
        Case.when("is-large", ctx -> amount(ctx) >= 1000, b -> b
            .fork(
                Branch.of("fraud-check", s -> s.step("fraud-check", Fraud::check,
                        RetryPolicy.exponential(3, Duration.ofMillis(50)))),
                Branch.of("manager-notice", s -> s.effect("manager-notice", Notify::manager)))),
        Case.otherwise("standard", b -> b.step("fast-path", ctx -> with(ctx, "fraudChecked", false))))
    .step("settle", ctx -> with(ctx, "settled", true))
    .build();
```

`choose` costs at most one guard evaluation per case, short-circuiting at the first match — no
join, since exactly one branch ever runs. Nesting a `fork` inside a `choose` case is ordinary
composition: the case's body is just another `WorkflowStream`.

## 3. `forkEach` + `defaultQueue` + a per-step queue

Runtime fan-out over a list, with one step in the branch pinned to a different worker pool.

```java
Workflow.defineJson("cb-foreach-queues").defaultQueue("cpu")
    .forkEach("charge-items", "items", "item", b -> b
        // forkEach branches share one context, so a plain "priced" key would race across
        // items (last write wins) -- namespace by itemIndex instead.
        .step("price", item -> with(item, "priced-" + item.get("itemIndex"), true))
        .step("render-thumbnail", item ->
                with(item, "thumbnail-" + item.get("itemIndex"), "thumb-" + item.get("itemIndex")),
                "gpu"))
    .step("summarise", ctx -> with(ctx, "done", true))
    .build();
```

One branch spawns per element of `items`; each sees its element under `item` and its position
under `itemIndex`. The `queue` argument affects only that one step — `render-thumbnail` moves to
`gpu`, `price` stays on the workflow's `cpu` default. An empty or missing list skips the fan-out
entirely.

## 4. `doWhile` + `gate`

A retry-until-ready loop, with an inner gate that can end the whole instance from inside the loop body.

```java
Workflow.defineJson("cb-poll-until-ready")
    .doWhile("still-pending", ctx -> !Boolean.TRUE.equals(ctx.get("ready")), b -> b
        .gate("not-cancelled", ctx -> !Boolean.TRUE.equals(ctx.get("cancelled")))
        .step("poll", ctx -> pollUpstream(ctx)))
    .step("finish", ctx -> with(ctx, "finishedAfter", ctx.get("polls")))
    .build();
```

`gate`'s false path short-circuits to the loop's exit, not just the next iteration — a
cancellation ends the instance immediately rather than looping forever. `doWhile` compiles to a
plain cycle in the graph, so it behaves identically under every [execution mode](#7-executionlocal_async--checkpoint--dowhile).

## 5. `awaitSignal` (timeout + escalation) + `choose`

Wait for a human, escalate if nobody acts, then branch on which one happened.

```java
Workflow.defineJson("cb-approval-escalation")
    .step("submit", ctx -> with(ctx, "submitted", true))
    .awaitSignal("manager-approval", Duration.ofHours(48),
        esc -> esc.step("auto-escalate", ctx -> with(with(ctx, "escalated", true), "approved", false)))
    .choose(
        Case.when("was-escalated", ctx -> Boolean.TRUE.equals(ctx.get("escalated")),
            b -> b.effect("notify-director", Notify::director)),
        Case.otherwise("was-approved", b -> b.effect("notify-submitter", Notify::submitter)))
    .build();
```

Exactly one of delivery or escalation happens; either way the flow rejoins after the wait, so
`choose` downstream can read whichever field the branch that ran actually set.

## 6. `subWorkflow` + `gate` + `fork`

Composing a *registered* workflow as a reusable child.

```java
Workflow.defineJson("cb-parent")
    .subWorkflow("run-eligibility", "cb-linear-gate")   // example 1, reused as a child
    .gate("child-passed", ctx -> Boolean.TRUE.equals(ctx.get("vip")))
    .fork(
        Branch.of("provision", s -> s.step("provision", ctx -> with(ctx, "provisioned", true))),
        Branch.of("audit", s -> s.effect("audit", Audit::log)))
    .build();
```

The child starts with the parent's current context and its final context merges back on
completion; a failed or cancelled child fails the parent. The child workflow (`cb-linear-gate`
here) must already be registered on the server — `CookbookDemo` registers all eight blueprints
before starting any instance for exactly this reason.

## 7. `execution(LOCAL_ASYNC)` + `checkpoint` + `doWhile`

Batched local execution, with an explicit commit point inside a loop.

```java
Workflow.defineJson("cb-batched-loop").execution(ExecutionMode.LOCAL_ASYNC)
    .doWhile("more-batches", ctx -> batchCount(ctx) < 3, b -> b
        .step("process-batch", ctx -> with(ctx, "batch", batchCount(ctx) + 1))
        .checkpoint())   // flush the buffer before the next iteration
    .step("finalise", ctx -> with(ctx, "batchesDone", ctx.get("batch")))
    .build();
```

Under `LOCAL_ASYNC` a worker buffers several steps and reports them in one call — fewer commits,
higher throughput, but a killed worker re-runs the whole unflushed batch. `checkpoint()` forces
a flush right after the step it follows, narrowing that replay window to the current loop
iteration instead of the whole run. It's a no-op under `SERVER` and `LOCAL_SYNC`, which already
commit every step.

## 8. Kitchen sink

`step`, `gate`, `choose`, `fork` (with a retried, queue-pinned branch), `forkEach`, `sleep`,
`awaitSignal` with escalation, `subWorkflow`, `doWhile` with a `checkpoint`, and
`defaultQueue` — one graph, nineteen nodes, every operator except `fixed`/`forever` retry
variants:

```java
Workflow.defineJson("cb-kitchen-sink").defaultQueue("default").execution(ExecutionMode.LOCAL_SYNC)
    .step("intake", ctx -> with(ctx, "stage", "intake"))
    .gate("has-items", ctx -> !items(ctx).isEmpty())
    .subWorkflow("run-eligibility", "cb-linear-gate")
    .choose(
        Case.when("is-vip", ctx -> Boolean.TRUE.equals(ctx.get("vip")), b -> b
            .fork(
                Branch.of("priority-pack", s -> s
                    .step("pack", ctx -> with(ctx, "packed", true),
                            RetryPolicy.fixed(2, Duration.ofMillis(20)), "packing")),
                Branch.of("priority-notice", s -> s
                    .sleep("brief-hold", Duration.ofMillis(50))
                    .effect("notice", Notify::vip)))),
        Case.otherwise("standard", b -> b
            .forkEach("pack-items", "items", "item", body -> body
                .step("pack-item", item -> with(item, "packed-" + item.get("itemIndex"), true)))))
    .awaitSignal("dock-clear", Duration.ofMinutes(30),
        esc -> esc.effect("auto-clear", Dock::autoClear))
    .doWhile("more-checks", ctx -> checks(ctx) < 2, b -> b
        .step("run-check", ctx -> with(ctx, "checks", checks(ctx) + 1))
        .checkpoint())
    .step("ship", ctx -> with(ctx, "stage", "shipped"))
    .build();
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
| `fork` / `Branch` | 2, 6, 8 |
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