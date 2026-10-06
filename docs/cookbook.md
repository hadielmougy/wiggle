# Cookbook

Nine small workflows, each pairing operators that don't otherwise appear together in the
`order-fulfilment` example. Read the source at
[`example/src/main/java/com/wiggle/cookbook/Cookbook.java`](../example/src/main/java/com/wiggle/cookbook/Cookbook.java)
alongside this page; run all nine end to end with:

```bash
./gradlew :example:runCookbook
```

`CookbookDemo` starts an embedded server and one worker, registers every flow spec below and binds
its handlers, runs one instance of each, and prints the resulting context.

## How a recipe is written

Each recipe is an **interface** declaring its steps, and a class implementing them:

<!-- snippet: cookbook-contract/contract -->
```java
public interface LinearGateSteps {
    Signup     normalise(Signup s);
    Classified classify(Signup s);
    boolean    eligible(Classified c);
    void       welcome(Classified c);
}

@ForFlow("tcb-linear-gate")
public static final class LinearWithGate implements LinearGateSteps {
    ...
}
```

The spec names its steps **through the interface** — `s::normalise`, never an implementation —
because a spec does not run a step: it records the step's *name*, and a worker supplies the code by
matching that name. The `s` handed to the body is inert; calling a method on it throws.

Implementing the interface is optional but it is the point: the compiler then checks that every step
the topology names exists, with the signature the topology assumed. A worker in Go or Python
obviously cannot implement it, and does not need to — binding has always been by name.

Three things follow from steps being named after methods, and they are worth knowing before you read
on:

- A step's **kind** comes from its signature. Returning a value is a task, returning `boolean` is a
  gate, returning `void` is an effect.
- A step's **name** is the method's name. `normalise` is the node `normalise`.
- A fan-out **arm** is named after the step it ends on, and the engine stages that arm's result in
  the context under that name — so an arm name shares the key namespace with the context. Name a
  step for its position (`innerA`), not for the key it writes (`ia`), or the two collide.

## 1. `thenApply` + `thenAccept` + `thenFilter`

The smallest useful pipeline: two transforms, a filter, and a side effect — and a context type that
changes half way through.

<!-- snippet: cookbook/linear-gate -->
```java
FlowSpec spec = FlowSpec.define("tcb-linear-gate", 1, Signup.class, LinearGateSteps.class, (f, s) -> f
        .thenApply(s::normalise)
        // classify returns a different record, so the context type changes here; every
        // step after it must consume Classified, and the compiler holds that
        .thenApply(s::classify)
        // a false gate ends the instance successfully as "gated:eligible" -- not an error
        .thenFilter(s::eligible)
        .thenAccept(s::welcome));
```

`thenFilter`'s false path ends the instance **successfully** as `gated:eligible` — not a failure, the
workflow equivalent of an empty stream. `thenAccept` runs for its side effect only (a `void`
method); the context is unchanged.

## 2. `oneOf` + `allOf` + retry

An exclusive branch whose arm itself fans out.

<!-- snippet: cookbook/choose-fork -->
```java
FlowSpec spec = FlowSpec.define("tcb-choose-fork", 1, Purchase.class, ChooseForkSteps.class, (f, s) -> {
    // the large arm fans out: a fan-out inside a choice arm is just a fan-out whose
    // common point is the guard
    var large = f.when(s::isLarge);
    var fraud = large.thenApply(s::fraudCheck,
            RetryPolicy.exponential(3, Duration.ofMillis(50)));
    var notice = large.thenAccept(s::managerNotice);
    var largeArm = Wiggle.allOf(fraud, notice).combine(s::largeMerge);

    var standard = f.otherwise().thenApply(s::fastPath);

    // both arms end at Purchase, which is what lets oneOf give back a Purchase
    return Wiggle.oneOf(largeArm, standard).thenApply(s::settle);
});
```

`allOf` needs a combine and `oneOf` does not. The arms of a fan-out run on **isolated copies** of the
context, so a combine is the only way their results reach the flow; the arms of a choice are
alternatives on the one context, so control simply continues from whichever ran. That is also why a
choice's arms must agree on the type they end at — which one ran is not knowable until run time.

A combine's parameters are found **by type**, in any order: each takes the arm whose last step
produces its type, a `List`/`Set` takes every arm of its element type, and a parameter no arm
matches receives the pre-fork context. Arms of the same type go to the parameters of that type in
the order given to `allOf`, matched from the last parameter back — so with every arm a `Map`, a
leading extra `Map` parameter is the base. A combine need not take every arm.

A single guarded arm is legal here — `oneOf(f.when(g).thenApply(step))` reads as "run this if the
guard holds, otherwise skip past it". A single-armed `allOf` is not: there is nothing to fan out.

## 3. `thenForEach` + a per-step queue

Dynamic fan-out with mixed worker pools. The element *is* each branch's context.

<!-- snippet: cookbook/foreach-queues -->
```java
FlowSpec spec = FlowSpec.define("tcb-foreach-queues", 1, Basket.class, ForEachSteps.class, (f, s) -> f
        .defaultQueue("cpu")
        .thenForEach(Basket::items, item -> item
                .thenApply(s::price)
                // only this step moves to the "gpu" queue; the default stays "cpu"
                .thenApply(s::renderThumbnail, "gpu"))
        .combine(s::collectItems)
        .thenApply(s::summarise));
```

The collection is read from the context at run time, so the fan-out width is decided per instance,
not at definition. Inside the body the **item is the context** — `price` takes an `Item`, not the
`Basket`. An empty or missing collection skips the body *and* the combine.

`Basket::items` is a reference to the context record's own component, not to a step: nothing runs to
produce the collection — it is already in the context, put there by the step before. The reference
gives the key (`items`, exactly as the component is persisted) and the element type (`Item`, from its
return type), so there is no `Class<E>` to pass and renaming the component carries the key with it.
Maps and arrays work the same way. When the context is a `Map<String, Object>` there is no accessor
to reference, so name the key: `thenForEach("items", Item.class, body)`.

The combine's collection parameter decides how results arrive: a `List` keeps order, a `Set`
deduplicates, a `Map` is keyed like the input.

When items need different chains, prefer [recipe 9](#9-stepcreate--combine): the same fan-out built
by a step at run time. Keep `thenForEach` when every item runs the same body and you want that body in
the definition, where the portal shows it, and the fan-out made by the server with no step of its own.

## 4. `repeatWhile` + a gate inside the body

Poll-until-ready, with an inner gate short-circuiting a cancelled job.

<!-- snippet: cookbook/poll-until-ready -->
```java
FlowSpec spec = FlowSpec.define("tcb-poll-until-ready", 1, Job.class, PollSteps.class, (f, s) -> f
        // the body runs once, then the condition is evaluated -- do-while, not while-do
        .repeatWhile(s::stillPending, b -> b
                // a gate short-circuits to the loop's exit, not just the body: a
                // cancellation ends the whole instance here
                .thenFilter(s::notCancelled)
                .thenApply(s::poll))
        .thenApply(s::finish));
```

The body runs **at least once** — the condition is evaluated after it, not before. `repeatWhile` also
takes an explicit iteration budget (`repeatWhile(cond, max, body)`); a loop that exceeds it fails the
instance with an error naming the loop, rather than spinning forever.

## 5. `thenAwait` with timeout and escalation, then `oneOf`

Wait for a signal, and branch on how the wait resolved.

<!-- snippet: cookbook/approval-escalation -->
```java
FlowSpec spec = FlowSpec.define("tcb-approval-escalation", 1, Expense.class, ApprovalSteps.class, (f, s) -> {
    var waited = f
            .thenApply(s::submit)
            // no worker is held while it waits; if nobody signals in time the
            // escalation branch runs instead, then rejoins here
            .thenAwait("manager-approval", Duration.ofMillis(200),
                    esc -> esc.thenApply(s::autoEscalate));

    var escalated = waited.when(s::wasEscalated).thenAccept(s::notifyDirector);
    var approved = waited.otherwise().thenAccept(s::notifySubmitter);

    return Wiggle.oneOf(escalated, approved);
});
```

A parked instance holds **no worker and no thread** — it is a row waiting for a signal or a deadline.
Deliver with `client.signal(instanceId, name, payload)`. The two-argument form fails the instance on
timeout instead of running an escalation branch.

## 6. `thenSubFlow` + gate + `allOf`

Compose a registered child workflow into a bigger one.

<!-- snippet: cookbook/parent -->
```java
FlowSpec spec = FlowSpec.define("tcb-parent", 1, Signup.class, ParentSteps.class, (f, s) -> {
    var checked = f
            // runs tcb-linear-gate as a child; its final context merges back here, which
            // is why this continues as Classified
            .thenSubFlow("run-eligibility", "tcb-linear-gate", Classified.class)
            .thenFilter(s::childPassed);

    var provision = checked.thenApply(s::provision);
    var audit = checked.thenAccept(s::audit);

    return Wiggle.allOf(provision, audit).combine(s::merge);
});
```

The child is referenced **by name**, so it is an independently registered and versioned workflow with
its own handlers — not an inlined fragment. The parent parks while it runs and resumes with the
child's final context, which is why the `Class` argument says what to continue as.

## 7. `executeInLocalAsync()` + `checkpoint` + `repeatWhile`

Batched local execution with an explicit flush.

<!-- snippet: cookbook/batched-loop -->
```java
FlowSpec spec = FlowSpec.define("tcb-batched-loop", 1, Batch.class, BatchedSteps.class, (f, s) -> f
        .executeInLocalAsync()
        .repeatWhile(s::moreBatches, b -> b
                .thenApply(s::processBatch)
                .checkpoint())   // flush the buffer before the next iteration
        .thenApply(s::finalise));
```

`LOCAL_ASYNC` lets a worker chain steps locally and buffer their commits, which is a large throughput
win and a change to the crash-replay contract: buffered steps may re-run. `checkpoint()` forces the
buffer to commit at that point — the escape hatch for a step that must not be repeated. See
[local-execution.md](local-execution.md) for the full contract per mode.

## 8. Everything at once

A gate, a sub-workflow, a `oneOf` whose arms fan out and fan over a collection, a timed await with
escalation, a checkpointed loop.

<!-- snippet: cookbook/kitchen-sink -->
```java
FlowSpec spec = FlowSpec.define("tcb-kitchen-sink", 1, Basket.class, KitchenSinkSteps.class, (f, s) -> {
    var ready = f
            .defaultQueue("default")
            .executeInLocalSync()
            .thenApply(s::intake)
            .thenFilter(s::hasItems);

    var vip = ready.when(s::isVip);
    var packed = vip.thenApply(s::pack,
            RetryPolicy.fixed(2, Duration.ofMillis(20)), "packing");
    var held = vip.thenSleep("brief-hold", Duration.ofMillis(50)).thenAccept(s::notice);
    var vipArm = Wiggle.allOf(packed, held).combine(s::priorityMerge);

    var standard = ready.otherwise()
            .thenForEach("pack-items", Basket::items, item -> item.thenApply(s::packItem))
            .combine(s::collectPacked);

    return Wiggle.oneOf(vipArm, standard)
            .thenAwait("dock-clear", Duration.ofMillis(150), esc -> esc.thenApply(s::autoClear))
            .repeatWhile(s::moreChecks, b -> b.thenApply(s::runCheck).checkpoint())
            .thenApply(s::ship);
});
```

Every step takes an optional `RetryPolicy` and an optional queue, in either order, so
`thenApply(s::pack, policy, "packing")` and `thenApply(s::pack, "packing", policy)` are the same
thing.

## 9. `Step.create` + `combine`

Recipe 3's fan-out, built by a step at run time. A `combine` directly after a step makes that step
one that creates branches:

<!-- snippet: cookbook/dynamic-branches -->
```java
FlowSpec spec = FlowSpec.define("tcb-dynamic-branches", 1, Basket.class, DynamicSteps.class, (f, s) -> f
        .thenApply(s::plan)        // creates one branch per item, below
        .combine(s::collect));     // runs once every branch is done
```

Inside its handler, `Step.create(input)` opens a branch and chains steps onto it with the same
operators a workflow uses. The `for` and the `if` are ordinary Java, so each item runs the chain it
needs: a gift is priced and wrapped, anything else is only priced.

<!-- snippet: cookbook/dynamic-branches-handler -->
```java
public Basket plan(Basket b) {
    for (Item i : b.items()) {
        if (i.sku().startsWith("gift")) {
            Step.create(i).thenApply(this::price).thenApply(this::wrap);
        } else {
            Step.create(i).thenApply(this::price);
        }
    }
    return b;   // the base the combine receives
}

public Basket collect(List<Item> priced, Basket base, List<Gift> gifts) {
    long total = 0;
    for (Item i : priced) total += i.price();
    for (Gift g : gifts) total += g.price() + 50;   // wrapping is extra
    return new Basket(base.items(), total);
}
```

The branches are sent with the step's report, like `Step.emit`, and run once it is committed; an
attempt that throws creates none. The handler's return is the base the combine receives. The
combine's parameters are found by type, in any order: `List<Item>` takes the branches whose last
step returns an `Item`, `List<Gift>` the ones that end in `wrap`, and `Basket`, which is not a
collection, is the base. A step without its own queue or retry policy takes the creating step's.

A created branch chains tasks, effects, gates and sleeps, and also waits for signals
(`thenAwait`), runs sub-flows (`thenSubFlow`), forks (`thenAllOf(...).combine(...)`), chooses
(`thenOneOf` with `when`/`otherwise`), loops (`repeatWhile`) and creates branches of its own
(`Step.create(line).thenApply(this::pick).combine(this::packed)`). Use `thenForEach` (recipe 3) when
every item runs the same body and you want it in the definition.

## Reference: what's covered where

| Operator | Recipe |
|---|---|
| `thenApply` (task) | all |
| `thenAccept` (effect, `void`) | 1, 2, 5, 8 |
| `thenFilter` (gate, `boolean`) | 1, 4, 6, 8 |
| `Wiggle.oneOf` + `when` / `otherwise` | 2, 5, 8 |
| `Wiggle.allOf` + `combine` | 2, 6, 8 |
| `thenForEach` + `combine` | 3, 8 |
| `Step.create` + `combine` (branches built at run time) | 9 |
| `repeatWhile` | 4, 7, 8 |
| `thenAwait` (+ timeout, + escalation) | 5, 8 |
| `thenSubFlow` | 6, 8 |
| `thenSleep` | 8 |
| `checkpoint` | 7, 8 |
| `executeInLocalSync()` / `executeInLocalAsync()` | 7, 8 |
| per-node queue / `defaultQueue` | 3, 8 |
| `RetryPolicy` per step | 2, 8 |
