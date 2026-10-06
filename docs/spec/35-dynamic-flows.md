# 35 — Dynamic flows (proposed)

← [Engine semantics](30-engine.md) · [Index](00-index.md) · Next: [Execution modes](40-execution-modes.md)

**Status: proposed.** Nothing in this chapter is implemented. Every requirement here is *unverified*
([WGL-GEN-001](00-index.md)) until the change that builds it names its test. Where this chapter and
the rest of the suite disagree, the rest of the suite describes the code as it is today.

A **dynamic flow** is a fan-out whose branches are built by a step at run time. Inside an ordinary
handler, `Step.create(input)` opens a branch, and the operators that author a workflow — `thenApply`,
`thenAccept`, `thenFilter`, `thenSleep`, with the same retry and queue overloads — chain steps onto
it. The handler still returns its context as usual. Like `Step.emit` ([WGL-EVT-011](60-event-log.md)),
the branches are buffered on the worker, ride the step's report, and are committed in the transaction
that settles the step. The engine runs every branch in isolation and hands the results to the combine
that follows the step.

```java
// topology
.thenApply(s::fulfil)
.combine(s::summarise)

// handler
Order fulfil(Order o) {
    for (Item i : o.items()) {
        if (i.digital()) Step.create(i).thenAccept(this::deliverLink, "digital");
        else             Step.create(i.sku(), i).thenApply(this::reserve).thenApply(this::ship);
    }
    return o;
}

// combine: parameters bind by type, in any order
Order summarise(List<Shipment> shipped, Order order, List<Link> links) { ... }
```

`thenForEach` fans out one fixed template over a collection. A dynamic flow lets each branch run a
different chain, decided by plain Java, over any handler method of the workflow.

A step **creates** branches; it never waits on them. The handler returns, its branches commit with
its report, and nothing is replayed. That keeps the model of [WGL-ENG-002](30-engine.md): there is no
determinism discipline, and a spawning step may call a model, a database or a clock.

## 1. Model

| Term | Meaning |
|---|---|
| **spawning step** | A task followed directly by a combine. Only it, and its combine, may call `Step.create`. |
| **branch** | One `Step.create(input)` chain. Runs isolated over its input. |
| **fragment** | The nodes the engine compiles from one report's branches. Immutable, scoped to one instance. |
| **round** | One report's branches and the combine run that consumes them. A combine that creates branches starts the next round. |

**WGL-DYN-001** (MUST) A combine directly after a task (`thenApply` or `thenAccept`), rather than after
`allOf` or `thenForEach`, MUST make that task a spawning step. Compilation MUST emit a dynamic `JOIN`
(`expected == 0`) after the task, followed by a combine `TASK` carrying
`collectKey = __spawn__<step name>` (`ScratchKeys.spawn`). That shape is what marks a spawning step
(`GraphTraversal.spawnCombine`); no node carries a flag for it.

**WGL-DYN-002** (MUST) A spawning step is still a `TASK`: no node kind is added, and its handler
signature is that of any task ([WGL-WRK-004](20-worker.md)).

**WGL-DYN-003** (MUST) Whether a step spawns MUST be part of the fingerprint
([WGL-AUTH-100](10-authoring.md)), which it is through the combine's `collectKey`. Created branches
MUST NOT be: what a branch runs is decided per instance, by the handler, and is not part of the
topology.

## 2. Creating branches in a handler

**WGL-DYN-010** (MUST) The ambient `Step` API ([WGL-WRK-060](20-worker.md)) MUST gain
`Step.create(input)` and `Step.create(key, input)`, returning a `Branch<I>`. `input` becomes the
branch's whole context; `key` makes it addressable in the combine.

**WGL-DYN-011** (MUST) `Branch<I>` MUST offer the chaining operators of `WiggleFlow` with the same
names, typing, method-reference rules ([WGL-AUTH-050](10-authoring.md)–[054](10-authoring.md)) and
overloads ([WGL-AUTH-061](10-authoring.md)): `thenApply`, `thenAccept`, `thenFilter`,
`thenApplyCompensable` and `thenSleep`, each taking an optional `RetryPolicy` and an optional queue.
Steps are named by reference — `this::reserve` on the handler object, or `Steps::reserve` on the
contract — and nothing referenced is invoked.

**WGL-DYN-012** (MUST) A branch step without its own queue MUST use the creating step's queue, and one
without its own retry policy MUST use the creating step's policy — the spawning step's in round 1, the
combine's in a later round. Its activity MUST be `<workflow>#<name>` ([WGL-AUTH-084](10-authoring.md)).

**WGL-DYN-013** (MUST) `Branch` MUST also offer `combine(ref)` directly after a task, making that
branch step a spawning step of its own: its handler calls `Step.create`, and the combine receives
those branches' results, by type ([§8](#8-parameters-by-type)), and continues the outer branch with
its return. The combine runs on the step's queue under its retry policy. This is how dynamic flows
nest, to any depth within [WGL-DYN-051](#6-validation-and-limits)'s bounds.

```java
Step.create(line).thenApply(this::pick).combine(this::packed).thenApply(this::ship);
// pick(line) itself calls Step.create(unit).thenApply(this::scan) once per unit
```

**WGL-DYN-014** (MUST) Branches MUST be buffered on the worker for the current attempt, in creation
order, and ride its report. An attempt that throws after creating branches MUST leave nothing behind,
and its retry MUST create fresh ones — the semantics of [WGL-EVT-011](60-event-log.md).

**WGL-DYN-015** (MUST) A spawning step's return MUST replace the context as for any task
([WGL-AUTH-111](10-authoring.md)), and that context MUST become the frozen **base** of the round,
injected into the combine by type ([§8](#8-parameters-by-type)) and reachable from branch steps
through `Step.base()` ([WGL-WRK-061](20-worker.md)).

**WGL-DYN-016** (MUST) `Step.create` MUST throw when the running step is neither a spawning step nor
its combine. A worker that does not hold the task's node — an unbound version, or a created node —
MUST leave the check to the server ([WGL-DYN-051](#6-validation-and-limits)).

**WGL-DYN-017** (MUST) `Branch` MUST also offer the workflow's server-side operators, with the same
meaning they have in a definition:

- `thenAwait(signal)`, `thenAwait(signal, timeout)` and `thenAwait(signal, timeout, escalation)`, where
  `escalation` chains steps run when the deadline passes, after which the branch goes on as after the
  signal; a deadline with no escalation fails the instance ([WGL-ENG-047](30-engine.md));
- `thenSubFlow(node, workflow, Class<R>)`, which runs `workflow` as a child on the branch's context
  and merges its result back ([WGL-ENG-048](30-engine.md));
- `thenAllOf(arms...)` followed by `combine(ref)`, which runs each arm on its own copy of the branch's
  context and merges them by type ([§8](#8-parameters-by-type)). An arm is named after its last step,
  which must be a task or a gate, and names must differ.

```java
Step.create(item).thenAwait("approve-" + item.id(), Duration.ofHours(1), e -> e.thenApply(this::escalate));
Step.create(item).thenSubFlow("ship", "shipping", Shipment.class);
Step.create(item).thenAllOf(a -> a.thenApply(this::pay), b -> b.thenApply(this::reserve)).combine(this::settle);
```

**WGL-DYN-018** (MUST) A report whose branches run a sub-flow starts that workflow, so with per-RPC
authorization ([chapter 70](70-api.md)) the reporter MUST hold `instance.start` on each such workflow,
or the whole report is refused as a start would be: a worker serving one tenant's queues must not start
another tenant's workflows through the branches it creates.

## 3. Worker contract

**WGL-DYN-020** (MUST) A worker MUST bind a branch step by its activity and step name, never by node
id: a fragment node's id is minted per instance and is unknown to the worker's graph.

**WGL-DYN-021** (MUST) Binding is graph-driven today, and a method matching no node is ignored as a
helper ([WGL-WRK-020](20-worker.md)). For a task on a created node (id prefix `~`), a worker MUST
instead bind the `@ForFlow` method of that name on first dispatch and check its signature against the branch step's
kind then, failing the step on a mismatch with the message [WGL-WRK-021](20-worker.md) would give at
`start()`. Its undo, `<activity>#compensate`, MUST bind the same way, since it may run on a worker
that never ran the step. A task on any other node MUST still bind against the graph only, so a
step a newer version added stays unbound on a worker that bound an older graph
([WGL-WRK-023](20-worker.md)).

**WGL-DYN-022** (MUST) A branch step's queue need not appear in the graph. A worker on default
settings serves only the graph's queues ([WGL-WRK-030](20-worker.md)), so a queue that appears only in
`Step.create` chains MUST be listed in `WorkerOptions.queues()` of the workers that serve it. Until
one polls it, the step waits `READY` like any step whose workers are down.

## 4. Wire

**WGL-DYN-030** (MUST) `StepResult` MUST gain `repeated CreatedBranch branches = 8`, beside `merge`:

```proto
message CreatedBranch {
    google.protobuf.Value input = 1;   // the branch's whole context
    string key = 2;                    // optional; all-or-none across one report
    repeated BranchStep steps = 3;     // at least one
}
message BranchStep {
    string name = 1;                   // step name; blank for a sleep
    string kind = 2;                   // TASK | PREDICATE | SLEEP
    bool compensable = 3;
    string queue = 4;                  // blank = the creating step's queue
    google.protobuf.Value retry = 5;   // a retry policy as JSON; absent = the creating step's
    int64 sleep_millis = 6;
    string combine = 7;                // set: this task creates branches, collected by this combine
}
```

**WGL-DYN-031** (MUST) A step kind the server does not know MUST reach the engine as unknown and be
refused there ([WGL-DYN-051](#6-validation-and-limits)), failing the step rather than the report.
Definitions are unchanged on the wire, so a topology registers with the same fingerprint from a
client library that does not know dynamic flows ([WGL-GEN-006](00-index.md)).

**WGL-DYN-032** (MUST) `TaskActivation` MUST gain `string collect_key = 17`, set for the combine of a
forEach or of created branches, and `repeated string arm_names = 18`, set for a fork's combine. A
worker binds a created combine by them ([WGL-DYN-021](#3-worker-contract)): no graph it holds has the
node.

**WGL-DYN-033** (MUST) `BranchStep` MUST gain `string workflow = 8` (a sub-flow's child),
`repeated BranchStep escalation = 9` (a wait's escalation) and `repeated BranchArm arms = 10` (a
fork's arms, each `repeated BranchStep steps = 1`), with `kind` naming `SIGNAL`, `SUB_WORKFLOW` or
`FORK` and `combine` naming a fork's combine.

## 5. Engine

### 5.1 Accepting branches

**WGL-DYN-040** (MUST) A spawning step's report MUST be applied in one transaction under the instance
lock: validate the branches ([§6](#6-validation-and-limits)), write the fragment, mark the token
`DONE`, freeze the returned context as the round's base, and mint the branch tokens. A report that
fails validation writes nothing.

**WGL-DYN-041** (MUST) Only the report that holds the lease commits branches; a stale report from an
expired lease MUST be refused by the lease check ([WGL-ENG-111](30-engine.md)), so two runs of a
spawning step can never both fan out.

### 5.2 Fragments

**WGL-DYN-042** (MUST) Each step of each branch MUST compile to one fragment node carrying the step's
kind, name, activity, queue, retry and `compensable` flag, with:

- id `~` followed by a freshly minted token ([`Ids.token`](../../core/src/main/java/com/wiggle/core/Ids.java));
- `next` = the branch's following fragment node, or the round's `JOIN` for the last step;
- for a gate, `altNext` = the round's `JOIN` (a false gate short-circuits the branch, as in
  [WGL-AUTH-087](10-authoring.md));
- for a sleep, kind `SLEEP` with the given `sleepMillis`;
- for a wait, kind `SIGNAL` with the deadline in `sleepMillis` and `altNext` = the entry of its compiled
  escalation, which goes on to the same `next`;
- for a sub-flow, kind `SUB_WORKFLOW` with the child's name in `activity`;
- for a fork, a `FORK` whose branches are the arms' compiled entries, a static `JOIN` of the arms'
  width, and a combine carrying the arms' names, going on to `next`. A false gate in an arm goes to
  the fork's `JOIN`, as in a definition.

**WGL-DYN-043** (MUST) A branch step with a combine MUST compile as a spawning step does in a
definition ([WGL-DYN-001](#1-model)): a fragment `TASK`, a fragment dynamic `JOIN`, and a fragment
combine collecting under `__spawn__<step name>`, which continues at the branch's following fragment
node. The step's own branches are written when it reports, from its own token.

**WGL-DYN-044** (MUST) Fragment nodes MUST be stored in a new table `wf_dyn_node` (migration 33),
keyed by node id with the owning instance id beside it, on the instance's shard
([WGL-SHARD-001](85-sharding.md)). They MUST be immutable once written and purged with their instance
([WGL-STOR-060](80-storage.md)).

**WGL-DYN-045** (MUST) Graph lookup MUST resolve an id with the `~` prefix from the fragments and any
other id from the definition. Node id generation ([WGL-AUTH-080](10-authoring.md)) MUST NOT mint a
static id containing `~`. A graph handle MAY cache the fragments it has read.

### 5.3 Fan-out and join

**WGL-DYN-046** (MUST) Branch tokens MUST be minted exactly as `DYN_FORK` mints them
([WGL-ENG-043](30-engine.md)): one token per branch at its first fragment node, each carrying an
`ITEM` frame whose view is the branch's `input` and whose index is its creation position, all sharing
the group `"<creatingTokenId>#<width>"`. The drive budget MUST grow by the width
([WGL-ENG-032](30-engine.md)).

**WGL-DYN-047** (MUST) The round's `JOIN` MUST behave as a dynamic barrier
([WGL-ENG-044](30-engine.md), [045](30-engine.md)). On satisfaction it MUST stage the branch views
under `__spawn__<step name>`: a list ordered by creation, or a map keyed by branch key when the
branches carried keys ([WGL-ENG-046](30-engine.md)).

**WGL-DYN-048** (MUST) A spawning step that creates no branch MUST continue straight at the combine
with an empty collection. Unlike `thenForEach` ([WGL-ENG-043](30-engine.md)), the combine always
runs, because it is where another round is decided.

### 5.4 Rounds

**WGL-DYN-049** (MUST) A spawning step's combine MAY itself call `Step.create`. When it does, its
report MUST, in one transaction: make its returned context the new base, write the new branches with
a `JOIN` created for this round whose `next` is the same combine, and mint them from the combine's
token. Their results reach the same combine, which runs again. A combine that creates nothing
continues the flow with its return as the post-join context, as today.

**WGL-DYN-050** (MUST) Rounds MUST be budgeted by `WIGGLE_DYN_MAX_ROUNDS` (default 100), counting the
spawning step's own branches as round 1. The report that would
start round `budget + 1` MUST fail the instance with a message naming the step and how to raise the
budget, as [WGL-ENG-102](30-engine.md) does for loops.

## 6. Validation and limits

**WGL-DYN-051** (MUST) The server MUST refuse a report's branches, failing the step **non-retryably**
with a reason naming the offending branch and step, when:

- the running step is neither a spawning step nor its combine;
- a branch has no steps, a blank step name, a negative sleep, or a malformed retry policy
  ([WGL-AUTH-030](10-authoring.md));
- some but not all branches carry a key, or two share one;
- a step that is not a task carries a combine, or a combine has no name;
- the branches would sit more than `WIGGLE_DYN_MAX_DEPTH` scopes deep (default 16), counting every
  enclosing fork, forEach and created branch;
- the instance's created nodes would exceed `WIGGLE_DYN_MAX_NODES` (default 100 000);
- one report creates more than `WIGGLE_DYN_MAX_BRANCHES` branches (default 10 000);
- a branch exceeds `WIGGLE_DYN_MAX_STEPS` steps (default 100);
- a wait or a sub-flow has no name, a sub-flow no workflow, or a wait a negative timeout or an
  escalation without a timeout;
- a fork has fewer than two arms or no combine, an arm is empty or does not end in a task or a gate,
  or two arms end in the same step;
- a step kind is unknown, or is not `TASK`, `PREDICATE`, `SLEEP`, `SIGNAL`, `SUB_WORKFLOW` or `FORK`.

Arms and escalations are chains like a branch, held to the same rules.

**WGL-DYN-052** (MUST) The server MUST NOT refuse a branch step for its queue. Which queues have
workers is a deployment fact, not part of the report ([WGL-DYN-022](#3-worker-contract)).

**WGL-DYN-053** (MUST) A refused report MUST NOT be retried: the step that produced it once is likely
to produce it again, and the failure is a defect in the handler, not a transient fault. It follows the
instance failure path ([WGL-ENG-100](30-engine.md)), which this chapter extends with "created branches
were refused" and "a spawning step exceeded its round budget".

## 7. Interaction with other features

**WGL-DYN-060** (MUST) **Sagas.** A compensable branch step MUST append a comp-log entry like any
compensable step, capturing its branch's own view ([WGL-SAGA-013](50-sagas.md)). The reverse pass MUST
resolve the entry's fragment node, which is why fragments live as long as their instance.

**WGL-DYN-061** (MUST) **Execution modes.** A spawning step MUST be a handback point in `LOCAL_SYNC`
and `LOCAL_ASYNC`, classified `FORK` by `GraphTraversal.classify`
([WGL-MODE-021](40-execution-modes.md)). Fragment steps MUST be dispatched one per claim: a worker
holds no fragment, so it cannot chain through one ([WGL-MODE-011](40-execution-modes.md)).

**WGL-DYN-062** (MUST) **Events.** A spawning step MAY both emit and create; both ride the same report
and commit in the same transaction.

**WGL-DYN-063** (MUST) **Cancellation.** Fragment tokens MUST be settled `CANCELLED` like any other
active token ([WGL-ENG-090](30-engine.md)).

**WGL-DYN-064** (MUST) **Versioning.** A fragment node binds by activity, not by version, so a branch
step runs on whatever handler the claiming worker has for `<workflow>#<name>`. A version-scoped worker
([WGL-WRK-024](20-worker.md)) MUST still only claim branch steps of instances of its version.

**WGL-DYN-065** (SHOULD) **Event log.** The engine SHOULD write a `BRANCHES_CREATED` lifecycle entry
per round carrying the step name, round and width ([chapter 60](60-event-log.md)).

**WGL-DYN-066** (SHOULD) **Portal.** The step table SHOULD group fragment steps under their spawning
step by round and branch, with each branch's key when it has one, and SHOULD flag `READY` tokens on a
queue no worker is polling ([chapter 70](70-api.md)).

## 8. Parameters by type

This section changes every combine, static or dynamic, and every step that reads a base. It drops
`@Context` and binds a combine's parameters the way dependency injection binds a constructor: by
type, in any order. It supersedes [WGL-WRK-004](20-worker.md)'s combine rows,
[WGL-WRK-005](20-worker.md), [WGL-AUTH-065](10-authoring.md) and the `@Context` clause of
[WGL-AUTH-071](10-authoring.md).

```java
// before: arms by position, base by annotation
Receipt merge(@Context Order order, Payment payment, Shipment shipment)

// after: any order; each parameter is found by its type
Receipt merge(Shipment shipment, Order order, Payment payment)

// created branches: split by the type their last step produces
Order summarise(List<Shipment> shipped, Order order, List<Link> links)
```

**WGL-DYN-080** (MUST) The `@Context` annotation and the `combineWithContext` overloads MUST be
removed. `Step.base()` remains.

**WGL-DYN-081** (MUST) A combine's **sources** are the base and each arm's or branch's result. A
result's **type** is resolved from the handler of the step that produced it, never from the JSON: the
step's return type, or — for an effect or a gate, which pass their input on — its parameter type.
An arm's producing step is its last step, which names the arm. For a forEach or created branches, the
engine records in each `ITEM` frame the last step that ran in it and stages those names beside the
results under `__steps__<collectKey>` (`ScratchKeys.steps`), in the same order or under the same keys.

**WGL-DYN-082** (MUST) A combine parameter is a **collection** when it is a `List`, a `Set`, or a
`Map` whose value type is not `Object` (`Map<String, Shipment>`). A raw `Map` or a
`Map<String, Object>` is a single value: it is the shape most contexts have.

**WGL-DYN-083** (MUST) In a fork's combine, a parameter that is not a collection MUST take the arm
whose type is assignable to it, preferring an arm of exactly its type. Parameters sharing a type MUST
take that type's arms in fork order, matched from the last parameter back, so that a parameter left
over at the front receives the pre-fork context. A collection parameter MUST take every arm of its
element type, in fork order, or keyed by arm name for a `Map`.

**WGL-DYN-084** (MUST) In the combine of a forEach or of created branches, a parameter that is not a
collection receives the context from before the fan-out. A single collection parameter MUST take
every result; several MUST each take the results whose type is assignable to their element type. A
combine with one parameter, an untyped `Map`, takes the results.

**WGL-DYN-085** (MUST) Parameter order MUST NOT matter beyond WGL-DYN-083's tie-break, and a combine
need not take every result. Two parameters that would both receive the base MUST be refused at worker
`start()` ([WGL-WRK-021](20-worker.md)), naming what the arms produce.

**WGL-DYN-086** (MUST) A fork combine's worker that holds no handler for some arm's step cannot tell
that arm's type; its parameters MUST then take the arms in fork order, one each, and the base is
`Step.base()`. A collection combine that splits results by type and meets a result whose step this
worker does not hold MUST fail the step non-retryably, naming that step. A combine SHOULD therefore
live in the same `@ForFlow` class as the steps that feed it.

**WGL-DYN-087** (MUST) Every step that is not a combine MUST take exactly one parameter, its input.
Inside a forEach item or a created branch, the base is read through `Step.base(Type.class)`.

**WGL-DYN-088** (MUST) The typed DSL MUST accept a combine reference of any arity from 1 to 11 and any
parameter types (`<P1, …, Pn, R> combine(FlowFnN<P1, …, Pn, R>)`), on every fan-out stage
(`Combines`). Binding is checked by the worker that binds the combine, not when the workflow is
defined.

**WGL-DYN-089** (MUST) Binding by type is a client binding rule: the wire is unchanged, and another
client library MAY bind differently. The engine's only part is staging `__steps__<collectKey>`.

## 9. Retiring `DYN_FORK`

Dynamic flows and `thenForEach` differ only in who builds the branches: a forEach runs one static body
per element of a collection, a spawning step builds each branch in its handler. Everything after
that — the tokens, their frames, the join and the staged results — is one path.

**WGL-DYN-070** (MUST) Phase 1 ships dynamic flows beside `DYN_FORK`, which is unchanged. It leaves
out the `BRANCHES_CREATED` lifecycle entry ([WGL-DYN-065](#7-interaction-with-other-features)) and
the portal's grouping ([WGL-DYN-066](#7-interaction-with-other-features)), which follow it.
Parameters by type ([§8](#8-parameters-by-type)) and nesting
([WGL-DYN-013](#2-creating-branches-in-a-handler)) follow it in their own changes.

**WGL-DYN-071** (MUST) Phase 2 MUST give `DYN_FORK` and created branches one fan-out path: the same
minting (`FanOut.items`), the same `ITEM` frames, join and staging. A forEach keeps its static body —
each element's token starts at the body's first node — so `DYN_FORK`, stored definitions and their
fingerprints are unchanged and nothing migrates. A forEach over an empty collection still skips its
combine ([WGL-ENG-043](30-engine.md)).

*Rejected:* compiling the body into created nodes for every element, as this requirement first read.
It writes one copy of the body per element where a static body writes none, and created branches are
linear chains, while a forEach body may hold forks, nested fan-outs, signals and sub-flows.

**WGL-DYN-072** (SHOULD) Phase 3 SHOULD steer fan-outs whose body is a plain chain of steps to
spawning steps: the `thenForEach` javadoc, the README and the cookbook (recipe 9) recommend them, and
keep `thenForEach` for a body that holds a `oneOf` or a `repeatWhile`.
`thenForEach` MUST NOT carry `@Deprecated` while created branches cannot express every body it can;
the annotation MAY follow once a branch can choose on a step's result and loop, as it can now nest
([WGL-DYN-013](#2-creating-branches-in-a-handler)), wait for signals, run sub-flows and fork
([WGL-DYN-017](#2-creating-branches-in-a-handler)). The engine keeps serving `DYN_FORK` either way.

*Verified by:* `tests/DynamicConstructsTest`, `tests/NestedScopesTest`, `tests/DynamicFlowTest`.

## 10. Open questions

- **Visibility.** The definition no longer says what a spawning step may run; the portal shows
  branches only once they exist, and a change to them mints no new version. An optional, advisory
  declaration could restore the view without being enforced.
- **Helpers become steps.** Any `@ForFlow` method can now be a branch step, including one meant as a
  helper. An opt-out annotation may be needed.
- **`oneOf` and `repeatWhile` on a branch.** Java conditionals in the handler choose at creation time,
  and a combine that creates branches again loops by rounds; neither chooses on a step's result
  mid-branch. Offering them would let `thenForEach` be deprecated ([WGL-DYN-072](#9-retiring-dyn_fork)).
- **Local chaining through fragments.** Returning the fragment in `RunApplied` would let a `LOCAL_*`
  worker chain through a branch. It is worth doing only if dynamic flows turn out to be hot.
- **Empty-round semantics.** [WGL-DYN-048](#53-fan-out-and-join) diverges from `thenForEach`. If the
  divergence surprises users, an opt-in `skipWhenEmpty()` would align them.
- **Report size.** 10 000 branches with large inputs can exceed the gRPC message limit. The branch
  limit may need to become a byte limit.
