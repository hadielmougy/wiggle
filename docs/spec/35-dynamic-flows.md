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
`allOf` or `thenForEach`, MUST make that task a spawning step. Compilation MUST set the task's new
`spawning` flag and emit a dynamic `JOIN` (`expected == 0`) followed by a combine `TASK` carrying
`collectKey = __spawn__<step name>` (`ScratchKeys.spawn`).

**WGL-DYN-002** (MUST) A spawning step is still a `TASK`: no node kind is added, and its handler
signature is that of any task ([WGL-WRK-004](20-worker.md)).

**WGL-DYN-003** (MUST) The `spawning` flag MUST be part of the fingerprint
([WGL-AUTH-100](10-authoring.md)). Created branches MUST NOT be: what a branch runs is decided per
instance, by the handler, and is not part of the topology.

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

**WGL-DYN-012** (MUST) A branch step without its own queue MUST use the spawning step's queue, and one
without its own retry policy MUST use the spawning step's policy. Its activity MUST be
`<workflow>#<name>` ([WGL-AUTH-084](10-authoring.md)).

**WGL-DYN-013** (MUST) `Branch` MUST also offer `combine(ref)` directly after a task, making that
branch step a spawning step of its own. This is how dynamic flows nest.

**WGL-DYN-014** (MUST) Branches MUST be buffered on the worker for the current attempt, in creation
order, and ride its report. An attempt that throws after creating branches MUST leave nothing behind,
and its retry MUST create fresh ones — the semantics of [WGL-EVT-011](60-event-log.md).

**WGL-DYN-015** (MUST) A spawning step's return MUST replace the context as for any task
([WGL-AUTH-111](10-authoring.md)), and that context MUST become the frozen **base** of the round,
injected into the combine by type ([§8](#8-parameters-by-type)) and reachable from branch steps
through `Step.base()` ([WGL-WRK-061](20-worker.md)).

**WGL-DYN-016** (MUST) `Step.create` MUST throw when the running step is neither a spawning step nor
its combine.

## 3. Worker contract

**WGL-DYN-020** (MUST) A worker MUST bind a branch step by its activity and step name, never by node
id: a fragment node's id is minted per instance and is unknown to the worker's graph.

**WGL-DYN-021** (MUST) Binding is graph-driven today, and a method matching no node is ignored as a
helper ([WGL-WRK-020](20-worker.md)). For an activity not in the graph, a worker MUST instead bind the
`@ForFlow` method of that name on first dispatch and check its signature against the branch step's
kind and `compensable` flag then, failing the step non-retryably on a mismatch with the message
[WGL-WRK-021](20-worker.md) and [WGL-WRK-011](20-worker.md) would give at `start()`.

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
    NodeKind kind = 2;                 // TASK | PREDICATE | SLEEP
    bool compensable = 3;
    bool spawning = 4;                 // followed by its own combine, named by `combine`
    string combine = 5;
    string queue = 6;                  // blank = the spawning step's queue
    RetryPolicy retry = 7;             // absent = the spawning step's policy
    int64 sleep_millis = 8;
}
```

**WGL-DYN-031** (MUST) The node message MUST gain `spawning`. A client library that does not know it
MUST still register a topology without spawning steps with a byte-identical fingerprint
([WGL-GEN-006](00-index.md)).

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

- id `d<spawnTokenId>.<round>.<branch>.<position>`;
- `next` = the branch's following fragment node, or the spawning step's `JOIN` for the last step;
- for a gate, `altNext` = the spawning step's `JOIN` (a false gate short-circuits the branch, as in
  [WGL-AUTH-087](10-authoring.md));
- for a sleep, kind `SLEEP` with the given `sleepMillis`.

**WGL-DYN-043** (MUST) A spawning branch step MUST compile to a fragment `TASK` with `spawning` set,
followed by its own fragment `JOIN` and combine; that combine continues at the branch's following
fragment node. The inner step's branches are written when it reports, under its own token id.

**WGL-DYN-044** (MUST) Fragment nodes MUST be stored in a new table `wf_dyn_node`, keyed
`(instance_id, node_id)`, on the instance's shard ([WGL-SHARD-001](85-sharding.md)). They MUST be
immutable once written and purged with their instance ([WGL-STOR-060](80-storage.md)).

**WGL-DYN-045** (MUST) Graph lookup MUST resolve an id with the `d` prefix from the instance's
fragments and any other id from the definition. Node id generation ([WGL-AUTH-080](10-authoring.md))
MUST NOT mint a static id with that shape. A server MAY cache fragments per instance.

### 5.3 Fan-out and join

**WGL-DYN-046** (MUST) Branch tokens MUST be minted exactly as `DYN_FORK` mints them
([WGL-ENG-043](30-engine.md)): one token per branch at its first fragment node, each carrying an
`ITEM` frame whose view is the branch's `input` and whose index is its creation position, all sharing
the group `"<spawnTokenId>.<round>#<width>"`. The drive budget MUST grow by the width
([WGL-ENG-032](30-engine.md)).

**WGL-DYN-047** (MUST) The spawning step's `JOIN` MUST behave as a dynamic barrier
([WGL-ENG-044](30-engine.md), [045](30-engine.md)). On satisfaction it MUST stage the branch views
under `__spawn__<step name>`: a list ordered by creation, or a map keyed by branch key when the
branches carried keys ([WGL-ENG-046](30-engine.md)).

**WGL-DYN-048** (MUST) A spawning step that creates no branch MUST continue straight at the combine
with an empty collection. Unlike `thenForEach` ([WGL-ENG-043](30-engine.md)), the combine always
runs, because it is where another round is decided.

### 5.4 Rounds

**WGL-DYN-049** (MUST) A spawning step's combine MAY itself call `Step.create`. When it does, its
report MUST, in one transaction: make its returned context the new base, write the new branches as
round `r + 1` of the same spawning token, and mint them. Their results reach the same combine, which
runs again. A combine that creates nothing continues the flow with its return as the post-join
context, as today.

**WGL-DYN-050** (MUST) Rounds MUST be budgeted: the spawning step's own budget when declared
(`combine(ref).maxRounds(n)`), else `WIGGLE_DYN_MAX_ROUNDS` (default 100). The report that would
start round `budget + 1` MUST fail the instance with a message naming the step and how to raise the
budget, as [WGL-ENG-102](30-engine.md) does for loops.

## 6. Validation and limits

**WGL-DYN-051** (MUST) The server MUST refuse a report's branches, failing the step **non-retryably**
with a reason naming the offending branch and step, when:

- the running step is neither a spawning step nor its combine;
- a branch has no steps, a blank step name, a negative sleep, or a malformed retry policy
  ([WGL-AUTH-030](10-authoring.md));
- a nested spawning step names no combine;
- some but not all branches carry a key, or two share one;
- one report creates more than `WIGGLE_DYN_MAX_BRANCHES` branches (default 10 000);
- a branch exceeds `WIGGLE_DYN_MAX_STEPS` steps (default 100);
- the instance's fragments would exceed `WIGGLE_DYN_MAX_NODES` nodes in total (default 100 000);
- nesting would exceed `WIGGLE_DYN_MAX_DEPTH` spawning steps deep (default 16).

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

This section also changes every combine, static or dynamic, and every step that reads a base. It
drops `@Context` and binds a combine's parameters the way dependency injection binds a constructor:
by type, in any order. It supersedes [WGL-WRK-004](20-worker.md)'s combine rows,
[WGL-WRK-005](20-worker.md), [WGL-AUTH-065](10-authoring.md) and the `@Context` clause of
[WGL-AUTH-071](10-authoring.md).

```java
// before: arms by position, base by annotation
Receipt merge(@Context Order order, Payment payment, Shipment shipment)

// after: any order; each parameter is found by its type
Receipt merge(Shipment shipment, Order order, Payment payment)
```

**WGL-DYN-080** (MUST) The `@Context` annotation and the `combineWithContext` overloads MUST be
removed. `Step.base()` remains.

**WGL-DYN-081** (MUST) A combine's **sources** are the base and each arm's or branch's result. Each
source has a **declared type**, resolved from handler method signatures, never from the JSON:

- an arm's or branch's type is the return type of its last step that returns a value; when every step
  in it is an effect or a gate, it is the type that flowed into it;
- the base's type is the return type of the last step before the fan-out that returns a value — for a
  dynamic flow, the spawning step's own return type.

**WGL-DYN-082** (MUST) A parameter of type `P` that is not a collection MUST bind the single source
whose declared type is assignable to `P`. No match, or more than one, is an error naming the
parameter and the candidates.

**WGL-DYN-083** (MUST) A parameter of type `List<P>` or `Set<P>` MUST bind every arm or branch result
whose declared type is assignable to `P`, in fork or creation order, and never the base. `Map<String,
P>` MUST bind the same results keyed by branch key, forEach map key, or arm name for a fork. An empty
match binds an empty collection.

**WGL-DYN-084** (MUST) Parameter order MUST NOT matter. One source MAY bind several parameters, and a
source that binds none is not injected: a combine need not take every arm.

**WGL-DYN-085** (MUST) Binding MUST be checked as early as the shape is known:

- a static fork or forEach combine at definition time, by the DSL reading the declared types off the
  method references ([WGL-AUTH-050](10-authoring.md)), and again at worker `start()`
  ([WGL-WRK-021](20-worker.md));
- a dynamic combine at each activation, failing the step non-retryably, because its branches exist
  only at run time.

**WGL-DYN-086** (MUST) A worker that cannot resolve a source's declared type, because it holds no
handler for the step that produced it, MUST fail as in WGL-DYN-085 and name that step. A combine
SHOULD therefore live in the same `@ForFlow` class as the steps that feed it.

**WGL-DYN-087** (MUST) A step inside a scope (a forEach item or a branch) MAY declare a second
parameter for the base, and the two MUST bind by type in the same way. When the input and the base
share a type, the step MUST take only the input and read the base through `Step.base()`.

**WGL-DYN-088** (MUST) The typed DSL MUST accept a combine reference of any arity and parameter types
(`<P1, …, Pn, R> combine(FlowFnN<P1, …, Pn, R>)`). The compile-time positional check moves to the
definition-time type check of WGL-DYN-085.

**WGL-DYN-089** (MUST) Nothing on the wire or in the engine changes: staged keys stay
`__arm__<name>`, `__forEach__<name>` and `__spawn__<name>`. Binding by type is a client binding rule,
and another client library MAY bind differently.

## 9. Retiring `DYN_FORK`

Dynamic flows and `thenForEach` share the minting, frame, join and staging machinery; they differ in
who builds the branches. Once dynamic flows ship, the engine needs only one fan-out path.

**WGL-DYN-070** (MUST) Phase 1 ships dynamic flows beside `DYN_FORK`, which is unchanged.

**WGL-DYN-071** (MUST) Phase 2 MUST re-implement `DYN_FORK` as branches the server creates: at drive
time the engine reads `itemsKey` and compiles one branch per element over the static template,
skipping the combine when the collection is empty ([WGL-ENG-043](30-engine.md)). Stored definitions
and their fingerprints MUST NOT change, so no instance or registration migrates.

**WGL-DYN-072** (MAY) Phase 3 MAY deprecate `thenForEach` in the DSL in favour of spawning steps. The
engine keeps serving stored `DYN_FORK` nodes either way.

## 10. Open questions

- **Visibility.** The definition no longer says what a spawning step may run; the portal shows
  branches only once they exist, and a change to them mints no new version. An optional, advisory
  declaration could restore the view without being enforced.
- **Helpers become steps.** Any `@ForFlow` method can now be a branch step, including one meant as a
  helper. An opt-out annotation may be needed.
- **Signals, sub-flows and `oneOf` on a branch.** Should `Branch` offer `thenAwait`, `thenSubFlow` and
  `oneOf`? They are left out of the first cut: Java conditionals in the handler cover choice, and the
  others would widen the fragment format.
- **Local chaining through fragments.** Returning the fragment in `RunApplied` would let a `LOCAL_*`
  worker chain through a branch. It is worth doing only if dynamic flows turn out to be hot.
- **Empty-round semantics.** [WGL-DYN-048](#53-fan-out-and-join) diverges from `thenForEach`. If the
  divergence surprises users, an opt-in `skipWhenEmpty()` would align them.
- **Ambiguous types in practice.** Two arms returning the same type, or an effect arm whose type
  equals the base, must now be split into distinct types or taken as a collection. Worth measuring
  against the examples before committing to errors over a positional fallback.
- **Report size.** 10 000 branches with large inputs can exceed the gRPC message limit. The branch
  limit may need to become a byte limit.
