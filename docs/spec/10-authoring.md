# 10 — Authoring: the workflow model and the DSL

← [Index](00-index.md) · Next: [Worker contract](20-worker.md)

A workflow is **pure topology**: named nodes, their kinds, their edges, their queues and their retry
policies. It carries no step logic and no context type. This chapter specifies the graph model, the
Java authoring surface that compiles to it, the validation applied at compile time, and how a
version is published and held immutable.

## 1. The graph model

### 1.1 Definition

**WGL-AUTH-001** (MUST) A definition is
`WorkflowDefinition(name, version, startNode, Map<String,Node> nodes, Set<String> queues,
ExecutionMode executionMode, Set<String> checkpoints)`, immutable once constructed.

**WGL-AUTH-002** (MUST) `version` MUST be a positive integer declared by the author; a
non-positive version is rejected with `IllegalArgumentException` at construction.

**WGL-AUTH-003** (MUST) A null `executionMode` MUST normalise to `DEFAULT`.

**WGL-AUTH-004** (MUST) `nodes` is keyed by node id. `node(id)` for an unknown id MUST throw
`IllegalStateException`.

**WGL-AUTH-005** (MUST) `queues` MUST contain the queue of every worker-dispatched node, and only
those; `workerQueues()` recomputes it from the nodes.

**WGL-AUTH-006** (MUST) `key()` is `"<name>:<version>"` and is the identity used for graph lookup,
caching and worker scoping.

*Verified by:* `client/…/PipelineTest`, `tests/FlowApiTest`.

### 1.2 Node

**WGL-AUTH-010** (MUST) A node is one flat record with nullable per-kind slots:
`id, kind, name, activity, queue, retry, sleepMillis, next, altNext, branches, expected, success,
reason, itemsKey, itemKey, loopBudget, compensable, armNames, collectKey`.

**WGL-AUTH-011** (MUST) Edge slots have fixed meanings: `next` is the primary / true / delivery
edge; `altNext` is the false / escalation edge; `branches` holds fork branch-start ids (or the single
template id of a runtime fan-out).

**WGL-AUTH-012** (MUST) The node kinds and their obligations are exactly:

| Kind | Executed by | Edges used | Unique name required | In `queues` | `activity` |
|---|---|---|---|---|---|
| `TASK` | worker | `next` | yes | yes | `<wf>#<name>` |
| `PREDICATE` | worker | `next`, `altNext` | yes | yes | `<wf>#<name>` |
| `SLEEP` | server | `next` | no | no | — |
| `SIGNAL` | server (external actor) | `next`, `altNext` | yes | no | — |
| `SUB_WORKFLOW` | server | `next` | yes | no | child workflow name |
| `FORK` | server | `branches` | no | no | — |
| `DYN_FORK` | server | `next`, `branches` (exactly 1) | yes | no | — |
| `JOIN` | server | `next` | no | no | — |
| `END` | server | — | no | no | — |

**WGL-AUTH-013** (MUST) `isWorkerDispatched()` is true for `TASK` and `PREDICATE` only. No other
kind is ever offered to a worker.

**WGL-AUTH-014** (MUST) A `JOIN` with `expected > 0` is a static barrier of that width. `expected == 0`
marks a dynamic barrier whose width travels in the join group (see [WGL-ENG-044](30-engine.md)).

**WGL-AUTH-015** (MUST) An `END` node carries `success` and an optional `reason`; an unsuccessful
`END` fails the instance with that reason.

### 1.3 Combines

**WGL-AUTH-020** (MUST) A combine is a `TASK` node that additionally carries either `armNames`
(non-empty — it merges a static fork) or `collectKey` (non-null — it merges a runtime fan-out).
`isCombine()` is exactly that test.

**WGL-AUTH-021** (MUST) A fork's arm results MUST be staged for its combine under the reserved key
`__arm__<armName>` (`ScratchKeys.arm`), one per arm, in fork order.

**WGL-AUTH-022** (MUST) A runtime fan-out's collected item results MUST be staged for its combine
under the reserved key `__forEach__<fanOutNodeName>` (`ScratchKeys.forEach`).

**WGL-AUTH-023** (MUST) Staged keys MUST be stripped from the context once the combine has run, and
the `__x__` prefix MUST be used so that a staged key can never collide with a user context key. A
fan-out's node name defaults to the collection key, so the unprefixed form would overwrite the very
collection it fanned over.

**WGL-AUTH-024** (MUST) An arm's name is its branch's **last step name**. Step names are unique
within a workflow, so arm names are unique without being declared.

*Verified by:* `client/…/ForkCombineTest`, `client/…/CombineOptsTest`, `tests/ForkIsolationTest`,
`tests/LegacyCombineTest`.

### 1.4 Retry policy

**WGL-AUTH-030** (MUST) A retry policy is
`RetryPolicy(maxAttempts, initialBackoffMillis, multiplier, maxBackoffMillis, jitter)` with
`maxAttempts >= 1` and `multiplier >= 1.0`; `maxAttempts` counts the first try, so `1` means no
retry.

**WGL-AUTH-031** (MUST) The named constructors are: `none()` = one attempt; `fixed(n, d)` = n
attempts, evenly spaced; `exponential(n, initial)` = n attempts, multiplier 2.0, cap 5 minutes,
jitter 0.2; `forever()` = 10 000 attempts, 1 s fixed, cap 1 minute.

**WGL-AUTH-032** (MUST) `backoffMillis(attempt)` is the delay before attempt `attempt + 1`:
`initial * multiplier^(attempt-1)`, capped at `maxBackoffMillis` when positive, then scaled by a
uniform factor in `[1-jitter, 1+jitter]` when jitter is positive, floored at 0.

*Verified by:* `tests/ErrorHandlingTest`.

## 2. The authoring API

### 2.1 Entry points

**WGL-AUTH-040** (MUST) `FlowSpec.define(name, version, Class<T> input, Function<WiggleFlow<T>,
WiggleFlow<?>> body)` and its contract-typed twin
`define(name, version, Class<T> input, Class<H> contract, BiFunction<WiggleFlow<T>, H,
WiggleFlow<?>> body)` are the public entry points; `RetryPolicy`-carrying overloads of both set the
workflow default.

**WGL-AUTH-041** (MUST) The default retry policy when none is given MUST be `RetryPolicy.forever()`.

**WGL-AUTH-042** (MUST) `input` anchors the chain's typing only. It is not persisted, not checked,
and does not appear in the definition.

**WGL-AUTH-043** (MUST) The body MUST run exactly once, at definition time, and MUST return the
handle it ends on; returning null is an `IllegalStateException` naming the workflow.

**WGL-AUTH-044** (MUST) The `contract` stand-in `s` MUST be inert: it exists to *name* steps, and
invoking a method on it throws.

**WGL-AUTH-045** (MUST) A handler class MAY implement the contract interface, and when it does the
compiler checks both halves against one declaration. Implementing it MUST NOT be required — binding
is by name.

*Verified by:* `tests/FlowApiTest`, `tests/FlowApiRegressionTest`, `tests/HandlerOnlyWorkerTest`.

### 2.2 Step names from method references

**WGL-AUTH-050** (MUST) A node's name MUST be read off the method reference by serialised-lambda
inspection; nothing on the referenced object is invoked.

**WGL-AUTH-051** (MUST) A lambda (`o -> h.step(o)`) MUST be rejected: its implementation is a
synthetic method with no handler to bind on a worker.

**WGL-AUTH-052** (MUST) A reference capturing a local value MUST be rejected: a captured value is not
part of the topology and would not survive registration, restart or dispatch.

**WGL-AUTH-053** (MUST) When the referenced method carries `@Handles("node-name")`, that name MUST
win over the method name.

**WGL-AUTH-054** (MUST) Names are matched under a canonical folding, so `inStock` and `in-stock` are
the same step.

*Verified by:* `client/…/StepNamesTest`, `tests/HandleBindingTest`.

### 2.3 Operators

**WGL-AUTH-060** (MUST) The operators and the nodes they record are exactly:

| Operator | Records | Notes |
|---|---|---|
| `thenApply(ref)` / `apply(ref)` | one `TASK` | the handler's return becomes the new context |
| `thenAccept(ref)` | one `TASK` (effect) | `void` handler; context flows on untouched |
| `thenFilter(ref)` | one `PREDICATE` (gate) | false ends the instance, or short-circuits the enclosing branch |
| `thenApplyCompensable(factory)` | one `TASK` with `compensable = true` | the only way a step is marked compensable |
| `thenSleep(Duration)` / `thenSleep(name, Duration)` | one `SLEEP` | the unnamed form is named `sleep-<millis>ms` |
| `thenAwait(signal[, timeout[, escalation]])` | one `SIGNAL` | `sleepMillis` carries the deadline; the escalation branch is wired to `altNext` |
| `thenSubFlow(node, workflow, Class<R>)` | one `SUB_WORKFLOW` | `activity` = the child workflow name |
| `thenForEach(items, body)` → `Items.combine(...)` | one `DYN_FORK`, one dynamic `JOIN`, one branch template, one combine `TASK` | the combine is mandatory |
| `Wiggle.allOf(arms...)` → `ForkN.combine(...)` | one `FORK`, one static `JOIN(expected = arms)`, one combine `TASK` | 2–10 arms typed, more via `allOf(WiggleFlow...)` |
| `Wiggle.oneOf(arms...)` | one `PREDICATE` per guarded arm, chained by `altNext` | no combine |
| `when(ref)` / `otherwise()` | opens an arm of `oneOf` | `otherwise()` must be last |
| `repeatWhile(cond, [max,] body)` | body, then one `PREDICATE` looping back | do-while: the body always runs once |
| `checkpoint()` | marks the preceding step | a `LOCAL_ASYNC` flush boundary |
| `defaultQueue(q)` | workflow setting | applies to steps defined after it |
| `executeInServer()` / `executeInLocalSync()` / `executeInLocalAsync()` | workflow setting | see [chapter 40](40-execution-modes.md) |
| `as(Class<R>)` | nothing | re-types the chain where the new type is not statically knowable |

**WGL-AUTH-061** (MUST) Every step-recording operator MUST accept an optional `RetryPolicy` and an
optional queue, in either order.

**WGL-AUTH-061a** (MUST) A task, effect or gate MUST be named through a method reference; there is no
string-named overload for them. A topology authored apart from its handlers names its steps through a
**contract interface it need not implement** ([WGL-AUTH-044](#21-entry-points)). The only nodes named by
a bare string are combines (`combine(name, Class)`), sleeps, signals, sub-flows, and a fan-out's
collection key.

**WGL-AUTH-062** (MUST) Continuing one handle twice MUST be what creates a fan-out. The two
continuations are the arms, and the step they both continue from is where the `FORK` lands.

**WGL-AUTH-063** (MUST) A split that is never passed to `Wiggle.allOf` MUST be rejected at definition
time, naming the dangling ends.

**WGL-AUTH-064** (MUST) A combine is **mandatory** after `allOf` and after `thenForEach`: arms run on
isolated context copies, so a combine is the only path by which an arm's result reaches the flow.
There is no implicit merge, and no default fold.

**WGL-AUTH-065** (MUST) A referenced combine's arity MUST be checked against the fan-out's width at
definition time; `combine(String name, Class<R>)` is the escape hatch that skips the check.

**WGL-AUTH-066** (MUST) `oneOf` evaluates guards in the order the arms are given. Exactly one arm
runs. With an `otherwise()` arm it always picks one; without, a choice where no guard held MUST
continue at the step after the choice.

**WGL-AUTH-067** (MUST) `when(...)` is not `thenFilter(...)`: a failing guard hands the choice to the
next arm, while a failing gate ends the instance (or short-circuits its branch).

**WGL-AUTH-068** (MUST) `repeatWhile` compiles to a plain cycle whose condition is a worker-evaluated
predicate. The body runs before the first evaluation.

**WGL-AUTH-069** (MUST) Ordinary Java control flow in a definition body is unrolled, because the body
runs once: a `for` loop records that many distinct nodes. A decision that depends on a step's
*result* MUST use `oneOf` or `repeatWhile`.

**WGL-AUTH-070** (MUST) One handler method is one node. Referencing the same method twice in one
workflow is a duplicate step name and MUST be rejected.

**WGL-AUTH-070a** (MUST) A named-step overload MUST NOT be assumed from the javadoc: `WiggleFlow`'s
javadoc references a `thenApply(String, Class)` that does not exist — see
[drift](00-index.md#6-known-documentation-drift).

**WGL-AUTH-071** (MUST) `thenForEach` maps **elements to contexts**: each spawned branch's whole
context is its element. The frozen pre-fan-out context is read-only and reachable via `Step.base()`
or a `@Context` parameter on the combine.

*Verified by:* `client/…/FlowGuardrailsTest`, `client/…/OneOfArmsTest`,
`client/…/ForEachAccessorTest`, `tests/DynamicConstructsTest`, `tests/ChooseTest`,
`tests/LoopBudgetTest`, `tests/NestedScopesTest`.

## 3. Compilation

**WGL-AUTH-080** (MUST) Node ids MUST be generated as `prefix + (++counter)` with a counter monotonic
across the whole workflow, and prefixes `end`, `dynfork`, `fork`, `join`, and `n` for everything
else.

**WGL-AUTH-081** (MUST) A step name MUST be reserved (and duplicates rejected) by task, predicate,
combine, signal, sub-workflow and dyn-fork nodes; sleep, fork, join and end nodes MUST NOT reserve a
name. A null or blank name is `IllegalArgumentException("step name is required")`.

**WGL-AUTH-082** (MUST) A node's queue is its own, else the current `defaultQueue`, which starts as
the **workflow name**. Only task, predicate and combine nodes contribute to the definition's queue
set.

**WGL-AUTH-083** (MUST) A node's retry policy is its own, else the workflow default.

**WGL-AUTH-084** (MUST) `activity` MUST be `<workflow>#<name>` for task, predicate and combine
nodes, the child workflow name for a sub-workflow node, and null otherwise.

**WGL-AUTH-085** (MUST) `startNode` is the first node attached to the root chain. A workflow that
records no node MUST be rejected at build (`"workflow defines no steps"`).

**WGL-AUTH-086** (MUST) Build MUST close the open frontier into a fresh successful `END` node.

**WGL-AUTH-087** (MUST) A gate's false edge MUST point at the enclosing join when the gate is inside
a branch, and otherwise at a fresh `END(success, reason = "gated:<name>")`.

## 4. Compile-time validation

**WGL-AUTH-090** (MUST) After assembly, every non-null `next` / `altNext` MUST reference an existing
node.

**WGL-AUTH-091** (MUST) `TASK`, `SLEEP`, `JOIN`, `SIGNAL` and `SUB_WORKFLOW` MUST each have a
successor; a `PREDICATE` MUST have both `next` and `altNext`; a `FORK` MUST have at least two
branches; a `DYN_FORK` MUST have a `next`, exactly one branch template, and non-null `itemsKey` and
`itemKey`.

**WGL-AUTH-092** (MUST) These MUST be rejected with `IllegalArgumentException`: a blank or duplicate
step name, a blank workflow name, a fork with fewer than two branches, a negative sleep or timeout,
an escalation branch without a timeout, and a nested body that defines no steps.

**WGL-AUTH-093** (MUST) These MUST be rejected with `IllegalStateException`: an empty workflow at
build, a missing successor, a malformed fan-out, an unknown edge target, a second `build()`, a fork
left un-combined, and `checkpoint()` in a position that is not immediately after a step.

*Verified by:* `client/…/PipelineTest`, `client/…/FlowGuardrailsTest`,
`client/…/CompensationDeclarationTest`.

## 5. Versioning

**WGL-AUTH-100** (MUST) The fingerprint MUST be SHA-256 over the canonical JSON of
`{name, startNode, nodes sorted by id, executionMode, checkpoints sorted}`, rendered lowercase hex.
The algorithm identifier is `sha256-canonical-v1` and MUST be stored beside the fingerprint.

**WGL-AUTH-101** (MUST) The fingerprint MUST NOT include the version. The version is the author's
declaration; the fingerprint only answers "is this the same graph as the stored one".

**WGL-AUTH-102** (MUST) The execution mode and the checkpoint set are part of the fingerprint, so
changing either requires a new version and an in-flight instance can never change mode.

**WGL-AUTH-103** (MUST) Registering a definition whose `(name, version)` is unknown MUST store it.
Registering an identical graph under a published version MUST be a no-op. Registering a **different**
graph under a published version MUST be refused with `FAILED_PRECONDITION` (engine status 409),
unless forced and permitted ([WGL-OPS-030](90-ops.md)).

**WGL-AUTH-104** (MUST) A stored fingerprint written by a different algorithm MUST be treated as
unknown and upgraded in place, never as a mismatch.

**WGL-AUTH-105** (MUST) "Latest" MUST mean the **highest** declared version, not the most recently
registered one, so registration order cannot change which version a start or a worker binds.

**WGL-AUTH-106** (MUST) An instance records its version at start and MUST run that exact graph to
completion regardless of later registrations.

**WGL-AUTH-107** (MUST) `start` without a version uses the latest at the moment of the start;
`start` with a version pins that graph.

*Verified by:* `tests/VersioningTest`, `postgres/PostgresRegistrationTest`.

## 6. Context semantics

**WGL-AUTH-110** (MUST) The context is opaque JSON to the engine. No schema is declared, validated
or migrated by the server.

**WGL-AUTH-111** (MUST) A task handler's return value **replaces** the context at that point; it does
not accumulate into it.

**WGL-AUTH-112** (MUST) A top-level key absent from a returned context MUST be removed, not persisted
as a JSON null.
*Verified by:* `tests/ContextNullDeleteTest`.

**WGL-AUTH-113** (MUST) A signal's payload and a completed sub-workflow's final context MUST merge
into the waiting scope rather than replace it.

**WGL-AUTH-114** (SHOULD) Context evolution SHOULD use a `@Decode` upcast deployed before the field
it defaults, since readers must precede writers.
