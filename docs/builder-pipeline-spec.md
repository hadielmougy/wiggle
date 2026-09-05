# Builder → Pipeline specification

The requirements for the workflow DSL's build layer, from the fluent `WorkflowBuilder` surface down
to the assembled, validated, content-versioned `Blueprint`. An implementation that follows this
produces byte-identical `WorkflowDefinition`s (same content-version) to the current one.

The build layer is **topology only** — it declares named nodes and how they chain, branch, and
rejoin. No step logic and no context type live here; implementations are bound separately on a
worker via `@Handlers` classes, matched to the graph by name.

---

## 1. Output contract

`build()` returns `Blueprint(WorkflowDefinition definition)` — pure topology, **no** handler table,
no context type.

`WorkflowDefinition(String name, int version, String startNode, Map<String,Node> nodes,
Set<String> queues, ExecutionMode executionMode, Set<String> checkpoints)`.

- `nodes` — keyed by node id.
- `queues` — the set of queues worker-dispatched nodes poll.
- `version` — content hash (see §3.7); deterministic from the graph, not caller-supplied.

## 2. Graph model (`Node`)

One flat record with nullable slots per kind:
`id, kind, name, activity, queue, retry, sleepMillis, next, altNext, branches, expected, success,
reason, itemsKey, itemKey`.

Edges: **`next`** = primary / true / delivery edge; **`altNext`** = false / escalation edge;
**`branches`** = fork branch-start ids (or the single dyn-fork template). A node is created
edge-less; edges are wired afterward.

| kind | set at create | edges used | reserves a unique name? | in `queues`? | `activity` |
|---|---|---|---|---|---|
| TASK (step/effect) | name, activity, queue, retry | `next` | **yes** | yes | `wf#name` |
| TASK combine | + `itemsKey` = JSON array of arm names | `next` | yes | yes | `wf#name` |
| PREDICATE (gate/guard/cond) | name, activity, queue, retry | `next`, `altNext` | **yes** | yes | `wf#name` |
| SLEEP | name, `sleepMillis` | `next` | **no** (sleep names needn't be unique) | no | null |
| SIGNAL | name, `sleepMillis` = deadline | `next`, `altNext` | **yes** | no | null |
| SUB_WORKFLOW | name, `activity` = child wf | `next` | **yes** | no | child wf name |
| DYN_FORK | name, `itemsKey`, `itemKey` | `next` (empty-list path), `branches` (1 template) | **yes** | no | null |
| FORK | name `"fork"` | `branches` | no | no | null |
| JOIN | name `"join"`, `expected` | `next` | no | no | null |
| END | `success`, `reason` | — | no | no | null |

`isWorkerDispatched()` = TASK or PREDICATE (the only kinds a worker polls/serves).

## 3. `Pipeline` responsibilities (storage + assembly)

**3.1 Node registry** keyed by a generated id. **Id = prefix + (++counter)**, prefixes: `end`,
`dynfork`, `fork`, `join`, everything else `n`. The counter is monotonic across the whole workflow.

**3.2 Name reservation** — a `Set<String> stepNames`. A null/blank name →
`IllegalArgumentException("step name is required")`; a duplicate →
`IllegalArgumentException("duplicate step name '…'")`. Reserved by: task, predicate, combine,
signal, sub-workflow, dyn-fork. **Not** reserved by: sleep, fork, join, end.

**3.3 Queue** — a node's own queue, else `defaultQueue`. `defaultQueue` starts as the **workflow
name**; `defaultQueue(q)` overrides it for subsequently-added nodes. Only task / predicate / combine
add their resolved queue to the `queues` set.

**3.4 Retry** — a node's own `RetryPolicy`, else the workflow `defaultRetry`. Constructor:
`defaultRetry = given ?? RetryPolicy.forever()`. The DSL entry `Workflow.define(name)` supplies
`RetryPolicy.exponential(3, 500ms)` as that default; `define(name, retry)` overrides.

**3.5 startNode** — recorded once, for the first node attached to the *root* stream. (This is the
only thing the "start" callback does.)

**3.6 Edge wiring** mutates the stored node: `wireNext(from,to)`, `wireAlt(from,to)`,
`setBranches(forkId, starts)`.

**3.7 `build()`**:
1. if `startNode == null` → `IllegalStateException("workflow defines no steps")`.
2. `version = contentVersion(name, startNode, nodes, executionMode, checkpoints)`.
3. validate (§3.8).
4. return `Blueprint(new WorkflowDefinition(...))`.

**contentVersion** (must match exactly for stable versions): SHA-256 over the **canonical JSON** of
`{name, startNode, nodes:[node.toJson() sorted by id], executionMode:name(), checkpoints:sorted}`;
take bytes[0..3] as a big-endian int with the top bit cleared (`byte[0] & 0x7f`); if the result is 0,
use 1. `node.toJson()` omits null / zero / empty fields (replicate `Node.toJson`'s inclusion rules
exactly — it feeds the hash).

**3.8 Validation** (per node, after assembly):
- every non-null `next` / `altNext` must reference an existing node.
- TASK, SLEEP, JOIN, SIGNAL, SUB_WORKFLOW: must have a `next` (`"node … has no successor"`).
- PREDICATE: must have both `next` and `altNext` (`"predicate … has no false branch"`).
- FORK: `branches.size() >= 2` (`"fork … has fewer than two branches"`).
- DYN_FORK: `next` present; exactly 1 branch template; `itemsKey` and `itemKey` non-null.
- END: nothing.

## 4. `WorkflowBuilder` responsibilities (shape)

**4.1** Owns the **frontier** (the set of open edges — node + which edge) and threads it as operators
append nodes. It never exposes that state; §5 lists what each operator does to it (append / start /
gate false-edge / fork reopen / doWhile / choose accumulate / signal escalation / build-close vs
empty-reject).

**4.2 Root vs branch** — the root stream reports its first node as `startNode`; a nested sub-stream
(fork branch, choose case, loop body, escalation branch) reports its first node back to its parent
(so the parent can wire into it) and carries an **enclosing-join id** that a false gate
short-circuits to.

**4.3 Sub-stream build** — run the branch body against a fresh child builder; require it defined ≥1
node (else `IllegalArgumentException("<what> defines no steps")`); require it isn't a fork left
un-combined (`IllegalStateException("… has a fork(...) with no combine()")`); expose its start id and
its tail's open frontier.

**4.4 forkPending** — set true by `fork(...)`, cleared by `combine(...)`. Building or nesting while
pending → the errors above / build error
`"a fork(...) has no merge: follow it with combine(...)"`.

**4.5 checkpoint()** — marks the last step id as a checkpoint; valid only immediately after a
step/effect/gate (`lastStepId != null`), else `IllegalStateException`. Track `lastStepId`; reset to
null after sleep / signal / subWorkflow / fork / forkEach / doWhile / choose.

**4.6 build()** — reject double-build (`consumed`); reject `forkPending`; close the frontier into a
fresh success END node; delegate to `Pipeline.build()`.

## 5. Operators (public API + graph produced + frontier effect)

Entry: `Workflow.define(name)` / `define(name, RetryPolicy)` → `WorkflowBuilder`.

| operator (signatures) | nodes / edges created | frontier effect |
|---|---|---|
| `step(name)` `step(name,queue)` `step(name,retry)` `step(name,retry,queue)`; `then(name)`; `effect(...)` (same 4) | one TASK | wire frontier → node; frontier = {node.next} |
| `gate(name)` `(…,queue)` `(…,retry)` `(…,retry,queue)` | one PREDICATE; **its false edge** → enclosing-join if in a branch, else a fresh `END(reason="gated:<name>")` | wire frontier → node; frontier = {node.next (true)} |
| `sleep(Duration)`; `sleep(name,Duration)` (neg → IAE) | one SLEEP | append; frontier = {next} |
| `awaitSignal(name)`; `(name,timeout)`; `(name,timeout,escalation)` (timeout<0 → IAE; escalation w/o timeout → IAE) | one SIGNAL (`sleepMillis` = timeout or 0). With escalation: build the escalation sub-stream; `signal.altNext` → escalation start | append; frontier = {signal.next} **plus** the escalation sub-stream's open ends (both stay open) |
| `subWorkflow(name, workflow)` | one SUB_WORKFLOW (activity = child) | append; frontier = {next} |
| `fork(Branch...)` (≥2 else IAE) → `ForkStage`; `ForkStage.combine(name)` → builder | FORK; JOIN(expected = #branches); each branch built as a sub-stream whose tail **closes into** the JOIN; `setBranches(fork, starts)`; a combine TASK with `itemsKey` = arm names; `JOIN.next` → combine | consume frontier into FORK; after branches, frontier = {combine.next} |
| `forkEach(name, itemsKey, itemKey, body)` | DYN_FORK(itemsKey,itemKey); JOIN(expected = 0); one branch template built as sub-stream closing into JOIN; `setBranches(dynfork,[template])`; `dynfork.next` → JOIN (empty-list path) | consume frontier into DYN_FORK; frontier = {join.next} |
| `doWhile(conditionName, body)` | body sub-stream; PREDICATE cond; `cond.next` → body start (loop); body tail closes into cond | enter body (frontier → body start); frontier = {cond.altNext (exit)} |
| `choose(Case...)` — `Case.when(name, body)`, `Case.otherwise(name, body)` (otherwise must be last & not alone) | one PREDICATE per guarded case; `guard[i].altNext` → `guard[i+1]`; each `guard.next` → its case branch start; `otherwise.altNext` → its branch | enter first guard (frontier emptied); **accumulate** every case branch's open ends; if no `otherwise`, also keep the last guard's `altNext` open |
| `defaultQueue(q)`, `execution(ExecutionMode)`, `checkpoint()` | workflow-level settings | — |
| `build()` | close frontier into a fresh success END | consumes the builder |
| `Branch.of(name, body)` | value: (name, `UnaryOperator<WorkflowBuilder>`) | — |

**Notes:**
- **Fixed edges** — the guard cascade `altNext`, the loop-back `cond.next → body`, each case's
  `guard → branch`, the signal's `altNext → escalation` — are wired **directly on the node**; they
  are not frontier operations.
- **Enter vs close** — entering a construct's first node (fork / guard-cascade / loop-body) reports a
  start if the frontier is empty (so a workflow that *starts* with that construct is rooted there);
  closing into a terminal (a JOIN, or the END) wires only existing ends and reports **no** start — so
  an empty workflow stays `startNode == null` and `build()` rejects it. This distinction is mandatory.

## 6. Errors to preserve

`IllegalArgumentException`: blank / duplicate step name; blank workflow name; `fork` with <2
branches; negative sleep / timeout; escalation without timeout; a sub-stream that defines no steps.

`IllegalStateException`: empty workflow at build; predicate / task with no successor; fork <2 /
dyn-fork malformed at validate; unknown edge target; double `build()`; `combine` already applied /
fork left un-combined; `checkpoint()` not after a step.

## 7. Constants / defaults

- id prefixes: `end` / `dynfork` / `fork` / `join` / `n` + counter.
- `activity` = `"<workflow>#<name>"` (task / predicate / combine); child workflow name
  (sub-workflow); else null.
- default queue = workflow name; default retry = `exponential(3, 500ms)` (via `define(name)`), else
  `forever()`.
- `ExecutionMode`: `SERVER`, `LOCAL_SYNC`, `LOCAL_ASYNC`, `DEFAULT`.
- combine node = a TASK whose `itemsKey` is `Json.write(List<armName>)`.
