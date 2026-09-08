# Durable Workflows Without Replay: Treating the Workflow as Data, Not Code

*The design of [Wiggle](https://wiggle.sh), an open-source workflow engine with cell-based sharding.*

## Key Takeaways

- Replay-based durable execution engines tax every workflow with a determinism contract: workflow
  code must produce identical decisions on re-execution, and violations surface only in
  production, during recovery — the worst possible moment.
- Storing the workflow as a **compiled graph** — data the server owns, rather than code the
  server replays — removes the determinism requirement entirely. Step handlers are plain
  functions that run exactly when their step runs, and never again.
- The trade-off is real and should be stated plainly: control flow is bounded by the graph
  DSL's operators. Workflows that need arbitrary, Turing-complete orchestration logic are
  better served by replay engines, which earn their tax.
- Once the workflow is data, making **every state transition explicit** follows naturally: a
  step's return *replaces* the state, parallel branches merge only through a hand-written
  combine step, and nothing is ever folded together implicitly. This eliminates a class of
  merge-surprise bugs that otherwise hide until two branches touch the same field.
- A graph-based engine can be operationally small — one JAR plus one relational database — while
  sustaining ~91,000 durable step completions per second embedded in a single JVM, and ~300
  workflow starts per second with sub-second completion latency on a laptop-hosted Kubernetes
  cluster.

## The determinism tax

Durable execution — the promise that a long-running business process survives crashes, restarts,
and redeployments, resuming exactly where it left off — has largely converged on one
implementation strategy: **event-sourced replay**. Engines in the Temporal/Cadence lineage
persist a history of events for each workflow. When a worker needs to make progress on a
workflow it has never seen (say, after a crash), it *re-executes the workflow function from the
beginning*, feeding recorded results back into it, until the code catches up with history and
takes its next real action.

It is a genuinely elegant trick, and it buys enormous expressiveness: your workflow is ordinary
code — loops, conditionals, local variables, whatever your language offers.

But the trick has a price, and every team that operates such a system eventually pays it. For
replay to work, the workflow function must be **deterministic**: given the same history, it must
make exactly the same decisions, in exactly the same order, every time. That outlaws — inside
workflow code — random numbers, clock reads, iteration over unordered collections, config
lookups, library calls that might change behavior between versions, and, most painfully,
*ordinary code changes*. Deploying a modified workflow while instances are in flight requires
version-patching APIs and discipline, because the new code must still replay old histories
faithfully.

The failure mode is what makes this a tax rather than a fee: **non-determinism does not fail at
the point of the mistake**. The workflow runs fine — until a worker somewhere replays it and the
re-execution diverges from history. The bug detonates during recovery, in production, possibly
weeks after it was introduced, in a workflow that "worked in every test."

The question worth asking is: what exactly is being replayed, and why? The answer is *control
flow*. The engine replays your code because your code is the only representation of the
workflow's structure it has. Which suggests an alternative: give the engine the structure
directly.

## The workflow as data

Wiggle's central bet is that a business process can be described as a **graph** — named steps and
how they chain, branch, and rejoin — and that this graph, not any function, is what the server
should own. A definition is built with a small, stream-flavored DSL:

```java
Blueprint orders = Workflow.define("order-fulfilment")
        .step("validate")
        .gate("in-stock")                    // false ⇒ the instance ends cleanly
        .fork(
            Branch.of("payment", s -> s
                .step("authorise", RetryPolicy.exponential(5, Duration.ofMillis(100)))
                .step("capture")),
            Branch.of("shipping", s -> s
                .step("reserve-stock")
                .step("print-label")))
        .combine("merge")                    // branches rejoin at an explicit merge step
        .step("notify")
        .build();
```

Nothing in that snippet executes. `build()` compiles a graph — nodes with kinds (task, gate,
fork, join, timer, signal wait, sub-workflow), edges, retry policies — and registering it ships
that graph to the server as data. A running workflow instance is then a set of **tokens**
positioned on the graph, in the spirit of a Petri net: a fork mints one token per branch, a join
consumes them, and the instance is terminal when no token remains active. Every token is a row
in a database.

Recovery, in this model, is almost embarrassingly boring. There is no history to replay and no
function to re-execute, because the engine never lost the state in the first place — the state
*is* the rows. A crashed server restarts and reads where every instance stands. A crashed worker
is handled by **leases**: a claimed step carries a lease; when the worker dies mid-step, the
lease expires and the step is redelivered to another worker. Execution is at-least-once at the
step level, dispatch is exactly-once, and no code anywhere is subject to a determinism contract
— because no code is ever executed twice *for the engine's benefit*.

The step logic lives in ordinary classes, matched to the graph **by name**:

```java
@Handlers("order-fulfilment")
class OrderHandlers {
    public Order   validate(Order o)  { return o.withStatus("VALIDATED"); }
    public boolean inStock(Order o)   { return o.quantity() > 0; }   // a gate: boolean return
    public Order   authorise(Order o) { return o.withPaymentRef(auth(o)); }
    // ...
}
```

A method's *signature* defines its step kind — a boolean return is a gate, `void` is a side
effect, anything else is a task whose return value becomes the new state. Handlers can freely
use clocks, randomness, and any library they like. They can be redeployed at will. Because
binding is by activity name over a language-neutral graph, the same workflow's steps can be
served by Java, Go, and Python workers simultaneously — one instance, three languages, no shared
SDK semantics beyond the wire protocol.

## What it costs

Symmetry demands the trade-off be stated as plainly as the benefit: **your control flow is
bounded by the DSL's operators.** Wiggle's vocabulary — sequential steps, gates, exclusive
choice, parallel fork/combine, dynamic fan-out over a collection, timers, external signals with
deadlines and escalation, sub-workflows, do-while loops — covers a large share of real business
processes. It does not cover all of them. If your orchestration logic is genuinely algorithmic —
control flow computed at runtime in ways no fixed operator set expresses — then a replay engine's
Turing-complete workflow code is the right tool, and its determinism tax is the fair price.

This is, we think, the honest way to frame the choice: replay engines make *code* durable and
tax it with determinism; graph engines make *data* durable and tax it with a vocabulary. Neither
tax is avoidable. You choose the one your workloads can afford.

## No implicit merges: explicitness as a design principle

Treating the workflow as data has a second-order consequence that turned out to shape the entire
engine: once state transitions are first-class, you must decide *exactly* what each one means —
and every softness in that definition becomes a bug factory.

Wiggle went through this crucible recently, and the resulting rules are strict:

**A step's return replaces the state.** A handler receives the workflow's context and returns
the next context — whole. Not a delta, not a patch; keys the handler omits are gone. There is no
diff-and-merge machinery anywhere in step execution, which means there is no question about what
the state is after a step: it is what the handler returned, byte for byte.

**Parallel branches never merge implicitly.** Each fork branch runs on an isolated copy of the
context; siblings cannot see each other's writes. The only path back to the shared state is the
**mandatory combine step** — a handler that receives every branch's final result plus the
pre-fork context, and returns the complete post-join state:

```java
Order merge(@Context Order base, @Arm("payment") Order pay, @Arm("shipping") Order ship) {
    return base.withPaymentRef(pay.paymentRef())
               .withShipmentRef(ship.shipmentRef());
}
```

An earlier version of the engine offered a "default union" that folded disjoint branch writes
automatically. It was removed deliberately: an automatic merge is a decision the engine makes on
the user's behalf, silently, and last-write-wins races are exactly the class of bug that hides
until production. If two branches' results must become one state, someone must write the line of
code that says how.

**Dynamic fan-out maps elements, it doesn't mutate shared state.** The `forEach` operator spawns
one isolated branch per element of a collection; *the element itself is that branch's context* —
handlers receive the item, return the transformed item, and the engine collects the final values
(a list for a list input, a map keyed like the input for a map input) and hands the collection to
— again — an explicit combine. The frozen pre-loop context rides along ambiently (a thread-local
accessor, or a parameter; the handler's signature chooses), so per-item logic can read shared
data without any possibility of writing it.

The through-line: **in a data-first engine, the state transition rules are the product.** Every
place we replaced an implicit behavior with an explicit one, a category of "why does my context
look like this?" question disappeared.

## Versioning without patch APIs

Replay engines need versioning APIs because old histories must remain replayable by new code.
A graph engine gets versioning almost for free: a definition's version *is the content hash of
its graph*. Re-registering an identical graph is a no-op; registering a changed one creates a new
version, and in-flight instances simply continue on the version they started with — their tokens
reference it, and both graphs coexist in the database. No `patched()` calls, no version branches
inside workflow code, no migration windows.

## The second bet: cells instead of one big cluster

Making the workflow data also changes what scaling can look like. Wiggle's unit of scale is the
**cell**: a namespace maps to one or more cells, and each cell is a complete, independent
deployment — its own server cluster and, crucially, *its own database*. A small Raft-backed
coordinator assigns work across cells by consistent hashing over **epochs**: publishing a new
shard-to-cell ring is an epoch bump; new instances follow the new ring while in-flight instances
finish where they live, so **resharding never migrates data**. Routing is directory-free because
each instance ID embeds its namespace, epoch, and shard (`orders.e0.s3.01J…`) — any party can
compute the owning cell from the ID alone.

The operational meaning is blast-radius isolation of a kind logical namespaces cannot offer:
tenant A's database melting down cannot touch tenant B's, because they do not share one. This is
the cell-based architecture pattern applied to the workflow tier itself — and it composes with
the data-first model, because an instance's entire state lives in rows that belong,
unambiguously, to exactly one cell.

## What the numbers look like

Honest numbers, from honest hardware — all of the following on a single MacBook Pro (M2 Pro,
10 cores), methodology and charts in the repository:

- **The engine alone** (embedded in one JVM, in-memory store, an 8-step fork/join workflow):
  2,481 instances/sec with a server round-trip per step, and **11,478 instances/sec — about
  91,800 durable step completions/sec** — when workers chain same-queue steps locally with
  batched commits.
- **A real deployment** (kind-based Kubernetes cluster, PostgreSQL-backed cells, gRPC through
  port-forwards): **~300 workflow starts/sec sustained with sub-second end-to-end completion
  latency**; ~340/s survives a one-minute burst before backlog compounds. Every component —
  submitter, worker, Kubernetes, coordinator, cells, databases — shared those ten cores, so the
  figure is a floor. Scaling past a cell's ceiling is, by design, a matter of adding cells on
  separate hardware.

One measurement from that work is worth passing on regardless of engine choice: the same cluster
on databases bloated with ~500k retained finished instances showed roughly *twice* the latency
at the same load. Retention and purge cadence are capacity parameters, not housekeeping
afterthoughts.

## Choosing

If your processes are expressible as steps, branches, waits, and fan-outs — and in our
experience the overwhelming majority of order flows, onboarding pipelines, approval chains, and
document processes are — a data-first engine gives you durable execution with no determinism
contract, trivially redeployable handlers, polyglot workers, content-hash versioning, and an
engine small enough to read in an afternoon and embed in a test. If your orchestration is
irreducibly algorithmic, pay the replay tax knowingly and take the expressiveness.

The deeper lesson generalizes beyond workflow engines: **when a system replays code to
reconstruct state, every property of that code becomes a correctness constraint.** Moving the
authoritative representation out of code and into data shrinks the constrained surface to a
vocabulary you control — and makes everything the engine does to your state something you can
point at, name, and test.

---

*Wiggle is Apache-2.0 licensed. Docs, patterns, and benchmarks are at
[wiggle.sh](https://wiggle.sh); the engine, the cell coordinator, the benchmark tooling, and
idiomatic Go and Python clients are at
[github.com/hadielmougy/wiggle](https://github.com/hadielmougy/wiggle). Contributions are
welcome — the honest to-do list is in the README.*
