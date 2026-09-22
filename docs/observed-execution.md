# Observed execution (`OBSERVED` mode)

Status: **implemented** -- server side (§2-5) and the `observe` reporting module (§6)

## 1. What it is

Every other execution mode has the server hand work to a worker. `OBSERVED` inverts that: the
steps run inside your own application, on your own threads, and the application reports each
completed step to the server after the fact. The server never dispatches, never leases to a
worker, and never blocks the application. What it does instead is:

- **Conformance.** Each reported step is checked against the declared topology. A step reported
  where another was due, a step the graph has no node for, steps arriving after the instance
  ended, or a run that closes before reaching `END` are each recorded as an **anomaly** rather
  than refused, and the run is resynchronised to where the report says it is, so the rest of
  the run still yields data.
- **Timing.** Each step carries its own `started_at` / `finished_at`, measured where it ran.
  The server keeps them on the token and answers per-node duration statistics (count, mean,
  p50, p95, max) over a bounded sample, slowest p95 first, so the head of the list is the
  bottleneck.

The unique value over a tracer is the first point: a declared model to check runs against.
Durations alone are something OpenTelemetry already gives you; use both.

## 2. What an observed graph may contain

Steps, predicates, static forks and joins, and ends; no compensable steps. Registration refuses
anything else with a 400: a sleep, signal, sub-workflow or runtime fan-out (`forEach`) needs the
server to run it, and there is no server-side run for something that already happened.

A spec never declares this mode: the DSL offers `executeInServer()`, `executeInLocalSync()` and
`executeInLocalAsync()`, and nothing else. OBSERVED is stamped on the definition by the observer
that publishes it, so a spec cannot be handed to a worker by mistake with a mode no worker
serves. A spec that names one of the worker modes is refused by the observer. Like the others,
the mode is part of the version's fingerprint:

<!-- snippet: observed/topology -->
```java
FlowSpec spec = FlowSpec.define("checkout", 1, Order.class, CheckoutSteps.class, (f, s) -> f
        .thenApply(s::validate)
        .thenFilter(s::inStock)
        .thenApply(s::charge));   // no execution mode: an observer stamps OBSERVED when it publishes
```

## 3. Wire protocol

```proto
rpc ObserveRun(ObserveRunRequest) returns (ObserveRunResult);
rpc ObserveMany(ObserveManyRequest) returns (ObserveManyResult);   // N runs, each applied alone
rpc GetStepStats(StepStatsRequest) returns (StepStats);
rpc ListAnomalies(ListAnomaliesRequest) returns (AnomalyList);

message ObserveRunRequest {
    string workflow = 1;
    int32 version = 2;            // 0 = latest
    string instance_id = 3;       // blank on a run's first report; the result carries the id
    string correlation_id = 4;    // business key, first report only
    string reporter = 5;          // the observing process
    repeated StepResult steps = 6;
    bool final = 7;               // the run is over: still RUNNING afterwards = INCOMPLETE
}
```

`StepResult` gained an `error` outcome (the step threw: fails the instance, as a worker's
`FailTask` would) and `started_at` / `finished_at` in epoch millis. The two timing fields are
honoured on `AdvanceRun` too, so `LOCAL_SYNC` / `LOCAL_ASYNC` workers can report real per-step
durations instead of the one collapsed server timestamp a flushed batch used to leave.

The reporting side converts a monotonic clock to epoch millis at flush time: what matters is
the difference between the two fields, never their agreement with the server's clock.

## 4. Engine semantics

Reports append; judgement happens later. That is what lets several services report one run in
any order.

- **A run is keyed.** The instance id is derived from `(workflow, correlation id)` -- a key
  names one run of a workflow, across its versions -- so every reporter of a run lands on the
  same instance whether it reports first or last. Two reporters creating it at once collide on
  the primary key and the loser reads the winner's row. A blank key mints a random one: that run
  is single-reporter by construction.
- **Across cells.** Under a coordinator the derived id is `namespace.e<epoch>.s<shard>.<digest>`
  with no cell label: the key hashes into a fixed shard space (4096) that the epoch's ring wraps
  by modulo, so every cell derives the same id and the coordinator resolves the key
  (`Resolve` by `run_key`) to the one cell that owns that shard. A reporter under a coordinator
  sends each run there. Known limit: a reshard between two services' first reports of one run
  splits that run across epochs.
- **A report only appends.** Each step becomes a settled, timed token carrying its reporter. A
  step whose successor is END also writes the END token, which marks the run as closing. A step
  the graph does not know is recorded as `UNKNOWN_NODE` at once. A step reported with an error
  fails the run on the spot (`<step>: <error>`), no retry: the code already threw.
- **Settling.** Every report pushes the run's settle time out by the stall threshold
  (`WIGGLE_OBSERVE_STALL_MILLIS`, default 10 min). Reaching END, or a report marked `final`,
  pulls it in to a short grace (`WIGGLE_OBSERVE_SETTLE_MILLIS`, default 5 s) so stragglers from
  other services still land. The leader's housekeeping tick judges runs whose settle time has
  passed.
- **Judgement** sorts the run's steps by their own clock (arrival order breaks ties) and walks a
  frontier -- the steps the graph expects next. A predicate's value picks its branch, a fork
  releases every branch head, a join releases its successor once every branch arrived, END closes
  the run. Interleaved branches are all in the frontier, so fan-out across services raises nothing.

| kind | when | then |
|---|---|---|
| `UNKNOWN_NODE` | no such step in the graph (at arrival) | step skipped |
| `AFTER_END` | steps arrive once the instance is terminal (at arrival) | tokens kept for their timings |
| `DUPLICATE` | a step already consumed runs again and lies on no cycle | ignored; at-least-once delivery, most likely |
| `OUT_OF_ORDER` | a step outside the frontier, not a duplicate | frontier resynchronised at that step |
| `INCOMPLETE` | judged without END reached | instance `FAILED` ("run ended before END, at …") |
| `STALLED` | judged because the run went quiet, not because END or `final` arrived | with `INCOMPLETE` |

- **Causal hints.** A step may name the step that caused it (`after_node`). Within a service
  that is the previous step, which arrival order already covers; at a message boundary the
  sender's last completed step travels in the message (`wiggle-after`) and becomes the
  receiver's first step's cause. Before the frontier walk the judge orders by clock, then moves a
  step after its named cause where the graph agrees the cause precedes it -- a hint naming a
  sibling branch, an unknown node, or a step never reported is ignored, so a lost report never
  blocks judgement. Two services with disagreeing clocks then judge clean.
- **Verdict.** A successful END reached means `COMPLETED`, whatever was recorded along the way;
  anomalies are findings, not failures. A failing END fails with its reason. No END fails as
  incomplete.
- **Context.** Off unless the reporter ships it; a shipped return value replaces the instance
  context, last writer wins.

## 5. Storage

Migration 15: `wf_token.started_at` / `finished_at` (nullable), an index for the duration
sample, and `wf_anomaly` (instance, workflow, version, kind, expected/reported node, detail,
time). Migration 16: `wf_instance.settle_at` (nullable, observed runs only) and its index; migration 17:
`wf_token.after_node` (nullable), the causal hint. Duration statistics are computed in the server over the newest N timed `DONE` tokens of
a version (default 10 000), so no percentile SQL has to be portable.

## 6. Console

The ops console's **Performance** tab reads both RPCs. Pick a workflow and a window (last 15
minutes to everything sampled): the diagram rings each step by its share of the slowest p95 and
labels it with p95 and run count, the table below ranks steps by p95 with a share bar, and the
anomaly list shows every departure newest first; clicking one opens the instance. Under a
coordinator the console asks every cell and merges: counts add up, means are weighted, and a
merged row keeps the worst cell's p50 and p95, since percentiles cannot be recombined exactly.

## 7. Reporting side: the `observe` module

`sh.wiggle:wiggle-observe` is its own module so the client API stays as it is: an observed
application publishes a topology and reports against it, and needs neither a worker nor the
instance API. It depends on `wiggle-client` for the flow DSL and owns its own connection.

<!-- snippet: observed/usage -->
```java
try (Observer observer = Observer.connect("localhost:8080")) {
    Observed<CheckoutSteps> checkout = observer.observe(spec, CheckoutSteps.class, new Checkout());
    CheckoutSteps s = checkout.steps();          // the application's own object, wrapped

    try (Run run = checkout.begin(orderId)) {    // a run under a business key
        Order o = s.validate(order);
        if (s.inStock(o)) s.charge(o);
    }
}
```

- **Wrapping.** `steps()` is a proxy over the flow's step interface around the application's
  implementation (which therefore implements that interface). Every call on a method that names
  a step is timed and recorded; a method that names no step passes straight through. The
  application sees its own return values and its own exceptions, unwrapped.
- **Runs.** `begin(correlationId)` opens a run on the calling thread as its originator;
  `join(correlationId)` or `join(RunContext)` opens one as a participant in a run another
  service started; a step called with no run open opens an implicit one. Every run has a key
  (a blank one is minted), and every report names it, so all reporters of a run land on the
  same instance. An implicit run ends at `END`. A begun or joined run lives until it is closed:
  an originator closing before `END` reports the run as over (`INCOMPLETE` if nothing reached
  `END`); a participant's close only flushes. A step that throws ends its run.
- **Across services.** `Observation.context()` (or `run.context()`) is what to send along with
  a message: `wiggle-workflow`, `wiggle-version`, `wiggle-run`, and `wiggle-after` (the step
  completed last on this side, the receiver's first step's cause), via `RunContext.toHeaders()`. The receiving service reads them back with
  `RunContext.fromHeaders(...)` and joins. The Kafka adapter does both for you.
- **Across threads.** `run.wrap(runnable)` / `run.wrap(callable)` bind the run on whatever
  thread executes the task; `attach(run)` / `detach()` cover hand-offs those cannot express.
- **Inside a step.** `Observation.correlationId()`, `instanceId()` and `context()` read the
  current thread's run, the way a worker reads `Step`; outside a run they throw.
- **Reporting.** Steps buffer per run and flush when the buffer reaches `batchSize` (64), when
  it has waited `linger` (1 s), or when the run ends. One daemon thread sends them in
  `ObserveMany` calls, at most one batch per run per call so the second batch of a run can name
  the instance its first minted. The application never waits on the network: a full queue
  (`queueCapacity`, 10 000) or a failed call drops the report, marks the run lost, and counts it
  in `Observer.dropped()`.
- **Timing.** `started_at` is wall-clock at entry; `finished_at` is that plus the monotonic
  elapsed time, so a duration is never skewed by a clock adjustment mid-step.
- **Context.** Off by default. `withCaptureContext(true)` ships each task step's return value as
  the instance's context, through the same record-to-JSON mapping the client uses.
- **TLS.** `ObserverOptions.withTls(Tls.Options)` / `withRequireTls`, the client's semantics.

## 8. Kafka: `wiggle-observe-kafka`

A run crosses a topic as three record headers. The producing service stamps its run on the
record; the consuming service joins it around its handler. Only the record and header types of
`kafka-clients` are touched (the dependency is `compileOnly`; you bring your own Kafka), so the
same two calls work from a plain consumer loop, a Spring Kafka listener or a Quarkus one.

<!-- snippet: observe-kafka/produce -->
```java
try (Run run = checkout.begin((String) order.get("id"))) {
    checkout.steps().accept(order);
    producer.send(KafkaRuns.inject(new ProducerRecord<>("orders", (String) order.get("id"), "{...}")));
}
```

<!-- snippet: observe-kafka/consume -->
```java
KafkaRuns.handle(checkout, record, r -> checkout.steps().reserve(Map.of("id", r.key())));
```

A record with no run headers is still observed: keyed by the record key when it has one, else
by its topic, partition and offset, so a platform that never adopted correlation ids gets a run
per message. `KafkaRuns.inject` outside an open run throws rather than sending a bare record.
