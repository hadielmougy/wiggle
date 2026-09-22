# Observed execution (`OBSERVED` mode)

Status: **implemented** -- server side (§2-5) and the `observe` reporting module (§6)

## 1. What it is

Every other execution mode has the server hand work to a worker. `OBSERVED` inverts that: the
steps run inside your own services, on your own threads, and each service reports the steps it
completed, naming the run they belong to. The server never dispatches, never leases to a worker,
and never blocks a service. What it does instead is:

- **Conformance.** Every report is appended to the run its key names; once the run has settled,
  the server judges it against the declared topology and records each departure as an
  **anomaly** rather than refusing it.
- **Timing.** Each step carries its own `started_at` / `finished_at`, measured where it ran.
  The server keeps them on the token and answers per-node duration statistics (count, mean,
  p50, p95, max) over a bounded sample, slowest p95 first, so the head of the list is the
  bottleneck.

The unique value over a tracer is the first point: a declared model to check runs against.

## 2. What an observed graph may contain

Steps, predicates, static forks and joins, and ends; no compensable steps. Registration refuses
anything else with a 400: a sleep, signal, sub-workflow or runtime fan-out (`forEach`) needs the
server to run it, and there is no server-side run for something that already happened.

A spec never declares this mode: the DSL offers `executeInServer()`, `executeInLocalSync()` and
`executeInLocalAsync()`, and nothing else. OBSERVED is stamped on the definition by the observer
that publishes it, so a spec cannot be handed to a worker by mistake with a mode no worker
serves. Like the others, the mode is part of the version's fingerprint:

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
    string instance_id = 3;       // optional; the key is what identifies the run
    string correlation_id = 4;    // the run's key
    string reporter = 5;          // the reporting service
    repeated StepResult steps = 6;
    bool final = 7;               // the originator is done: a run still short of END is incomplete
}
```

`StepResult` gained an `error` outcome (the step threw: fails the run) and `started_at` /
`finished_at` in epoch millis. The two timing fields are honoured on `AdvanceRun` too, so
`LOCAL_SYNC` / `LOCAL_ASYNC` workers can report real per-step durations. All changes are additive.

## 4. Engine semantics

Reports append; judgement happens later. That is what lets several services report one run in
any order.

- **A run is keyed.** The instance id is derived from `(workflow, correlation id)`, so every
  reporter of a run lands on the same instance whether it reports first or last. Two reporters
  creating it at once collide on the primary key and the loser reads the winner's row. A blank
  key mints a random one: that run is single-reporter by construction.
- **A report only appends.** Each step becomes a settled, timed token carrying its reporter and
  the order it was reported in, which breaks ties between steps whose clocks agree to the
  millisecond. A step whose successor is END also writes the END token, which marks the run as
  closing. A step the graph does not know is recorded as `UNKNOWN_NODE` at once. A step reported
  with an error also marks the run closing; once it settles, the run fails with that step's error
  (`<step>: <error>`), so another service's earlier steps that land a moment later still belong
  to the run rather than reading as `AFTER_END`.
- **Settling.** Every report pushes the run's settle time out by the stall threshold
  (`WIGGLE_OBSERVE_STALL_MILLIS`, default 10 min). Reaching END, or a report marked `final`,
  pulls it in to a short grace (`WIGGLE_OBSERVE_SETTLE_MILLIS`, default 5 s) so stragglers from
  other services still land. The leader's housekeeping tick judges runs whose settle time has
  passed.
- **Judgement** sorts the run's steps by their own clock and walks a frontier -- the steps the
  graph expects next. A predicate's value picks its branch, a fork releases every branch head, a
  join releases its successor once every branch arrived, END closes the run. Interleaved branches
  are all in the frontier, so fan-out across services raises nothing.

| kind | when | then |
|---|---|---|
| `UNKNOWN_NODE` | no such step in the graph (at arrival) | step skipped |
| `AFTER_END` | steps arrive once the instance is terminal (at arrival) | tokens kept for their timings |
| `DUPLICATE` | a step already consumed runs again and lies on no cycle | ignored; at-least-once delivery, most likely |
| `OUT_OF_ORDER` | a step outside the frontier, not a duplicate | frontier resynchronised at that step |
| `INCOMPLETE` | judged without END reached | instance `FAILED` ("run ended before END, at …") |
| `STALLED` | judged because the run went quiet, not because END or `final` arrived | with `INCOMPLETE` |

- **Verdict.** A successful END reached means `COMPLETED`, whatever was recorded along the way;
  anomalies are findings, not failures. A failing END fails with its reason. No END fails as
  incomplete.

## 5. Storage

Migration 15: `wf_token.started_at` / `finished_at` / `seq` (nullable), an index for the
duration sample, `wf_instance.settle_at` (nullable, observed runs only) with its index, and
`wf_anomaly` (instance, workflow, version, kind, expected/reported node, detail, time). Duration
statistics are computed in the server over the newest N timed `DONE` tokens of a version
(default 10 000), so no percentile SQL has to be portable.

## 6. Reporting side: the `observe` module

`sh.wiggle:wiggle-observe` is its own module so the client API stays as it is: an observed
service publishes a topology and reports against it, and needs neither a worker nor the
instance API. It depends on `wiggle-client` for the flow DSL and owns its own connection.

One call is the whole contract: a service names the run's key, the step, and when it ran.
Nothing is opened or scoped, no thread is involved, and a run's first report creates it wherever
it comes from -- a request handled on one thread and its reply on another report with the same
key, and so do two services. Times are the caller's, epoch millis; `start(key, step)` returns a
timer that measures for you. Step names are validated against the published spec, so a typo
fails in the service rather than as an anomaly on the server.

<!-- snippet: observed/report -->
```java
try (Observer observer = Observer.connect("localhost:8080")) {
    ObservedFlow checkout = observer.publish(spec);        // stamps OBSERVED, registers, validates names

    checkout.record(orderId, "validate", startedAt, finishedAt);
    checkout.recordPredicate(orderId, "inStock", true, startedAt, finishedAt);
    checkout.recordError(orderId, "charge", "CardDeclined", startedAt, finishedAt);
}
```

`end(key)` says the originator is done. Reports are batched and sent behind the caller: a full
queue or a failed call drops the report and counts it in `observer.dropped()`, never blocks.
`ObserverOptions` names the reporter (`host@pid` by default), the linger and queue capacity, and
TLS with the client's semantics.

Everything else -- code instrumentation, carrying a run across a broker, causal ordering across
services, observed compensation -- is built on this call, and comes later.
