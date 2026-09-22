# Observed execution (`OBSERVED` mode)

Status: **server side implemented** (this document) · client instrumentation: separate module, separate PR

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

Only `TASK`, `PREDICATE` and `END` nodes, and no compensable steps. Registration refuses
anything else with a 400: a sleep, fork, join, signal or sub-workflow needs the server to run
it, and there is no server-side run for something that already happened. Fan-out (`fork`,
`forEach`) is deferred until run correlation across threads is solved on the reporting side.

The mode is declared like the others, and like the others it is part of the version's
fingerprint:

```java
FlowSpec spec = FlowSpec.define("checkout", 1, Ctx.class, Steps.class, (f, s) -> f
        .execution(ExecutionMode.OBSERVED)
        .thenApply(s::validate)
        .thenFilter(s::inStock)
        .thenApply(s::charge));
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

- The first report of a run mints the instance. Its one token is created already **held by the
  reporter** (`RUNNING`, lease owner = `reporter`, expiry = never), which is what keeps it out
  of every poll and every lease-reclaim sweep. There is never a `READY` token in an observed
  instance.
- Each step is applied as the shared step machinery would (route, loop budget, settle), then the
  continuation is held the same way. An `END` successor is driven, which is the only pump an
  observed instance ever sees.
- Anomaly kinds and what follows each:

| kind | when | then |
|---|---|---|
| `OUT_OF_ORDER` | reported node ≠ the node the held token is at | held token cancelled, run resynchronised at the reported node |
| `UNKNOWN_NODE` | no such step in the graph | step skipped |
| `AFTER_END` | steps arrive once the instance is terminal | rest of the report ignored |
| `INCOMPLETE` | `final` while still `RUNNING` | instance `FAILED` ("run ended before END, at …") |

- A step reported with `error` fails its token (`FAILED`, `last_error`) and the instance
  (`<step>: <error>`), no retry: the code already threw.
- The reply carries the instance id, its status, and how many anomalies that report added.

## 5. Storage

Migration 15: `wf_token.started_at` / `finished_at` (nullable), an index for the duration
sample, and `wf_anomaly` (instance, workflow, version, kind, expected/reported node, detail,
time). Duration statistics are computed in the server over the newest N timed `DONE` tokens of
a version (default 10 000), so no percentile SQL has to be portable.

## 6. Reporting side

Lives in its own module so the existing client API is untouched. The plan: a proxy over the
flow's step interface that records entry/exit per call, a thread-scoped run opened explicitly or
implicitly at the start node, fire-and-forget flushing through a bounded queue on `END`, a size
threshold or a short linger, and context capture off by default (a `null` merge leaves the
context untouched).
