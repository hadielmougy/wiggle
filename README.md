# Wiggle

A lightweight, embeddable **workflow engine for Java 21**. You describe a workflow with a
small `java.util.stream`-style DSL, run it on a server (embedded in your JVM or as a
standalone cluster), and process the steps with *workers* that pull work when they have
capacity.

- **Fluent DSL** — build a workflow as a chain of steps: `step`, `gate`, `choose`, `fork`, `sleep`.
- **Durable** — instances survive restarts; run in-memory for dev, or on Postgres for real.
- **Pull-based workers** — workers ask for work; the server never pushes. Backpressure is built in, and workers need no inbound connectivity.
- **Automatic retries, timers, and parallel fork/join**, with at-least-once execution and lease-based recovery when a worker dies.

**Current version: `2.1.0`** · Java 21+ · Apache-2.0

> New here, or looking for every configuration knob in one place? See
> **[docs/onboarding.md](docs/onboarding.md)** — onboarding + the full configuration reference.

---

## Install

Artifacts are published to Maven Central under `io.github.hadielmougy`.

**Gradle**

```kotlin
dependencies {
    // The DSL + worker + client — this is what your application needs.
    implementation("io.github.hadielmougy:wiggle-client:2.1.0")

    // Only if you embed the server in your own JVM (otherwise run it standalone).
    // The server core is database-agnostic; with no JDBC URL it uses the in-memory store.
    implementation("io.github.hadielmougy:wiggle-server:2.1.0")

    // For a real, multi-node deployment on PostgreSQL: add the database module (it plugs in
    // via a ServiceLoader SPI) plus the JDBC driver. The standalone server distribution
    // already bundles both.
    runtimeOnly("io.github.hadielmougy:wiggle-postgres:2.1.0")
    runtimeOnly("org.postgresql:postgresql:42.7.4")
}
```

**Maven**

```xml
<dependency>
  <groupId>io.github.hadielmougy</groupId>
  <artifactId>wiggle-client</artifactId>
  <version>2.1.0</version>
</dependency>
```

---

## Quick start

The fastest way to see it end to end — an embedded server, one worker, and a workflow
instance — all in one JVM:

```java
import dev.wiggle.client.dsl.*;
import dev.wiggle.client.worker.*;
import dev.wiggle.core.InstanceView;
import dev.wiggle.server.*;

import java.time.Duration;
import java.util.Map;

// 1. Define a workflow. Here the context is a plain Map; see below for typed records.
Blueprint<Map<String, Object>> greet = Workflow.defineJson("greet")
        .step("say-hello", ctx -> Map.of("greeting", "hello, " + ctx.get("name")))
        .build();

// 2. Start an embedded, in-memory server (great for dev and tests).
try (WiggleServer server = new WiggleServer(ServerConfig.fromEnvironment()).start();
     WiggleClient client = new WiggleClient(server.baseUrl())) {

    // 3. Run a worker that knows how to execute the steps.
    try (Worker worker = new Worker(client, "worker-1").register(greet)) {
        worker.start();

        // 4. Start an instance and wait for it to finish.
        String id = client.start(greet, Map.of("name", "ada"));
        InstanceView result = client.awaitCompletion(id, Duration.ofSeconds(10));

        System.out.println(result.status());   // COMPLETED
        System.out.println(result.context());   // {name=ada, greeting=hello, ada}
    }
}
```

Prefer to just run it? The repo ships a full example:

```bash
./gradlew :example:run          # embedded server + worker + a few orders, one JVM
```

---

## Defining a workflow

A workflow is a chain of steps. Nothing runs while you build it; `build()` produces a
`Blueprint` you register and start.

```java
Blueprint<Order> orders = Workflow.define("order-fulfilment", ContextCodec.records(Order.class))

        .step("validate", order -> order.withStatus("VALIDATED"))

        // A false guard ends the instance successfully — an empty stream, not an error.
        .gate("in-stock", order -> order.quantity() > 0)

        .fork(
                Branch.of("payment", s -> s
                        // Retry policy is an optional parameter on the step itself.
                        .step("authorise", Payments::authorise, RetryPolicy.exponential(5, Duration.ofMillis(100)))
                        .step("capture", Payments::capture)),

                Branch.of("shipping", s -> s
                        .step("reserve-stock", Stock::reserve)
                        .sleep("await-warehouse", Duration.ofMillis(300))
                        .step("print-label", Labels::print)))

        .step("notify", Notifier::send)
        .build();
```

### The operations

| Operation | What it does |
|---|---|
| `step(name, fn)` / `then(name, fn)` | run `fn` on a worker; its return value becomes the new context |
| `step(name, fn, retry)` | same, with an explicit `RetryPolicy` for that step |
| `effect(name, fn)` | run `fn` for a side effect; context unchanged |
| `gate(name, pred)` | continue only while `pred` is true; a false result ends the instance as `gated:<name>` |
| `choose(cases…)` | switch/case: run the branch of the **first** matching guard, then continue |
| `sleep(name, duration)` | wait on a server-side timer — **no worker is held** while waiting |
| `awaitSignal(name[, timeout[, escalation]])` | wait for a named external signal; optional deadline escalates or fails |
| `subWorkflow(name, workflow)` | run another workflow as a child; its result merges back, its failure fails the parent |
| `fork(branches…)` | run branches in parallel, then wait for all of them to finish (join) |
| `forkEach(name, itemsKey, itemKey, body)` | **runtime** fan-out: one parallel branch per element of the list at `itemsKey`, each seeing its element as `itemKey` (and `itemKey + "Index"`); empty list skips through |
| `doWhile(name, cond, body)` | run `body`, then re-run while `cond` holds (body runs at least once) |
| `onQueue(q)` / `defaultQueue(q)` | route steps to a dedicated worker pool |
| `build()` | finish; produces the `Blueprint` |

`step`, `effect`, and `gate` all accept an optional trailing `RetryPolicy` argument; omit it
to use the workflow's default policy.

### The context

The context is your workflow's data. It's the same type from the first step to the last:
`step` returns a new context of the same type (think `UnaryOperator<T>`, not
`Function<T,R>`). Two flavors:

- **Typed records** — `Workflow.define("name", ContextCodec.records(Order.class))`. Your
  steps take and return an `Order`. Records are immutable and serialize cleanly.
- **JSON maps** — `Workflow.defineJson("name")`. Steps take and return a
  `Map<String, Object>`. Handy when you don't want a dedicated type.

> **Parallel branches merge automatically.** Each step writes back only the fields it
> *changed*, so branches that touch different fields merge cleanly. If two branches write
> the same field, the later write wins.

### Branching: `choose` vs `fork`

- **`fork`** runs branches **in parallel** and waits for all of them (fan-out / join).
- **`choose`** is an exclusive **switch/case**: guards are tested in order and only the
  **first** match runs. Unmatched input falls through to `Case.otherwise(...)` if present,
  or straight to the next step. Exactly one branch runs.

```java
import static dev.wiggle.client.dsl.Case.*;   // when, otherwise

.choose(
        when("is-digital",  o -> o.type() == Type.DIGITAL,
                b -> b.step("grant-access", Fulfil::grantAccess)),

        when("is-physical", o -> o.type() == Type.PHYSICAL,
                b -> b.step("reserve", Stock::reserve)
                      .sleep("await-warehouse", Duration.ofMillis(300))
                      .step("ship", Shipping::send)),

        otherwise("backorder",
                b -> b.step("queue-backorder", Backorders::queue)))

.step("notify", Notifier::send)   // runs once, after the chosen branch
```

### Execution mode (local step chaining)

By default the server drives one step at a time: each step is a poll → execute → complete
round-trip. For step-heavy linear workflows you can let a worker **chain consecutive same-queue
steps locally** instead, cutting the round-trips and keeping the context in the worker between
steps:

```java
Workflow.defineJson("etl").execution(ExecutionMode.LOCAL_SYNC)
        .step("extract",  ...)
        .step("transform",...)
        .step("load",     ...)
        .build();
```

- `SERVER` (default) — server-driven, one step per claim.
- `LOCAL_SYNC` — the worker runs consecutive steps back-to-back, committing each to the server
  before the next. **As durable as `SERVER`** (a crash re-runs at most one step), just faster.
- `LOCAL_ASYNC` — the worker buffers up to `WorkerOptions.localBatchSize` steps and reports the
  run in **one** call at the handback boundary. Highest throughput (far fewer commits), at the
  cost of a wider *crash* blast radius — a killed worker loses the unflushed batch, which
  re-runs on recovery, so steps must be idempotent. Use `.checkpoint()` after a step to force it
  to commit before the next runs. A **graceful** `worker.close()` does not pay this cost: it
  drains any buffered steps to the server before returning, so a rolling deploy or scale-down
  loses nothing already computed — only an unclean process death does.
- The worker hands control back at any boundary — a `sleep`, `fork`, `join`, `awaitSignal`, a `subWorkflow`, a step
  on a different queue, a failure/retry, or the end — so those still coordinate through the server.

The mode is part of the definition's content hash, so an in-flight instance keeps the mode it
started on. `LOCAL_ASYNC` only pays off when there's a run of consecutive same-queue steps to
batch; the win shows against a real database (fewer WAL fsyncs). Compare the modes with
`./gradlew :example:bench` (see `docs/local-execution.md`).

---

## Running workers

A worker registers one or more blueprints and pulls work. Run as many as you like, in as
many processes as you like — they share the load automatically.

```java
try (WiggleClient client = new WiggleClient("localhost:8080")) {
    Worker worker = new Worker(client, "worker-1",
                    WorkerOptions.defaults()
                        .withConcurrency(16)                  // steps in flight at once
                        .withLease(Duration.ofSeconds(30)))   // how long a step may run before recovery
            .register(orders)
            .start();

    // ... worker runs in the background until closed ...
    Runtime.getRuntime().addShutdownHook(new Thread(worker::close));
}
```

`WIGGLE_URL` (default `localhost:8080`), `WIGGLE_WORKER_ID`, and
`WIGGLE_WORKER_CONCURRENCY` are read from the environment by the example worker; anything
else is a `WorkerOptions` setting.

**Worker specialization**: by default a worker serves every queue of the blueprints it
registered. Pair `onQueue("gpu")` on a step with `WorkerOptions.defaults().withQueues("gpu")`
on a dedicated worker pool, and only those workers execute it — a local-execution chain hands
the step over automatically at the queue boundary.

---

## Starting and tracking instances

```java
// Start an instance with an initial context.
String id = client.start(orders, Order.of("A-1001", "ada", 3, new BigDecimal("249.90")));

// Poll, or block until it reaches a terminal state.
InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));
switch (v.status()) {                       // COMPLETED | FAILED | CANCELLED
    case "COMPLETED" -> System.out.println(v.context());
    case "FAILED"    -> System.out.println(v.error());
    default          -> { }
}

// Cancel a running instance.
client.cancel(id, "customer changed their mind");
```

---

## Retries and failures

- A step that throws is **retried** per its `RetryPolicy` (default: exponential backoff
  with jitter). When the policy is exhausted, the instance fails.
- Throw `PermanentActivityException` to **skip retries** for an error you know won't recover.
- If a worker dies mid-step, its lease expires and the step is automatically **redelivered**
  to another worker.

Need the current attempt inside a step (e.g. to behave differently after earlier
failures)? Read it from `Step` — no change to your step's signature:

```java
import dev.wiggle.client.worker.Step;

.step("authorise", order -> {
    if (Step.attempt() <= 2) {                 // 1 on the first try, +1 each retry
        throw new IllegalStateException("payment gateway timeout");
    }
    return order.withPaymentRef("auth-" + order.orderId());
}, RetryPolicy.exponential(5, Duration.ofMillis(100)))
```

`Step` also exposes `Step.name()` and `Step.instanceId()`. It's valid only inside a
running step.

---

## Signals (human / external input)

`awaitSignal` pauses the instance until a **named signal** arrives from the outside — a human
approving, another system reporting back. No worker is held while it waits, so it can sit for
days. Signals are addressed by *(instance, name)*, not an opaque task id.

```java
Workflow.defineJson("expense")
        .step("submit", Expenses::record)

        // Waits for the "manager-approval" signal. Optional deadline (here 48h)
        // runs the escalation branch if nobody acts.
        .awaitSignal("manager-approval", Duration.ofHours(48),
                b -> b.step("auto-escalate", Escalations::toDirector))

        .step("pay-out", Expenses::disburse)
        .build();
```

- `awaitSignal(name)` — waits indefinitely.
- `awaitSignal(name, timeout)` — if it never arrives, the **instance fails** with a timeout error.
- `awaitSignal(name, timeout, escalation)` — if it never arrives, the **escalation branch runs**, then rejoins.

Deliver a signal from code (`client.signal(instanceId, "manager-approval", payload)` — a
first-class gRPC RPC), from the [dashboard](#web-dashboard) (a "Pending signals" panel), or
over HTTP:

```bash
curl -X POST http://localhost:8090/api/instances/{id}/signal/manager-approval \
     -H 'Content-Type: application/json' -d '{"decision":"approved"}'
```

The payload merges into the context like a `step`'s return value, and the flow continues.
Signals are **not buffered**: the instance must currently be waiting on that name, otherwise
the delivery is rejected with a conflict the sender can retry. Cancelling the instance clears
any pending wait.

---

## Sub-workflows

`subWorkflow(name, workflow)` runs another registered workflow as a **child instance**: the
child starts with the parent's current context, the parent waits (holding no worker), and on
completion the child's final context merges back. A failed or cancelled child **fails the
parent** with the child's error; cancelling the parent **cascades** to running children.

```java
Workflow.defineJson("onboarding")
        .step("create-account", Accounts::create)
        .subWorkflow("run-kyc", "kyc-checks")     // reuse the whole kyc-checks workflow
        .step("activate", Accounts::activate)
        .build();
```

---

## Schedules

A schedule starts a workflow on a **fixed interval** or a **cron expression** — the leader
fires it, a compare-and-set on the fire time makes each firing exactly-once even across leader
failover, and a missed window does not burst (the next fire is the next interval/cron match).

**A workflow has at most one schedule.** Creating one is an upsert keyed on the workflow name,
so calling `createSchedule`/`createCronSchedule` again (e.g. every app instance doing "ensure my
schedule exists" on startup) updates the existing schedule's cadence/context in place instead of
piling up duplicate firers — safe to call from as many client instances as you like.

From the client (first-class gRPC RPCs):

```java
client.createSchedule("nightly-report", Duration.ofHours(1), Map.of("source", "timer"));
client.createCronSchedule("nightly-report", "0 3 * * *", null);   // 03:00 UTC daily
client.schedules();          // List<ScheduleInfo>: id, workflow, cadence, nextFireAt
client.deleteSchedule(id);
```

Cron is the standard five fields (`minute hour day-of-month month day-of-week`) with `*`,
lists, ranges and steps (`*/15`, `9-17`, `1,15`); both dom and dow restricted means *either*
matches (vixie rule); evaluated in **UTC** so every node agrees. Or over HTTP:

```bash
curl -X POST http://localhost:8090/api/schedules -H 'Content-Type: application/json' \
     -d '{"workflow": "nightly-report", "cron": "0 3 * * *"}'          # or "everyMillis": 3600000

curl http://localhost:8090/api/schedules            # list
curl -X DELETE http://localhost:8090/api/schedules/{id}   # stop
```

Programmatically on the server: `engine.createSchedule(workflow, Duration, context)` /
`createCronSchedule(workflow, cron, context)` / `deleteSchedule(id)` / `schedules()`.
Scheduled instances carry `correlationId = "schedule:<id>"`.

---

## Running the server

### Single node (in-memory)

No database, no clustering — perfect for development and tests. This is the default when
no JDBC URL is set.

```bash
./gradlew :server:run           # or run WiggleServer with ServerConfig.fromEnvironment()
```

### A cluster (Postgres)

Point several server nodes at one Postgres and they form a cluster: every node serves the
API and hands out work, and exactly one is elected to run clock-driven duties (timers,
lease recovery). Kill any node — including the leader — and the rest carry on.

> **Pluggable storage.** The server core knows nothing about any database; a JDBC store is a
> separate module (`wiggle-postgres`) that plugs in through a `StorageProvider` SPI. With no
> JDBC URL it runs in-memory; with one it picks the provider that matches the URL. Supporting
> another database is a new module — no changes to the engine.

```bash
docker compose up -d postgres
scripts/cluster.sh 20           # three server nodes, two workers, one Postgres

# or on Kubernetes (kind):
scripts/kind-up.sh 3            # 3 server nodes + Postgres, reachable at localhost:30080
scripts/run-workers.sh 5 20     # 5 local workers, then submit 20 orders
scripts/kind-down.sh            # tear it down
```

Set clustering on any node just by giving it a JDBC URL:

```bash
WIGGLE_JDBC_URL=jdbc:postgresql://localhost:5432/wiggle \
WIGGLE_JDBC_USER=wiggle WIGGLE_JDBC_PASSWORD=wiggle \
  ./gradlew :server:run
```

### Configuration

Everything has a sensible default; override via environment variable or system property.

| Environment variable | Default | Meaning |
|---|---|---|
| `WIGGLE_PORT` | `8080` | gRPC port (`0` picks a free one) |
| `WIGGLE_JDBC_URL` | *(unset)* | **unset = in-memory, single node**; set it to cluster on a database |
| `WIGGLE_JDBC_USER` / `WIGGLE_JDBC_PASSWORD` | | database credentials |
| `WIGGLE_JDBC_POOL_SIZE` | `10` | connection pool size |
| `WIGGLE_LEASE_MILLIS` | `30000` | default task lease before a stalled step is reclaimed |
| `WIGGLE_LONGPOLL_MAX_MILLIS` | `20000` | how long a worker's poll may block server-side |
| `WIGGLE_RETENTION_MILLIS` | `86400000` | how long finished instances are kept |
| `WIGGLE_NODE_NAME` | hostname | name shown in cluster membership |
| `WIGGLE_DASHBOARD_PORT` | `0` (off) | set a port to enable the web dashboard |
| `WIGGLE_LOG_FILE` | *(unset)* | set a path to also log to a rotating file |
| `WIGGLE_LOG_LEVEL` | `INFO` | file log level: `INFO`, `DEBUG`, `WARNING`, `ERROR` |
| `WIGGLE_QUEUE_LAG_CHECK_INTERVAL_MILLIS` | `5000` | how often the leader checks the queue backlog |
| `WIGGLE_QUEUE_LAG_WARN_MILLIS` | `10000` | log a WARNING once the backlog isn't draining within this budget |

### Queue lag monitoring

The leader runs a background check (independent of housekeeping) that watches whether the
dispatchable backlog is being drained fast enough. It compares the current queue depth
against the actual completion rate across the whole cluster (read from the database, not an
in-process counter, so every node's throughput counts) and logs a `WARNING` once the backlog
either isn't draining or its oldest task has been waiting past the threshold:

```
WARNING: queue lag: 10 task(s) queued, consumption rate=0.00 tasks/sec, estimated drain
time=never (no throughput), oldest queued task has waited 12595ms
```

This is a symptom of too few workers, a stuck/misbehaving worker pool, or a step that's
much slower than its arrival rate -- add workers, check worker logs, or split the slow step
onto its own queue (`onQueue`) to isolate it.

### Logging

Wiggle logs through the JDK's `System.Logger`, so there's **no logging dependency** — by
default it goes to the console via `java.util.logging`. To also write to a **rotating file**
(5 × 10 MB), just set an env var:

```bash
WIGGLE_LOG_FILE=/var/log/wiggle/wiggle-%g.log WIGGLE_LOG_LEVEL=DEBUG ./gradlew :server:run
```

For full control (formatters, per-package levels, console tuning), point the JVM at a
`java.util.logging` config instead — see `deploy/logging.properties`:

```bash
WIGGLE_OPTS="-Djava.util.logging.config.file=/etc/wiggle/logging.properties" bin/wiggle
```

Note the level mapping when writing `logging.properties` by hand: `System.Logger`'s
`DEBUG → FINE`, `TRACE → FINER`, `INFO → INFO`, `WARNING → WARNING`, `ERROR → SEVERE`. (The
`WIGGLE_LOG_LEVEL` env var takes the `System.Logger` names and maps them for you.) The demo
CLIs print to stdout directly, so their output isn't captured by the logging config — only
the `dev.wiggle.*` server logs are.

### Schema migrations

The schema (`wf_definition`, `wf_graph_node`, `wf_graph_edge`, `wf_instance`, `wf_token`,
`wf_node`) is created and evolved automatically on startup by a small **versioned migration
runner** in `JdbcStorage`:

- Migrations are an ordered, **forward-only** list (`JdbcStorage.MIGRATIONS`); applied
  versions are tracked in a `wf_schema_version` table, so each runs exactly once.
- On boot, pending migrations run inside one transaction under a cross-node advisory lock —
  safe when several nodes start at once, and atomic on PostgreSQL (a failed migration rolls
  back and records nothing). Version 1 is the baseline, using `IF NOT EXISTS`, so a database
  created before versioning existed adopts it without re-creating anything.

To change the schema, **append** a new `Migration(n, "name", sql)` — never edit or reorder a
released one. Keep changes backward-compatible (add nullable columns, new tables/indexes) so
a rolling deploy, where old and new nodes briefly share the database, stays safe; do
destructive changes a release later, once every node is upgraded.

### Web dashboard

A single-page dashboard ships with the server — off by default. Give it a port to turn it on:

```bash
WIGGLE_DASHBOARD_PORT=8090 ./gradlew :server:run
# → open http://localhost:8090
```

It has four tabs:

- **Instances** — filter by workflow/status; select one to see a **live trace**: the workflow
  diagram with every node ringed by its token's status (done / running / failed / waiting),
  plus the token table, context, and a cancel button. If the instance is parked on a signal,
  an inline form delivers it.
- **Workflows** — pick a workflow to render its compiled graph as a diagram (tasks, gates,
  fork/join, signals, sub-workflows, and the back-edges that `doWhile` loops introduce).
- **Schedules** — create interval or cron schedules (with a seed context) and delete them.
- **Signals** — every instance currently waiting on a signal, with an inline deliver form.

The UI is a **ClojureScript + Reagent** app under [`dashboard-ui/`](dashboard-ui/), compiled to
a single JS bundle that ships inside the server jar and is served straight from its classpath —
no gRPC proxy, no CDN, no runtime dependencies. It talks to the same JSON API the server always
exposed (`/api/instances`, `/api/instances/{id}`, `/api/workflows`, `/api/workflows/{name}` for
the graph, `/api/signals`, `/api/schedules`, `/api/cluster`). In a cluster, every node can run
its own dashboard, and each shows the whole system (they share the database).

Working on the UI:

```bash
cd dashboard-ui
npm install
npx shadow-cljs watch app     # hot-reloading dev build on http://localhost:8280,
                              # proxying /api to a server running on :8090
```

`./gradlew :server:build` compiles the release bundle automatically (via `buildDashboard`).
It needs Node on the PATH; without it — or with `-PskipDashboard` — the build skips the SPA, and
the dashboard responds `503 dashboard UI not built` until you run the bundle build (`make cljs`).
The rest of the server (gRPC API, JSON endpoints) works regardless.

---

## Examples

The `example` module is a complete, runnable order-fulfilment app — a good template to
copy from:

| File | Shows |
|---|---|
| `OrderFulfilment.java` | the workflow definition (validate → filter → fork(payment, shipping) → notify) |
| `Demo.java` | embedded server + worker + happy / filtered / failed instances in one JVM |
| `WorkerMain.java` | a standalone worker process |
| `SubmitOrders.java` | submitting and awaiting a batch of instances |

```bash
./gradlew :example:run                       # the full demo in one JVM
./gradlew :example:runWorker                 # a standalone worker (needs a running server)
./gradlew :example:submitOrders -Pcount=20   # submit 20 orders
```

---

## Good to know

- **Execution is at-least-once.** A worker crash can cause a step to run again on recovery,
  so make steps idempotent where it matters.
- **`forkEach` branch writes share one context** — per-element results belong under
  per-element keys (use `itemKey + "Index"`); two branches writing the same key race,
  last write wins.
- **A failed instance stops; it does not roll back.** There's no built-in saga/compensation.
- **The gRPC API is plaintext** (no auth/TLS). Keep it on a trusted network or front it
  with something that terminates TLS.

---

## Building from source

```bash
./gradlew build        # full build + tests
./gradlew :tests:run   # conformance scenarios, no test framework required
```

Requires JDK 21+. The Gradle wrapper is included; if it's missing, run
`gradle wrapper --gradle-version 8.10` once.
