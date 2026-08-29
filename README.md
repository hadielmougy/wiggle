# Wiggle

A **durable, cellular state-machine platform** — and the control plane to shard it. You describe a
process as a graph with a small `java.util.stream`-style DSL; Wiggle runs it as a durable state
machine (tokens over the graph) that survives crashes, resumes exactly where it left off, and drives
its steps with pull-based *workers*. Its distinctive move is **cellular**: a namespace becomes a
*cell* — its own database and its own cluster — and a coordinator shards work across cells with
directory-free routing and zero-migration rebalancing. Blast-radius isolation and scale-out, built in.

- **Cellular by design** — a namespace is a cell with its **own database and cluster**. A coordinator places instances across cells by consistent hashing over epochs; an instance id **carries its own routing**, so there's no lookup directory and an instance never moves. Grow by adding cells; **drain and retire** old ones with zero data migration. Physical per-tenant isolation, not just logical.
- **Durable** — every instance is DB-backed: it survives restarts, retries, and worker death (lease-based recovery). Exactly-once dispatch, at-least-once execution.
- **State machine, not glue code** — `step`, `gate`, `choose`, `fork`, `sleep`, signals, timers, sub-workflows — a compiled graph, versioned by content hash. No workflow-code determinism to get wrong.
- **Pull-based & polyglot** — workers ask for work over gRPC (no inbound connectivity, backpressure built in); idiomatic **Java, Go, and Python** workers interoperate on one server, dispatched by activity name. A coordinator-aware worker fans polling out across a namespace's active cells and shifts as they rebalance.
- **Optional & lightweight** — the cell coordinator is opt-in: a single cluster runs unchanged without one, and the whole thing is a JAR plus a database (Postgres/MySQL/Oracle/SQL Server/Cassandra) — embeddable in your process, no Elasticsearch, no server mesh.

**Current version: `2.1.5`** · Apache-2.0

```bash
# Run the server (dashboard + every storage backend bundled) as a container:
docker run --rm -p 8080:8080 -p 8090:8090 -e WIGGLE_DASHBOARD_PASSWORD=change-me hadielmougy/wiggle:2.1.5
```

> New here, or looking for every configuration knob in one place? See
> **[docs/onboarding.md](docs/onboarding.md)** — onboarding + the full configuration reference.
>
> Want the 5-minute tour? See the **[slide deck](https://hadielmougy.github.io/wiggle/presentation.html)**
> ([source](docs/presentation.html)).
>
> Want to see every operator combined in runnable code? See the
> **[DSL cookbook](docs/dsl-cookbook.md)** — eight workflows, run them all with
> `./gradlew :example:runCookbook`.
>
> Prefer not to write Java to define a workflow? Author the topology as a YAML file and register it
> with the **`wiggle` CLI** — see **[docs/workflow-yaml.md](docs/workflow-yaml.md)**.
>
> Not on the JVM? There are idiomatic **[Python](https://github.com/hadielmougy/wiggle-python)** and
> **[Go](https://github.com/hadielmougy/wiggle-go)** clients that speak the same gRPC control plane, so
> their workers interoperate with Java (and each other) on one server — dispatch is by activity name,
> not by language.

---

## Install

Artifacts are published to Maven Central under `io.github.hadielmougy`.

**Gradle**

```kotlin
dependencies {
    // The DSL + worker + client — this is what your application needs.
    implementation("io.github.hadielmougy:wiggle-client:2.1.5")

    // Only if you embed the server in your own JVM (otherwise run it standalone).
    // The server core is database-agnostic; with no JDBC URL it uses the in-memory store.
    implementation("io.github.hadielmougy:wiggle-server:2.1.5")

    // For a database in an embedded server, add the storage module you want (each pools with
    // HikariCP) and build the store explicitly with a StorageFactory -- no ServiceLoader:
    //   new WiggleServer(config, cfg -> new JdbcStorage(
    //       cfg.jdbcUrl(), cfg.jdbcUser(), cfg.jdbcPassword(), cfg.jdbcPoolSize(), new PostgresDialect()));
    // (The standalone server image bundles EVERY backend and picks one from the URL scheme, so as a
    // container you never choose at build time -- see "Clustering" below.)
    implementation("io.github.hadielmougy:wiggle-postgres:2.1.5")   // PostgreSQL + H2 dialects
    runtimeOnly("org.postgresql:postgresql:42.7.4")

    // Other backends are drop-in modules, each contributing a dialect (or, for Cassandra, its own
    // partition-aware store):
    //   io.github.hadielmougy:wiggle-mysql      + com.mysql:mysql-connector-j
    //   io.github.hadielmougy:wiggle-oracle     + com.oracle.database.jdbc:ojdbc11
    //   io.github.hadielmougy:wiggle-sqlserver  + com.microsoft.sqlserver:mssql-jdbc
    //   io.github.hadielmougy:wiggle-cassandra  (cassandra:// URLs)
}
```

**Maven**

```xml
<dependency>
  <groupId>io.github.hadielmougy</groupId>
  <artifactId>wiggle-client</artifactId>
  <version>2.1.5</version>
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
import java.util.HashMap;
import java.util.Map;

// 1. Define a workflow. Here the context is a plain Map; see below for typed records.
// A step must return the whole context, not just the fields it changed -- the engine
// diffs the return value against what it was given, so a bare Map.of("greeting", ...)
// would tell it "name" was deliberately cleared.
Blueprint<Map<String, Object>> greet = Workflow.defineJson("greet")
        .step("say-hello", ctx -> {
            Map<String, Object> next = new HashMap<>(ctx);
            next.put("greeting", "hello, " + ctx.get("name"));
            return next;
        })
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
| `step(name, fn, retry)` / `step(name, fn, queue)` / `step(name, fn, retry, queue)` | same, with an explicit `RetryPolicy` and/or a dedicated `queue` for that step |
| `effect(name, fn)` | run `fn` for a side effect; context unchanged (also takes optional `retry`/`queue`) |
| `gate(name, pred)` | continue only while `pred` is true; a false result ends the instance as `gated:<name>` (also takes optional `retry`/`queue`) |
| `choose(cases…)` | switch/case: run the branch of the **first** matching guard, then continue |
| `sleep(name, duration)` | wait on a server-side timer — **no worker is held** while waiting |
| `awaitSignal(name[, timeout[, escalation]])` | wait for a named external signal; optional deadline escalates or fails |
| `subWorkflow(name, workflow)` | run another workflow as a child; its result merges back, its failure fails the parent |
| `fork(branches…)` | run branches in parallel, then wait for all of them to finish (join) |
| `forkEach(name, itemsKey, itemKey, body)` | **runtime** fan-out: one parallel branch per element of the list at `itemsKey`, each seeing its element as `itemKey` (and `itemKey + "Index"`); empty list skips through |
| `doWhile(name, cond, body)` | run `body`, then re-run while `cond` holds (body runs at least once) |
| `defaultQueue(q)` | set the queue for every following step (a per-step `queue` argument overrides it) |
| `build()` | finish; produces the `Blueprint` |

`step`, `effect`, and `gate` all accept an optional trailing `RetryPolicy` argument; omit it
to use the workflow's default policy.

### The context

The context is your workflow's data. It's the same type from the first step to the last:
`step` returns a new context of the same type (think `UnaryOperator<T>`, not
`Function<T,R>`). Two flavors:

- **Typed records** — `Workflow.define("name", ContextCodec.records(Order.class))`. Your
  steps take and return an `Order`. Records are immutable and serialize cleanly.
- **Versioned records** — `VersionedContextCodec.builder(Order.class, 3)…`. Same as above,
  but the context is wrapped in a `{_schema, _v, data}` envelope so the record's shape can
  evolve safely. See [Evolving the context schema](#evolving-the-context-schema).
- **JSON maps** — `Workflow.defineJson("name")`. Steps take and return a
  `Map<String, Object>`. Handy when you don't want a dedicated type.

> **Parallel branches merge automatically.** Each step writes back only the fields it
> *changed*, so branches that touch different fields merge cleanly. If two branches write
> the same field, the later write wins.

### Evolving the context schema

A plain `ContextCodec.records(Order.class)` stores the record's fields directly. If you later
add, remove, rename, or retype a field, **already-running instances** were written under the old
shape — they'll be silently defaulted, lose data, or fail to decode, because nothing tracks which
version their context was written at.

`VersionedContextCodec` fixes this. It wraps the context in a small envelope
(`{"_schema":"order","_v":3,"data":{…}}`) and migrates older data forward on read, so your steps
only ever see the current shape:

```java
var codec = VersionedContextCodec.builder(Order.class, /* current version */ 3)
    .schema("order")
    .upcast(1, m -> { m.put("currency", "USD"); return m; })   // v1 → v2: add a field
    .upcast(2, m -> { m.put("total", m.remove("amount")); return m; })  // v2 → v3: rename
    .build();

Blueprint<Order> orders = Workflow.define("order-fulfilment", codec) …;
```

- **Upcast to current.** On decode, data written at an older `_v` runs through the `upcast` chain
  until it matches the current record; on the next write the instance is re-stored at the current
  version. Bare, pre-envelope contexts are read as version 1, so existing instances upgrade
  transparently — no flag day.
- **Condition on the version.** Inside a step, `ContextVersion.current()` returns the version the
  context was persisted at, so a handler can treat older instances specially. For an origin marker
  that survives every step, stamp it into the data from an upcast (it then persists as a normal field).

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
registered. Pair a step's queue argument — `step("render", fn, "gpu")` — with
`WorkerOptions.defaults().withQueues("gpu")` on a dedicated worker pool, and only those workers
execute it — a local-execution chain hands the step over automatically at the queue boundary.

---

## Name-only binding: define the flow once, implement steps by name

The server drives the graph, and workers dispatch by activity name (`"<workflow>#<step>"`) — so
the topology only needs to live in **one** place. Register the workflow once, then attach step
implementations **by name** from wherever each step belongs. Independent workers (different teams,
different deploys) can own different steps of the same flow without any of them re-declaring it.

```java
// The order team authors and registers the graph — topology only.
client.register(OrderFulfilment.blueprint());

// A worker that never saw that blueprint implements some steps, by (workflow, step) name:
new Worker(client, "fulfilment-worker")
        .handle("order-fulfilment", "validate", ctx -> put(ctx, "status", "VALIDATED"))
        .handleGate("order-fulfilment", "in-stock", ctx -> qty(ctx) > 0)
        .handleEffect("order-fulfilment", "notify", ctx -> email(ctx))
        .start();   // reconciles against the registered graph before polling

// A separate worker owns just `charge` — on its own queue:
new Worker(client, "payments-worker")
        .handle("order-fulfilment", "charge", ctx -> put(ctx, "paymentRef", auth(ctx)))
        .start();
```

`handle` (task), `handleGate` (predicate), and `handleEffect` (side effect) bind by name. On
`start()` the worker **reconciles** its bindings against the graph the server holds: it verifies each
step exists and is the right kind (a mistyped name or a task-bound-as-gate fails fast with the
available step names), and it **discovers which queue each step polls** — so a name-only worker needs
no queue configuration. Steps a worker doesn't implement are simply served by whoever does.

One constraint: the graph must be **registered before** a name-only worker starts, or it fails fast.
Register it as a deploy step; for local/dev, `WorkerOptions.withAwaitRegistration(Duration)` rides out
the startup race.

See it run: [`example:runBinding`](example/src/main/java/dev/wiggle/binding/BindingDemo.java)
authors the flow and serves it from two independent workers bound purely by name — a fulfilment
worker and a payments-queue worker — then submits an order and prints the result.

**A whole object of handlers.** Instead of one `handle(...)` per step, hand the worker an object whose
methods *are* the steps — matched by name, with the **method signature picking the kind**:

```java
class OrderHandlers {
    Map<String,Object> validate(Map<String,Object> c) { return put(c, "status", "VALIDATED"); } // task
    boolean inStock(Map<String,Object> c)             { return qty(c) > 0; }                     // gate; matches "in-stock"
    void notify(Map<String,Object> c)                 { email(c); }                              // side effect
}

new Worker(client, "fulfilment-worker")
        .registerHandlers("order-fulfilment", new OrderHandlers())
        .start();
```

Each method that takes the context and returns a `Map` (task), `boolean` (gate), or `void` (effect) is
matched to a step **by case-insensitive name** (`inStock` ↔ `in-stock`) on `start()`, the graph
confirming the exact name and gate-vs-task. Methods of any other shape are ignored, so helpers can live
on the object. Two method names that collide under case-folding are rejected at `registerHandlers`; a
method matching no step, or a signature that clashes with the graph's kind, fails fast on `start()`.

**Typed contexts too.** A handler can work on a **record** rather than a JSON map — pass a
`ContextCodec` to `handle`:

```java
ContextCodec<Purchase> codec = ContextCodec.records(Purchase.class);
new Worker(client, "fulfilment")
        .handle("typed-order", "validate", codec, p -> p.withStatus("VALIDATED"))   // Purchase -> Purchase
        .handleGate("typed-order", "in-stock", codec, p -> p.quantity() > 0)
        .start();
```

A record and a map are the **same JSON on the wire**, so a typed handler and an untyped one can serve
different steps of the same instance interchangeably — binding is by step name, not by type.
[`example:runTypedBinding`](example/src/main/java/dev/wiggle/binding/typed/TypedBindingDemo.java)
is the same demo with a typed `Purchase` context.

Since the topology is authored once and implemented by name, it doesn't have to be authored in Java
at all — describe it as a **[YAML file](docs/workflow-yaml.md)** and register it with the `wiggle`
CLI, then bind handlers by name exactly as above.

### Other language clients

Steps can be implemented in any language too. Two idiomatic clients speak the same gRPC control plane,
so their workers interoperate with Java workers (and each other) on one server — a single instance can
have its steps served by a mix of languages, dispatched by activity name:

- **[wiggle-python](https://github.com/hadielmougy/wiggle-python)** (`pip install wiggle-client`) — a
  declarative `Graph` topology plus a worker that binds handlers by name (`handle` / `register_handlers`).
- **[wiggle-go](https://github.com/hadielmougy/wiggle-go)** (`go get github.com/hadielmougy/wiggle-go`) —
  declarative `Graph` structs plus a worker that binds handlers by name (`Handle` / `RegisterHandlers`).

Both register topology and bind handlers the same way this section describes; the content-hash version
is per-language and need not match, because interop is by activity name.

---

## Command-line tool (`wiggle`)

`wiggle` authors and registers workflows from a declarative **[YAML file](docs/workflow-yaml.md)** —
no Java required. It's the deploy-time companion to name-only binding: register the topology once,
then let workers implement steps by name.

```bash
wiggle validate order.yaml                          # compile + validate offline (no server) — great in CI
wiggle register order.yaml --server prod:8080       # register with a running server
```

- **Which server:** `--server`/`-s`, else `$WIGGLE_URL`, else `localhost:8080`. `validate` needs no server.
- **TLS:** reads the same `WIGGLE_TLS_*` env as workers; `--tls-truststore`/`--tls-keystore` (mTLS) or
  `--tls` (JVM default trust store) override per invocation.
- **Install:** `brew tap hadielmougy/wiggle https://github.com/hadielmougy/wiggle && brew install hadielmougy/wiggle/wiggle`,
  or download the archive from the release — see [docs/workflow-yaml.md](docs/workflow-yaml.md#installing-the-cli).
  (It's a JVM app; needs a recent JDK.)

The full YAML schema and every subcommand detail live in **[docs/workflow-yaml.md](docs/workflow-yaml.md)**.

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
./gradlew :dist:run           # or run WiggleServer with ServerConfig.fromEnvironment()
```

### Docker

A prebuilt image runs the standalone server with the dashboard and the PostgreSQL provider
bundled in. It reads the same environment variables as the JAR (see [Configuration](#configuration)).

```bash
# in-memory, secured dashboard — gRPC on :8080, dashboard on http://localhost:8090
docker run --rm -p 8080:8080 -p 8090:8090 \
  -e WIGGLE_DASHBOARD_PASSWORD=change-me \
  hadielmougy/wiggle:2.1.5

# against PostgreSQL
docker run --rm -p 8080:8080 -p 8090:8090 \
  -e WIGGLE_JDBC_URL=jdbc:postgresql://db:5432/wiggle \
  -e WIGGLE_JDBC_USER=wiggle -e WIGGLE_JDBC_PASSWORD=wiggle \
  -e WIGGLE_DASHBOARD_PASSWORD=change-me \
  hadielmougy/wiggle:2.1.5
```

Or bring up a **complete stack** — server + Postgres, dashboard with admin login, durable
volume — with the bundled compose file:

```bash
docker compose -f docker-compose.full.yml up -d     # → http://localhost:8090 (admin / change-me)
docker compose -f docker-compose.full.yml down      # add -v to wipe the database
```

TLS works the same as the JAR: set `WIGGLE_TLS_KEYSTORE` (+ password) and mount the keystore
(e.g. `-v $PWD/certs:/certs:ro -e WIGGLE_TLS_KEYSTORE=/certs/server.p12`). Build the image
yourself with `docker build -t wiggle .`; publish a multi-arch image with `scripts/docker-release.sh`.

> The server image runs the control plane and dashboard; it does not include a **worker**. Run
> workers as your own processes against `:8080` (your app on `wiggle-client`, or
> `./gradlew :example:runWorker`) so steps actually execute.

### A cluster (Postgres)

Point several server nodes at one Postgres and they form a cluster: every node serves the
API and hands out work, and exactly one is elected to run clock-driven duties (timers,
lease recovery). Kill any node — including the leader — and the rest carry on.

> **Pluggable storage.** The server core knows nothing about any database; it builds its store from
> an injected `StorageFactory` (an explicit switch on the URL — no `ServiceLoader`). PostgreSQL and
> H2 (`wiggle-postgres`), MySQL/MariaDB (`wiggle-mysql`), Oracle (`wiggle-oracle`) and SQL Server
> (`wiggle-sqlserver`) all share one HikariCP-pooled, dialect-aware JDBC store (`wiggle-jdbc`);
> **Cassandra** (`wiggle-cassandra`) is a separate, partition-aware store on the CQL driver
> (lightweight transactions in place of row locks — see `cassandra/README.md`). The standalone
> distribution (`wiggle-dist`, what the Docker image runs) bundles **every** backend and picks one
> from the URL scheme (`jdbc:…` or `cassandra://…`); with none set it runs in-memory. **One image,
> all databases** — you never build a per-database image. Supporting another database is a new
> module — no changes to the engine core.

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
  ./gradlew :dist:run
```

### Configuration

Everything has a sensible default; override via environment variable or system property.

| Environment variable | Default | Meaning |
|---|---|---|
| `WIGGLE_PORT` | `8080` | gRPC port (`0` picks a free one) |
| `WIGGLE_JDBC_URL` | *(unset)* | **unset = in-memory, single node**; set it to cluster on a database. `jdbc:postgresql:`, `jdbc:h2:`, `jdbc:mysql:`/`jdbc:mariadb:`, `jdbc:oracle:`, `jdbc:sqlserver:` or `cassandra://` — the engine is detected from the URL |
| `WIGGLE_JDBC_USER` / `WIGGLE_JDBC_PASSWORD` | | database credentials |
| `WIGGLE_JDBC_POOL_SIZE` | `10` | HikariCP maximum pool size |
| `WIGGLE_LEASE_MILLIS` | `30000` | default task lease before a stalled step is reclaimed |
| `WIGGLE_LONGPOLL_MAX_MILLIS` | `20000` | how long a worker's poll may block server-side |
| `WIGGLE_RETENTION_MILLIS` | `86400000` | how long finished instances are kept |
| `WIGGLE_NODE_NAME` | hostname | name shown in cluster membership |
| `WIGGLE_DASHBOARD_PORT` | `0` (off) | set a port to enable the web dashboard |
| `WIGGLE_DASHBOARD_PASSWORD` | *(unset)* | admin password for the dashboard/API; **unset = unauthenticated** |
| `WIGGLE_DASHBOARD_USER` | `admin` | admin username for the dashboard/API |
| `WIGGLE_TLS_KEYSTORE` | *(unset)* | keystore path; **unset = plaintext** for gRPC + HTTP. Enables TLS for both |
| `WIGGLE_TLS_KEYSTORE_PASSWORD` | *(unset)* | password for the keystore |
| `WIGGLE_TLS_TRUSTSTORE` | *(unset)* | truststore path; on a server this **requires client certs (mTLS)** |
| `WIGGLE_TLS_TRUSTSTORE_PASSWORD` | *(unset)* | password for the truststore |
| `WIGGLE_LOG_FILE` | *(unset)* | set a path to also log to a rotating file |
| `WIGGLE_LOG_LEVEL` | `INFO` | file log level: `INFO`, `DEBUG`, `WARNING`, `ERROR` |
| `WIGGLE_QUEUE_LAG_CHECK_INTERVAL_MILLIS` | `5000` | how often the leader checks the queue backlog |
| `WIGGLE_QUEUE_LAG_WARN_MILLIS` | `10000` | log a WARNING once the backlog isn't draining within this budget |
| `WIGGLE_MEMORY_SHEDDING_ENABLED` | `false` | **memory admission control**: when GC-accurate heap utilization crosses the threshold, the server rejects a fraction of new worker polls (empty + a hold-off) instead of taking on request/response memory it can't hold; it recovers on its own once utilization falls |
| `WIGGLE_MEMORY_THRESHOLD` | `0.90` | live-heap utilization (post-GC used / max, `0`–`1`) at/above which the server is "under pressure" |
| `WIGGLE_MEMORY_REJECT_RATIO` | `0.10` | fraction of polls to reject while under pressure (`0.10` = accept 90%, reject 10%) |
| `WIGGLE_MEMORY_RETRY_MILLIS` | `2000` | retry interval a rejected worker is told to wait before polling again |
| `WIGGLE_MEMORY_RETRY_JITTER_MILLIS` | `1000` | random extra added per response to the retry interval, so workers don't retry in lockstep |

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
onto its own queue (the per-step `queue` argument) to isolate it.

### Logging

Wiggle logs through the JDK's `System.Logger`, so there's **no logging dependency** — by
default it goes to the console via `java.util.logging`. To also write to a **rotating file**
(5 × 10 MB), just set an env var:

```bash
WIGGLE_LOG_FILE=/var/log/wiggle/wiggle-%g.log WIGGLE_LOG_LEVEL=DEBUG ./gradlew :dist:run
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
WIGGLE_DASHBOARD_PORT=8090 ./gradlew :dist:run
# → open http://localhost:8090
```

**Securing it.** Set `WIGGLE_DASHBOARD_PASSWORD` and the dashboard and its JSON API require
authentication against a single admin account (`WIGGLE_DASHBOARD_USER`, default `admin`). In a
browser, an unauthenticated visit redirects to a **`/login` page**; signing in sets an HttpOnly
session cookie (12h) that carries the whole SPA, and **`/logout`** ends the session. Programmatic
clients can skip the form and use HTTP **Basic auth** instead (`curl -u admin:…`). The `/healthz`
endpoint is always exempt so load balancers and probes reach it without credentials.

```bash
WIGGLE_DASHBOARD_PORT=8090 WIGGLE_DASHBOARD_PASSWORD=$(openssl rand -hex 16) \
  ./gradlew :dist:run
curl -u admin:$PASS http://localhost:8090/api/instances
```

With no password set the dashboard is **unauthenticated** and logs a warning at startup — fine on
a trusted network, but credentials travel in cleartext over plain HTTP, so serve it over TLS
(`WIGGLE_TLS_KEYSTORE`, or a reverse proxy) for anything exposed. Auth is per-node — set the same
credentials on every node, and note sessions aren't shared across nodes (logging into one node's
dashboard doesn't log you into another's).

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
| `binding/BindingDemo.java` | name-only binding — one flow authored once, served by two independent workers by step name |
| `binding/typed/TypedBindingDemo.java` | the same, with a **typed** record context served by typed handlers |

```bash
./gradlew :example:run                       # the full demo in one JVM
./gradlew :example:runWorker                 # a standalone worker (needs a running server)
./gradlew :example:submitOrders -Pcount=20   # submit 20 orders
./gradlew :example:runBinding                # name-only binding demo (see "Name-only binding" above)
./gradlew :example:runTypedBinding           # name-only binding demo with a typed record context
```

---

## Good to know

- **Execution is at-least-once.** A worker crash can cause a step to run again on recovery,
  so make steps idempotent where it matters.
- **`forkEach` branch writes share one context** — per-element results belong under
  per-element keys (use `itemKey + "Index"`); two branches writing the same key race,
  last write wins.
- **A failed instance stops; it does not roll back.** There's no built-in saga/compensation.
- **Transport security is opt-in.** With no `WIGGLE_TLS_KEYSTORE` the gRPC API and HTTP dashboard
  are plaintext — keep them on a trusted network or enable TLS (see below).

---

## Transport security (TLS / mTLS)

TLS is off by default. Point the server at a keystore and **both** the gRPC API and the HTTP
dashboard serve over TLS; add a truststore to also **require client certificates (mTLS)**. With
nothing set, both fall back to plaintext.

```bash
# server-side TLS for gRPC + HTTPS dashboard
WIGGLE_TLS_KEYSTORE=/etc/wiggle/server.p12 WIGGLE_TLS_KEYSTORE_PASSWORD=… \
WIGGLE_DASHBOARD_PORT=8090 ./gradlew :dist:run

# mutual TLS: also verify client certs against a truststore
WIGGLE_TLS_KEYSTORE=/etc/wiggle/server.p12   WIGGLE_TLS_KEYSTORE_PASSWORD=… \
WIGGLE_TLS_TRUSTSTORE=/etc/wiggle/trust.p12  WIGGLE_TLS_TRUSTSTORE_PASSWORD=… \
  ./gradlew :dist:run
```

Workers and clients read the same variables: `WIGGLE_TLS_TRUSTSTORE` verifies the server, and
`WIGGLE_TLS_KEYSTORE` presents a client certificate when the server requires mTLS. Stores are
PKCS12 (`.p12`) by default; a `.jks` path is loaded as JKS. Set the credentials per role (a
server's keystore holds its server cert; a worker's holds its client cert).

> **TLS authenticates the connection; it is not authorization.** Any client with a trusted
> certificate can call any gRPC RPC, including privileged ones (start/cancel/signal/schedule).
> For per-role restrictions, terminate at a gateway or gate the privileged RPCs separately. The
> dashboard's HTTP API additionally supports Basic auth (`WIGGLE_DASHBOARD_PASSWORD`).

---

## Building from source

```bash
./gradlew build        # full build + tests
./gradlew :tests:run   # conformance scenarios, no test framework required
```

Requires JDK 21+. The Gradle wrapper is included; if it's missing, run
`gradle wrapper --gradle-version 8.10` once.
