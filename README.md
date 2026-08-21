# Wiggle

A lightweight, embeddable **workflow engine for Java 21**. You describe a workflow with a
small `java.util.stream`-style DSL, run it on a server (embedded in your JVM or as a
standalone cluster), and process the steps with *workers* that pull work when they have
capacity.

- **Fluent DSL** — build a workflow as a chain of steps: `step`, `gate`, `choose`, `fork`, `sleep`.
- **Durable** — instances survive restarts; run in-memory for dev, or on Postgres for real.
- **Pull-based workers** — workers ask for work; the server never pushes. Backpressure is built in, and workers need no inbound connectivity.
- **Automatic retries, timers, and parallel fork/join**, with at-least-once execution and lease-based recovery when a worker dies.

**Current version: `2.0.0`** · Java 21+ · Apache-2.0

---

## Install

Artifacts are published to Maven Central under `io.github.hadielmougy`.

**Gradle**

```kotlin
dependencies {
    // The DSL + worker + client — this is what your application needs.
    implementation("io.github.hadielmougy:wiggle-client:2.0.0")

    // Only if you embed the server in your own JVM (otherwise run it standalone).
    implementation("io.github.hadielmougy:wiggle-server:2.0.0")

    // Only for a real, multi-node deployment: a JDBC driver at runtime.
    runtimeOnly("org.postgresql:postgresql:42.7.4")
}
```

**Maven**

```xml
<dependency>
  <groupId>io.github.hadielmougy</groupId>
  <artifactId>wiggle-client</artifactId>
  <version>2.0.0</version>
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
| `fork(branches…)` | run branches in parallel, then wait for all of them to finish (join) |
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

The schema (`wf_definition`, `wf_graph_node`, `wf_graph_edge`, `wf_instance`, `wf_token`,
`wf_node`) is created automatically on startup.

### Web dashboard

A small, read-only web UI ships with the server — off by default. Give it a port to turn
it on:

```bash
WIGGLE_DASHBOARD_PORT=8090 ./gradlew :server:run
# → open http://localhost:8090
```

It lists instances (filter by workflow/status), and clicking one shows its status, error,
context, and token history; running instances can be cancelled. It reads the engine
directly over a tiny JSON API (`/api/instances`, `/api/instances/{id}`, `/api/workflows`,
`/api/cluster`) — no gRPC proxy, no build step, no extra dependencies. In a cluster, every
node can run its own dashboard, and each shows the whole system (they share the database).

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
- **`fork` branches are fixed at definition time** — there's no runtime fan-out over a
  collection of unknown size.
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
