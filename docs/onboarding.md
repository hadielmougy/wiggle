# Wiggle — Onboarding & Configuration Reference

Everything a new contributor or operator needs: what Wiggle is, how to get it running, how to
author workflows, and **every configuration option** in one place.

- New to the code? Read **§1–§4**.
- Writing a workflow? **§5**.
- Deploying / tuning? **§6 (the full config reference)** and **§7**.

---

## 1. What Wiggle is

A lightweight, embeddable **workflow engine for Java 21**. You describe a workflow as a chain of
steps with a `java.util.stream`-style DSL; a **server** owns the durable state machine; **workers**
pull work when they have capacity and run the step logic. See `README.md` for the elevator pitch
and `docs/local-execution.md` for the execution-mode deep dive.

Core properties: durable (survives restarts), at-least-once execution, pull-based workers (no
inbound connectivity), content-addressed immutable definitions, and multi-node clustering over a
shared database.

---

## 2. Getting the code

```bash
git clone https://github.com/hadielmougy/wiggle.git
cd wiggle
./gradlew build          # compiles everything and runs the test suite
```

Prerequisites:

- **JDK 21+** (the Gradle toolchain pins language level 21).
- The **Gradle wrapper** is committed (`./gradlew`); no local Gradle needed. If it's ever missing,
  regenerate once with `gradle wrapper --gradle-version 8.10`.
- **Docker** only for the Postgres cluster demos (§4.3); nothing else needs it.

Dev loop:

```bash
./gradlew build                        # full build + tests
./gradlew check                        # tests only (JUnit)
./gradlew :tests:run                   # the conformance scenarios, framework-free, no network
./gradlew :module:test --tests "Foo"   # a single test class
```

Branch off `main`, keep changes focused, and run `./gradlew check` before pushing. Schema changes
go through the migration runner (§7.4), never by editing an already-released migration.

---

## 3. Modules

| Module | What's in it | Published artifact |
|---|---|---|
| `core` | JSON, the compiled graph model, retry policy, execution mode, wire records | `wiggle-core` |
| `proto` | the `WiggleControlPlane` gRPC service + generated stubs | `wiggle-proto` |
| `client` | the workflow DSL, `WiggleClient`, the pulling `Worker` | `wiggle-client` |
| `server` | engine, cluster manager, housekeeper, queue-lag monitor, gRPC API, dashboard, in-memory store, `StorageProvider` SPI | `wiggle-server` |
| `postgres` | JDBC-backed store + `StorageProvider` (Postgres / H2) | `wiggle-postgres` |
| `example` | order-fulfilment demo, standalone worker/submitter, benchmark | *(not published)* |
| `tests` | conformance scenarios + JUnit wrapper | *(not published)* |

Published under group `io.github.hadielmougy`, version **2.0.0**. The server core is
database-agnostic; a DB is a separate module contributed via the SPI (§7.3).

---

## 4. Running it

### 4.1 Single node (in-memory) — dev default

```bash
./gradlew :server:run                          # gRPC on :8080, in-memory store
./gradlew :example:run                          # embedded server + worker + a few orders, one JVM
```

### 4.2 Server + workers as separate processes

```bash
./gradlew :server:run                                  # terminal 1
./gradlew :example:runWorker                            # terminal 2 (WIGGLE_URL=localhost:8080)
./gradlew :example:submitOrders -Pcount=20             # terminal 3
```

### 4.3 A cluster on Postgres

```bash
docker compose up -d postgres        # Postgres on :5433 (see docker-compose.yml)
scripts/cluster.sh 20                 # three server nodes, two workers, one Postgres

# or on Kubernetes (kind):
scripts/kind-up.sh 3                   # 3 server pods + Postgres at localhost:30080
scripts/run-workers.sh 5 20            # 5 local workers, then submit 20 orders
scripts/kind-down.sh                   # tear down
```

### 4.4 Handy scripts

| Script | Purpose |
|---|---|
| `scripts/build.sh` | `javac` over the whole monorepo, no Gradle |
| `scripts/verify.sh` | run the conformance suite |
| `scripts/demo.sh` | run the single-JVM demo |
| `scripts/cluster.sh [count]` | 3 nodes + 2 workers on local Postgres |
| `scripts/kind-up.sh [nodes]` / `kind-down.sh` | Postgres cluster on kind |
| `scripts/run-workers.sh <workers> [submit]` | run N worker JVMs against `WIGGLE_URL` |

### 4.5 Gradle tasks

| Task | Runs |
|---|---|
| `:server:run` | `WiggleServer` (standalone server) |
| `:server:installDist` | server distribution (bundles `wiggle-postgres`) — used by the Docker image |
| `:example:run` | `Demo` (embedded end-to-end) |
| `:example:runWorker` | `WorkerMain` |
| `:example:submitOrders -Pcount=N` | `SubmitOrders` |
| `:example:bench` | `Benchmark` (throughput micro-benchmark, §6.6) |
| `:tests:run` | `Scenarios` (framework-free conformance) |

---

## 5. Authoring workflows

```java
Blueprint<Order> orders = Workflow.define("order-fulfilment", ContextCodec.records(Order.class))
        .step("validate", Orders::validate)
        .gate("in-stock", o -> o.quantity() > 0)
        .fork(
                Branch.of("payment",  s -> s.step("authorise", Pay::authorise, RetryPolicy.exponential(5, ofMillis(100)))
                                            .step("capture",   Pay::capture)),
                Branch.of("shipping", s -> s.step("reserve", Stock::reserve)
                                            .sleep("await", ofMillis(300))
                                            .step("label", Labels::print)))
        .step("notify", Notifier::send)
        .build();
```

### 5.1 Operations

| Operation | Meaning |
|---|---|
| `step(name, fn)` / `step(name, fn, retry)` / `then(...)` | run `fn` on a worker; its result becomes the new context |
| `effect(name, fn)` | run for a side effect; context unchanged |
| `gate(name, pred)` | continue only while true; false ends the instance as `gated:<name>` |
| `choose(when(...), …, otherwise(...))` | switch/case: first matching guard's branch runs |
| `fork(branches…)` | run branches in parallel, then join |
| `forkEach(name, itemsKey, itemKey, body)` | runtime fan-out: one branch per element of the list at `itemsKey` |
| `doWhile(name, cond, body)` | run `body`, then repeat while `cond` holds (at least once) |
| `sleep(name, duration)` | server-side timer; holds no worker |
| `userTask(name[, timeout[, escalation]])` | wait for a human/external completion; optional deadline escalates or fails |
| `onQueue(q)` / `defaultQueue(q)` | route steps to a dedicated worker pool |
| `execution(mode)` | set the execution mode (§6.4) |
| `checkpoint()` | (LOCAL_ASYNC) flush this step to the server before the next runs |
| `build()` | produce the `Blueprint` |

`step`/`effect`/`gate` take an optional trailing `RetryPolicy`. The context type is fixed for the
whole pipeline (a `map`-like `UnaryOperator<T>`), stored as either typed records
(`ContextCodec.records(X.class)`) or JSON maps (`Workflow.defineJson(name)`).

### 5.2 Running instances

```java
try (WiggleClient client = new WiggleClient("localhost:8080")) {
    String id = client.start(orders, Order.of(...));
    InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));   // COMPLETED | FAILED | CANCELLED
    client.cancel(id, "reason");
}
```

---

## 6. Configuration reference

### 6.1 How settings are supplied

The **server** reads each setting from a **system property first, then an environment variable**,
falling back to a default (`ServerConfig.fromEnvironment()`). So `-Dwiggle.port=9090` and
`WIGGLE_PORT=9090` are equivalent. For the application distribution, pass JVM flags via the
`WIGGLE_OPTS` (or `JAVA_OPTS`) environment variable that `bin/wiggle` honours.

The **worker** is configured programmatically via `WorkerOptions` (§6.5); the `WIGGLE_*` worker
variables in §6.7 are conventions of the *example* `WorkerMain`, not the client library.

### 6.2 Server — core & storage

| Env var | System property | Default | Meaning |
|---|---|---|---|
| `WIGGLE_PORT` | `wiggle.port` | `8080` | gRPC port (`0` = pick a free one) |
| `WIGGLE_NODE_NAME` | `wiggle.node.name` | hostname | name shown in cluster membership |
| `WIGGLE_JDBC_URL` | `wiggle.jdbc.url` | *(unset)* | **unset = in-memory, single node**; set to cluster on a database |
| `WIGGLE_JDBC_USER` | `wiggle.jdbc.user` | *(unset)* | database user |
| `WIGGLE_JDBC_PASSWORD` | `wiggle.jdbc.password` | *(unset)* | database password |
| `WIGGLE_JDBC_POOL_SIZE` | `wiggle.jdbc.poolSize` | `10` | JDBC connection pool size |

### 6.3 Server — engine, cluster & housekeeping

| Env var | System property | Default | Meaning |
|---|---|---|---|
| `WIGGLE_LEASE_MILLIS` | `wiggle.lease.millis` | `30000` | default task lease before a stalled step is reclaimed |
| `WIGGLE_LONGPOLL_MAX_MILLIS` | `wiggle.longpoll.maxMillis` | `20000` | server-side cap on how long a `PollTasks` may block |
| `WIGGLE_POLL_INTERVAL_MILLIS` | `wiggle.poll.intervalMillis` | `1000` | housekeeping tick cadence (timers, lease reclaim, deadlines) |
| `WIGGLE_HEARTBEAT_INTERVAL_MILLIS` | `wiggle.heartbeat.intervalMillis` | `5000` | cluster heartbeat/election interval |
| `WIGGLE_MISSED_HEARTBEATS` | `wiggle.heartbeat.missedBeforeDead` | `3` | missed beats before a node is considered dead |
| `WIGGLE_RETENTION_MILLIS` | `wiggle.retention.millis` | `86400000` | how long finished instances are kept before purge |
| `WIGGLE_HOUSEKEEPING_BATCH` | `wiggle.housekeeping.batch` | `100` | max items a housekeeping sweep processes per tick |
| `WIGGLE_QUEUE_LAG_CHECK_INTERVAL_MILLIS` | `wiggle.queueLag.checkIntervalMillis` | `5000` | how often the leader checks the backlog (§7.5) |
| `WIGGLE_QUEUE_LAG_WARN_MILLIS` | `wiggle.queueLag.warnThresholdMillis` | `10000` | WARN once the backlog isn't draining within this budget |

### 6.4 Execution modes

Set per workflow in the DSL: `Workflow.define(...).execution(ExecutionMode.LOCAL_SYNC)`. The mode
is part of the definition's **content hash**, so an in-flight instance keeps the mode it started on.

| Mode | Behaviour | Crash blast radius | Use for |
|---|---|---|---|
| `SERVER` (default) | server advances one node per claim | one step | anything non-idempotent |
| `LOCAL_SYNC` | worker chains steps, commits each before the next | one step (same as SERVER) | most workflows — safe speedup |
| `LOCAL_ASYNC` | worker buffers up to `localBatchSize` steps, flushes in one call at handback | whole batch re-runs | idempotent, throughput-critical |
| `DEFAULT` | defer to the server default (currently resolves to `SERVER`) | — | leave the choice to deployment |

`.checkpoint()` after a step forces LOCAL_ASYNC to commit it before continuing. Details and
benchmark numbers: `docs/local-execution.md`.

### 6.5 Worker — `WorkerOptions` (programmatic)

```java
new Worker(client, "worker-1", WorkerOptions.defaults()
        .withConcurrency(16)
        .withLease(Duration.ofSeconds(30))
        .withLongPollWait(Duration.ofSeconds(10))
        .withLocalBatchSize(64));
```

| Field | Default | Meaning |
|---|---|---|
| `concurrency` | CPU count | max steps in flight at once |
| `lease` | 30s | lease requested per task (renewed by heartbeats) |
| `longPollWait` | 10s | how long the worker lets a poll block server-side |
| `idleBackoff` | 200ms | pause when a poll returns nothing |
| `errorBackoff` | 2s | pause after a poll error |
| `registerOnStart` | true | (re)register blueprints when the worker starts |
| `localBatchSize` | 64 | LOCAL_ASYNC steps buffered before a flush (ignored by SERVER/LOCAL_SYNC) |

### 6.6 Logging

Wiggle logs through the JDK's `System.Logger` (routes to `java.util.logging`), so there's no
logging dependency. See `docs/` / `deploy/logging.properties` and:

| Env var | Default | Meaning |
|---|---|---|
| `WIGGLE_LOG_FILE` | *(unset)* | set a path to also log to a rotating file (5 × 10 MB) |
| `WIGGLE_LOG_LEVEL` | `INFO` | file level in `System.Logger` names: `INFO`, `DEBUG`, `WARNING`, `ERROR` |
| *(JVM flag)* `-Djava.util.logging.config.file=…` | — | full control via a `logging.properties`; overrides the env shortcut |

Level mapping to java.util.logging: `DEBUG→FINE`, `TRACE→FINER`, `INFO→INFO`, `WARNING→WARNING`,
`ERROR→SEVERE`. INFO is a clean lifecycle narrative (start/stop, membership, leader, registrations,
failures, purges, lag warnings); DEBUG adds per-token/step/RPC detail.

### 6.7 Example worker & benchmark variables

Conventions of the `example` module's `WorkerMain` / `Benchmark` (not the library):

| Env var | Default | Read by | Meaning |
|---|---|---|---|
| `WIGGLE_URL` | `localhost:8080` | WorkerMain, SubmitOrders | server gRPC target (a leading `http(s)://` is stripped) |
| `WIGGLE_WORKER_ID` | `worker-<pid>` | WorkerMain | worker identity |
| `WIGGLE_WORKER_CONCURRENCY` | `8` | WorkerMain | worker concurrency |
| `WIGGLE_LOCAL_BATCH_SIZE` | `64` | WorkerMain, Benchmark | LOCAL_ASYNC batch size |
| `WIGGLE_EXECUTION_MODE` | `SERVER` | Benchmark | execution mode for the benchmark workflow |
| `WIGGLE_BENCH_STEPS` | `20` | Benchmark | steps in the linear pipeline |
| `WIGGLE_BENCH_COUNT` | `2000` | Benchmark | instances to run |
| `WIGGLE_BENCH_WORKERS` | `4` | Benchmark | worker JVM-internal instances |
| `WIGGLE_JDBC_URL` / `_USER` / `_PASSWORD` | *(unset)* | Benchmark | run the benchmark against a real DB |

> The example workflows set their mode in code via `.execution(...)`. To sweep modes without
> editing, change `OrderFulfilment.blueprint()` to call the provided `mode()` helper (reads
> `WIGGLE_EXECUTION_MODE`); the benchmark already reads it.

---

## 7. Operations

### 7.1 Web dashboard

Off by default. Set `WIGGLE_DASHBOARD_PORT` to a port and open `http://localhost:<port>` — a
read-only view of instances (filter by workflow/status), instance detail + token history, pending
user tasks (with a complete action), and cluster membership. Any node can run its own; each shows
the whole system.

| Env var | System property | Default | Meaning |
|---|---|---|---|
| `WIGGLE_DASHBOARD_PORT` | `wiggle.dashboard.port` | `0` (off) | HTTP port for the dashboard |

### 7.2 Storage backends (SPI)

No JDBC URL → in-memory (single node, dev/test). With a URL, the server resolves a
`StorageProvider` via `ServiceLoader`; `wiggle-postgres` (bundled in the server distribution)
supports `jdbc:postgresql:` and `jdbc:h2:`. Another database is a new module implementing the SPI —
no engine change.

### 7.3 User tasks

`userTask(name)` parks an instance until completed out of band; no worker is held. Complete via the
dashboard's Tasks panel or `POST /api/tasks/{taskId}/complete` with a JSON body (merged into the
context). Optional deadline: `userTask(name, timeout)` fails the instance on timeout;
`userTask(name, timeout, escalation)` runs the escalation branch instead.

### 7.4 Schema migrations

`JdbcStorage` runs a **versioned, forward-only** migration list on startup, tracked in
`wf_schema_version`, under a cross-node advisory lock, atomic on Postgres. To evolve the schema,
append a `Migration(n, "name", sql)` — never edit a released one; keep changes backward-compatible
for rolling deploys. Tables: `wf_definition`, `wf_graph_node`, `wf_graph_edge`, `wf_instance`,
`wf_token`, `wf_node`, `wf_schema_version`.

### 7.5 Queue-lag monitoring

The leader watches whether the dispatchable backlog is draining fast enough (backlog vs
cluster-wide completion rate) and logs a `WARNING` when it isn't — a sign of too few workers, a
stuck worker pool, or a slow step. Tune with the two `WIGGLE_QUEUE_LAG_*` knobs (§6.3).

### 7.6 Benchmarking

```bash
# in-memory (async ≈ sync, commits are free):
WIGGLE_EXECUTION_MODE=LOCAL_ASYNC ./gradlew :example:bench

# against Postgres (async wins — far fewer WAL fsyncs):
docker compose up -d postgres
WIGGLE_EXECUTION_MODE=LOCAL_ASYNC WIGGLE_JDBC_URL=jdbc:postgresql://localhost:5433/wiggle \
  WIGGLE_JDBC_USER=wiggle WIGGLE_JDBC_PASSWORD=wiggle ./gradlew :example:bench
```

---

## 8. Where to go deeper

- `README.md` — overview, quick start, DSL walkthrough.
- `docs/local-execution.md` — execution modes, the wire protocol, the shared traversal seam, and
  the crash-replay contract per mode.
- `RELEASING.md` — publishing to Maven Central.
- `proto/src/main/proto/wiggle.proto` — the control-plane wire contract.
</content>
