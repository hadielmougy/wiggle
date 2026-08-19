# Wiggle

A workflow engine in two halves.

**The server** owns the state machine: it holds workflow definitions, drives instances,
schedules timers, distributes work across a cluster, and elects a leader for
clock-driven duties. Node election and work handout follow JobRunr's model.

**The client** is a DSL shaped like `java.util.stream`, plus a worker that *pulls*
work. The server never calls a worker; workers ask for as much as they have capacity
for. Backpressure is a property of the protocol rather than a thing to configure, and
workers need no inbound connectivity.

Java 21, Gradle, monorepo. Storage and engine are dependency-free — the JDK's own
JDBC and reflection are the whole toolkit there, and a JDBC driver is needed at
runtime only if you want multi-node clustering. The control plane talks gRPC, so
`server` and `client` also pull in `grpc-java` and generated protobuf stubs (the
`proto` module).

---

## Quick start

```bash
./gradlew :example:run        # embedded server + worker + three orders, one JVM
./gradlew :tests:run          # 18 conformance scenarios, no test framework needed
./gradlew build                  # full build, including the JUnit wrapper
```

No Gradle available, or no network? The build is plain enough to skip it entirely:

```bash
scripts/build.sh                 # javac over the whole monorepo
scripts/verify.sh                # run the conformance suite
scripts/demo.sh                  # run the demo
```

If the wrapper is missing, generate it once with `gradle wrapper --gradle-version 8.10`.

### Running the pieces apart

```bash
# terminal 1 -- server (in-memory store, single node)
./gradlew :server:run

# terminal 2 -- a worker, as many as you like
./gradlew :example:runWorker

# terminal 3 -- submit some orders
./gradlew :example:submitOrders -Pcount=20
```

Both worker and submitter honour `WIGGLE_URL` (default `localhost:8080`, a gRPC target
rather than a URL — a leading `http://`/`https://` is stripped if present).

### Running a cluster

```bash
docker compose up -d postgres
scripts/cluster.sh 20            # three server nodes, two workers, one Postgres
```

Every node serves the API and hands out work; exactly one holds the leader role. Kill
the leader and watch `GetCluster` re-elect within a few heartbeats (`ClusterStatus` in
the `example` module, or `scripts/cluster.sh`, print it).

---

## The DSL

```java
Blueprint<Order> blueprint = Workflow.define("order-fulfilment", ContextCodec.records(Order.class))

        .map("validate", order -> order.withStatus("VALIDATED"))

        // A false predicate ends the instance successfully -- an empty stream, not an error.
        .filter("in-stock", order -> order.quantity() > 0)

        .fork(
                Branch.of("payment", s -> s
                        .map("authorise", Payments::authorise)
                        .retry(RetryPolicy.exponential(5, Duration.ofMillis(100)))
                        .map("capture", Payments::capture)),

                Branch.of("shipping", s -> s
                        .map("reserve-stock", Stock::reserve)
                        .sleep("await-warehouse", Duration.ofMillis(300))
                        .map("print-label", Labels::print)))

        .map("notify", Notifier::send)
        .build();
```

Intermediate operations append nodes and return a stream; nothing executes until an
instance is started. The terminal `build()` produces one artifact with two audiences:
an immutable graph to register with the server, and the local handler table the worker
dispatches against. The lambdas never leave the client.

| Operation | Meaning |
|---|---|
| `map(name, fn)` / `then` | run on a worker, result becomes the context |
| `peek(name, fn)` | run for effect, context unchanged |
| `filter(name, pred)` | continue only while true; false ends the instance as `filtered:<name>` |
| `sleep(name, duration)` | server-side timer; no worker is held |
| `fork(branches...)` | fan out, then wait for every branch |
| `retry(policy)` | override the retry policy of the step just added |
| `onQueue(q)` / `defaultQueue(q)` | route steps to a dedicated worker pool |
| `build()` | terminal; produces the `Blueprint` |

### One deliberate departure from `java.util.stream`

The context type does not change from step to step: `map` is closer to
`UnaryOperator<T>` than to `Function<T, R>`. A workflow context is a durable document
that survives process restarts and is merged across parallel branches, so a single type
for the whole pipeline is what actually models the storage.

### How parallel branches share a context

Each step writes back only the top-level fields it *changed*, computed as a shallow
diff on the worker. Two branches touching different fields therefore merge cleanly; if
they write the same field, the later write wins. Without this, the slower branch would
clobber the faster one with its own stale copy of the whole object.

---

## Architecture

```
   client JVM                                server JVM(s)
  ┌────────────────────┐   register       ┌──────────────────────────────┐
  │ Workflow DSL       │ ───────────────► │ DefinitionRegistry           │
  │   → Blueprint      │                  │   content-addressed versions │
  │                    │                  ├──────────────────────────────┤
  │ Worker             │  PollTasks (gRPC)│ WorkflowEngine               │
  │   pull ≤ free slots│ ───────────────► │   tokens over a graph        │
  │   execute          │ ◄─────────────── │   leases, retries, joins     │
  │   complete / fail  │ ───────────────► │                              │
  └────────────────────┘                  ├──────────────────────────────┤
                                          │ ClusterManager (election)    │
                                          │ Housekeeper (leader only)    │
                                          ├──────────────────────────────┤
                                          │ Storage: in-memory | JDBC    │
                                          └──────────────────────────────┘
```

### The state machine

An instance is a set of **tokens** moving over the compiled graph, in the spirit of a
Petri net rather than a program counter. A fork mints one token per branch, a join
consumes them, and the instance is terminal when no token is active. Each token carries
a `joinStack`, so forks nest without any special casing.

The engine advances a token until it needs the outside world:

| Token state | Waiting on |
|---|---|
| `READY` | a worker to lease it |
| `RUNNING` | the worker holding its lease |
| `WAITING` | the clock (a sleep timer) |
| `JOINED` | its siblings to reach the barrier |
| `DONE` / `FAILED` / `CANCELLED` | nothing |

Every mutation for an instance runs inside a transaction that takes the instance's write
lock first (`SELECT ... FOR UPDATE` on JDBC, a global lock in memory). That is what lets
several server nodes drive the same instance concurrently without stepping on each other.

### Node election and work distribution

Modelled on JobRunr's background job server scheme:

- every node announces itself once, then heartbeats on a fixed interval into `wf_node`;
- a node is alive while its last heartbeat is inside the timeout window;
- the **leader is the alive node with the earliest first heartbeat** — the
  longest-running one — with ties broken by id, so every node computes the same answer
  from the same table. No consensus protocol is needed because the shared database *is*
  the source of truth;
- a node whose own heartbeat has gone stale steps down before doing leader work, so a
  partitioned node cannot keep acting as leader.

Leader-only duties are firing due timers, reclaiming expired leases, and retention
sweeps. All are idempotent and re-entrant, so a brief overlap during failover duplicates
work but never corrupts state.

Work handout is a conditional update — `UPDATE ... WHERE id = ? AND status = 'READY'`
— so a task goes to exactly one worker even with every node handing out work
simultaneously. This is portable across Postgres and H2, unlike `SKIP LOCKED`.

### Failure handling

- A step that throws is retried per its `RetryPolicy` (exponential with jitter by
  default); exhausting the policy fails the instance and cancels its siblings.
- Throwing `PermanentActivityException` skips retries entirely.
- A worker that dies mid-task simply stops renewing its lease. The leader reclaims it,
  bumps the attempt counter, and makes it dispatchable again — verified by the
  `expiredLeaseIsReclaimed` scenario.
- Completing a task without holding its lease is rejected with `409`, so a worker that
  wakes up from a long GC pause cannot corrupt an instance another worker has moved on.

### Definition versioning

A definition's version is a SHA-256 content hash of its topology. Registering the same
chain twice is idempotent; changing a step mints a new version. Instances pin the
version they started on, so a deploy never re-routes in-flight work through a graph it
did not begin with.

---

## gRPC API

Defined in `proto/src/main/proto/wiggle.proto`, service `WiggleControlPlane`. Arbitrary
JSON (workflow node graphs, instance/task context and results) travels as
`google.protobuf.Value`/`Struct`, converted at the edges by `dev.wiggle.proto.ProtoJson`
— everything else is plain proto fields.

| RPC | Purpose |
|---|---|
| `RegisterWorkflow` | register a definition |
| `ListWorkflows` · `GetWorkflow` | list · fetch latest |
| `StartInstance` | start an instance |
| `ListInstances` | list instances (workflow/status/limit filters) |
| `GetInstance` | instance with full token history |
| `CancelInstance` | cancel |
| `PollTasks` | lease work (unary, blocks up to `waitMillis` server-side) |
| `CompleteTask` · `FailTask` · `HeartbeatTask` | report outcome, extend lease |
| `GetCluster` · `HealthCheck` | membership and leader · liveness |

```bash
# grpcurl, or just use the reference clients:
java -cp out/classes dev.wiggle.order.ClusterStatus localhost:8080
```

---

## Configuration

Every setting takes a system property or an environment variable; defaults are in
`ServerConfig`.

| Property | Env | Default | Meaning |
|---|---|---|---|
| `wiggle.port` | `WIGGLE_PORT` | `8080` | gRPC port (`0` picks a free one) |
| `wiggle.node.name` | `WIGGLE_NODE_NAME` | hostname | name shown in `/v1/cluster` |
| `wiggle.jdbc.url` | `WIGGLE_JDBC_URL` | *(unset)* | **unset means in-memory, single node** |
| `wiggle.jdbc.user` / `.password` | `WIGGLE_JDBC_USER` / `_PASSWORD` | | credentials |
| `wiggle.jdbc.poolSize` | `WIGGLE_JDBC_POOL_SIZE` | `10` | connection pool size |
| `wiggle.poll.intervalMillis` | `WIGGLE_POLL_INTERVAL_MILLIS` | `1000` | housekeeping cadence |
| `wiggle.heartbeat.intervalMillis` | `WIGGLE_HEARTBEAT_INTERVAL_MILLIS` | `5000` | election heartbeat |
| `wiggle.heartbeat.missedBeforeDead` | `WIGGLE_MISSED_HEARTBEATS` | `3` | missed beats before a node is dead |
| `wiggle.lease.millis` | `WIGGLE_LEASE_MILLIS` | `30000` | default task lease |
| `wiggle.longpoll.maxMillis` | `WIGGLE_LONGPOLL_MAX_MILLIS` | `20000` | long-poll ceiling |
| `wiggle.retention.millis` | `WIGGLE_RETENTION_MILLIS` | `86400000` | terminal instance retention |

Workers read `WIGGLE_URL`, `WIGGLE_WORKER_ID` and `WIGGLE_WORKER_CONCURRENCY`;
everything else is `WorkerOptions`.

---

## Modules

| Module | Contents |
|---|---|
| `core` | JSON reader/writer, record↔JSON binder, compiled graph model, retry policy, wire records |
| `proto` | `wiggle.proto` (the `WiggleControlPlane` gRPC service), generated stubs, `ProtoJson` |
| `server` | `Storage` SPI with in-memory and JDBC implementations, engine, election, housekeeper, gRPC API |
| `client` | the DSL, `WiggleClient`, the pulling `Worker` |
| `example` | order-fulfilment workflow, embedded `Demo`, standalone `WorkerMain` / `SubmitOrders` / `ClusterStatus` |
| `tests` | 18 conformance scenarios plus a JUnit wrapper |

Schema DDL lives in `JdbcStorage.migrate()` and is applied on startup: `wf_definition`,
`wf_instance`, `wf_token`, `wf_node`.

---

## Status and known limits

The in-memory path is exercised by 18 passing scenarios covering sequencing, filtering,
fork/join merge semantics, nested forks, retries, permanent failures, sleep timers,
lease reclamation, stale-lease rejection, cancellation, leader election with failover,
and work distribution across workers. The server, a worker and a submitter have also
been run as three separate JVMs over gRPC.

Worth knowing before you rely on it:

- **`JdbcStorage` has not been run against a live database.** It compiles and its SQL
  was checked column-by-column against the DDL, but the environment this was built in
  had no driver available. Multi-node clustering depends on it, so give it a pass
  against Postgres before trusting it.
- **`InMemoryStorage` uses one global lock.** It is correct and fine for development,
  but it is not a concurrency showcase — and it is single-process, so no clustering.
- **No `flatMap`.** Fan-out over a collection of unknown size at runtime is not
  expressible; `fork` needs its branches known at definition time.
- **No compensation or saga rollback.** A failed instance stops; it does not unwind.
- **Long-polling holds a virtual thread per waiting worker.** Cheap, but not free.
- **No auth or TLS on the gRPC API** (plaintext channels both sides). Put it behind
  something before it leaves your network.
