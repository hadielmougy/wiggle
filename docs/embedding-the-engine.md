# Embedding the engine

`wiggle-engine` is the state machine on its own: the graph, the tokens, fork/join, `forEach`,
`doWhile`, sagas, retries, timers and signals, plus the storage SPI and an in-memory store — with
**no transport and no external dependencies**. Its whole POM is `wiggle-core` and
`wiggle-placement`, both of which are dependency-free.

Reach for it when you want to run workflows *inside* your own process and build your own thing
around them: no server to deploy, no database to provision, no gRPC on the classpath.

```kotlin
implementation("sh.wiggle:wiggle-engine:0.0.6")
```

Reach for `wiggle-server` + `wiggle-client` instead when you want the deployable control plane —
workers polling over gRPC, multi-node clustering, the ops console. The engine is the same one; the
server is the process around it.

## The whole surface

An embedder wires three things — a store, a registry and the engine — and then drives it:

<!-- snippet: embedded-engine/embed -->
```java
try (Storage storage = new InMemoryStorage()) {
    storage.migrate();
    DefinitionRegistry registry = new DefinitionRegistry(storage);
    WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
    engine.register(greetThenShout());

    String id = engine.start("greet", null, Map.of("name", "ada"), "corr-1");

    // Your runner: claim what is ready, run it, report the result.
    long deadline = System.currentTimeMillis() + 10_000;
    while (isRunning(engine, id) && System.currentTimeMillis() < deadline) {
        for (TaskActivation task : engine.poll("embedder", Set.of(QUEUE), 10, null)) {
            engine.complete(task.taskId(), "embedder", handle(task));
        }
    }
}
```

`poll` leases tasks the way a worker would; `complete`, `fail` and `advance` report back. Between
them, that loop is the entire execution contract — how you dispatch (thread pool, virtual threads,
an actor, a single thread in a test) is yours to decide.

Dispatching a task is yours too. There is no handler binding here, so `activity()` is the hook:

<!-- snippet: embedded-engine/handler -->
```java
private static Object handle(TaskActivation task) {
    Map<String, Object> ctx = new LinkedHashMap<>(Json.asObject(task.context()));
    switch (task.activity()) {
        case "greet" -> ctx.put("greeting", "hello " + ctx.get("name"));
        case "shout" -> ctx.put("greeting", String.valueOf(ctx.get("greeting")).toUpperCase());
        default -> throw new AssertionError("unexpected activity " + task.activity());
    }
    return ctx;
}
```

Both blocks above are generated from `engine/src/test/java/.../EmbeddedEngineTest.java`, which is
the executable version of this page. It lives in the engine module, whose classpath is exactly what
the published artifact carries, so it cannot accidentally depend on anything an embedder would not
get — and if the API moves, the build fails here rather than leaving this page quietly wrong.

## Authoring definitions

The fluent authoring DSL (`FlowSpec`, `Wiggle.allOf`, `thenForEach`) ships in `wiggle-client`,
because it is also what a client uses to register a flow with a server. The engine takes a
`com.wiggle.core.WorkflowDefinition`, which you can build either way:

- add `wiggle-client` and use the DSL, then hand `spec.definition()` to `engine.register(...)`;
- or assemble `Node` values from `wiggle-core` directly, as the test does — more verbose, but it
  keeps the dependency set at rock bottom and is the better fit if you are *generating* topologies.

## What you take on yourself

The server does three things for the engine that you inherit responsibility for when embedding:

**Leader duties.** Timers, signal deadlines, expired leases and schedules only advance when
something calls them. Run them on a scheduler, roughly once a second:

<!-- snippet: embedded-engine/housekeeping -->
```java
engine.fireDueTimers(100);
engine.fireDueSignalDeadlines(100);
engine.reclaimExpiredLeases(100);
engine.fireDueSchedules(100);
```

Without this, a `sleep` never wakes and a task whose worker died is never re-dispatched — a due
timer stays invisible to `poll` until something promotes it, which the test pins down.

**Retention.** Terminal instances accumulate. `purgeTerminalInstancesOlderThan(retentionMillis, max)`
is the broom; nothing calls it for you.

**Handler binding.** `wiggle-client`'s `Worker` binds handler methods to step names by reflection,
but it talks to a server over gRPC, so it is not usable against an embedded engine today. Embedders
dispatch on `TaskActivation.activity()` themselves — a `switch`, a `Map<String, Handler>`, whatever
suits. (A transport seam that lets `Worker` drive an in-process engine is the natural next step
here; it is not in place yet.)

## Durability

`InMemoryStorage` is exactly what it says: when the process exits, the instances are gone. That is
the right trade for tests, for short-lived orchestration inside a request, and for building
something around the engine before deciding where state should live.

`Storage` is an interface. When you want durability, add `wiggle-jdbc` plus a dialect module
(`wiggle-postgres` covers PostgreSQL and H2) and hand `JdbcStorage` to the same `WorkflowEngine`
constructor — nothing above changes. Note that `StorageFactory` (which maps a `ServerConfig` to a
store) belongs to `wiggle-server`, not here: an embedder constructs the store it wants directly.
