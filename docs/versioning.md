# Versioning

Two things version independently, and confusing them is the usual source of trouble:

| | versioned by | changes when | who notices |
|---|---|---|---|
| **the topology** | you, at `define(...)` | you add, remove or rewire a node | the server: it refuses a changed graph under a published version |
| **the context** | nothing — it is your record | you add or drop a field | only the handler that decodes it |

The engine versions the *graph*. It has no opinion about the shape of the data flowing through it,
which is deliberate — the context is opaque JSON to the server — but it means schema evolution is a
separate job with a separate seam. Both are below.

---

## 1. You declare the version; the engine holds it immutable

The version is the second argument to `define(...)` — a positive integer you choose, so it stays
readable in logs, in the console, and in `registerHandler(handlers, 7)`. What the engine supplies is
the guarantee that goes with it: it fingerprints the compiled topology and **refuses to redefine a
version whose graph has changed**.

<!-- snippet: versioning/contract-v1,topology-v1 -->
```java
public interface OrderSteps {
    Order validate(Order o);
    Order charge(Order o);
}

FlowSpec v1 = FlowSpec.define("orders", 1, Order.class, OrderSteps.class, (f, s) -> f
        .thenApply(s::validate)
        .thenApply(s::charge));
```

Add a step and you publish it as a new version:

<!-- snippet: versioning/topology-v2 -->
```java
FlowSpec v2 = FlowSpec.define("orders", 2, Order.class, OrderStepsV2.class, (f, s) -> f
        .thenApply(s::validate)
        .thenApply(s::fraudCheck)      // a new step -- so a new graph, so a new version number
        .thenApply(s::charge));
```

Three consequences:

- **Re-registering an identical graph is a no-op.** Deploy the author as often as you like; a
  restart that registers the same flow changes nothing, on any number of replicas.
- **A changed graph under a *new* version sits beside the old one.** It redirects nothing and
  rewrites nothing. Both remain registered, and both remain startable.
- **A changed graph under an *existing* version is refused**, with `FAILED_PRECONDITION`:

  ```
  workflow 'orders:1' is already registered with a different graph;
  publish it under a new version
  ```

  Forgetting to bump is therefore a loud error at deploy time, not a graph silently swapped
  underneath the instances running on it.

> **Local edit-run loops.** Bumping the version on every keystroke is noise while you are still
> shaping a flow, so `client.register(spec, /* force */ true)` asks the server to replace the graph
> in place. The server refuses unless it was started with `WIGGLE_ALLOW_GRAPH_REPLACE=true`, so a
> `force` left in application code cannot rewrite a published graph in production.

**"Latest" means the highest version**, not the most recently registered — so re-publishing an older
version does not make it latest, and two replicas registering in different orders agree.

## 2. In-flight instances keep the version they started on

An instance records its version when it starts and runs that graph to completion, whatever is
registered afterwards. A deploy in the middle of a long-running instance does not move it, does not
re-plan it, and cannot strand it on a node that no longer exists.

This is where an engine that replays code has to think hard and wiggle does not: your code is never
re-executed to rebuild state, so a new deployment of handlers cannot change the history of a running
instance. The graph is data, the state is stored, and the version pins which graph.

## 3. Starting: latest, or pinned

<!-- snippet: versioning/start -->
```java
client.start("orders", ctx);                          // latest registered version
client.start("orders", ctx, spec.version(), "corr-1"); // pinned: immune to a mid-deploy change
```

Unpinned means *latest at the moment the instance starts*. That is usually right, and it is exactly
what you do not want during a deploy window, when "latest" can change between two calls. Pin when a
submitter must produce a homogeneous batch, or when a caller has to know precisely which graph it
asked for.

A submitter that starts by name and never pins picks up a new version automatically for **new**
starts only.

## 4. Workers serve every version by default

A worker binds handlers to step *names*, and names are stable across versions, so one implementation
usually covers them all. That is the default and it is almost always what you want: `validate` in v1
and `validate` in v2 are the same method.

**But it binds against one graph, not all of them.** An unscoped worker fetches the *latest*
registered version and validates its handlers against that. A method whose step exists only in some
*other* version is simply not bound — and a task for it fails with `no handler registered for
activity 'workflow#step'`. So "serves every version" means every version whose steps appear in the
graph it bound, which is why adding a step is safe (the new graph is a superset).

*Latest* is the highest declared version, so which graph an unscoped worker binds is deterministic
and independent of registration order. It is still a snapshot taken at `start()`: a worker that
started before a newer version was published has bound the older graph, and steps only the newer one
has are unbound on it until it restarts. Scope the workers when that matters.

Narrow it when they should not be:

<!-- snippet: versioning/scoped-workers -->
```java
new Worker(client, "service-a").registerHandler(new V1Handlers(), v1.version());  // only v1
new Worker(client, "service-b").registerHandler(new V2Handlers(), v2.version());  // only v2
```

A scoped worker filters its claim by `(workflow, version)`. This is what makes a **staged hand-over**
possible: service A keeps serving v1 while service B takes v2, A drains its in-flight instances, then
A retires. No shared deploy, no cutover, no moment where both must be right at once.

Without the scoping A would keep claiming v2's tasks and running them with v1's code — silently,
because the activity a handler binds is `workflow#step` and carries no version, so the names match
and nothing looks wrong. Binding a version that was never registered fails at `start()`, where a
deploy can fail cleanly, rather than as a decode error on the first task.

**The cost.** A version nobody serves has tasks nothing can claim, and they sit dispatchable
forever. That is not a crash and it is not an error — it is a queue that never drains, which is
harder to notice. The console's coverage view exists for exactly this; see
[§7.5 of the onboarding guide](/docs/onboarding/#75-backlog-coverage-work-nothing-can-claim).

## 5. The context is a separate problem

The topology version says nothing about the shape of your data. Add a field to `Order` and every
in-flight instance still has contexts written before that field existed. The default decode is
lenient by field name — a missing field arrives as `null`, an unknown one is dropped — which is
forgiving right up until `null` is not a valid value for that field.

The seam is `@Decode`: a method in the handler class that takes the raw persisted JSON and returns
the typed context, running instead of the reflective mapping wherever that type is bound.

<!-- snippet: versioning/decode -->
```java
@ForFlow("orders")
static class UpcastingHandlers implements OrderSteps {

    /** Runs instead of the reflective mapping wherever an Order parameter is bound. */
    @Decode
    public Order load(Map<String, Object> raw) {
        raw.putIfAbsent("currency", "USD");   // a field added after these instances started
        return (Order) RecordMapper.fromJson(raw, Order.class);
    }

    @Override public Order validate(Order o) { return new Order(o.id(), "VALIDATED", o.currency()); }
    @Override public Order charge(Order o) { return new Order(o.id(), "CHARGED", o.currency()); }
}
```

Because it is ordinary code, an upcast can do whatever it needs: default a field, rename one, split
one into two, or branch on a marker you stamped into the data yourself. If you want to know *which*
shape you are looking at, put a version field in the record and read it — the engine will not do it
for you, and a field you control is more honest than one inferred from the graph's version.

**Deploy the upcast before the field.** The worker that decodes an old context must already know how
to. That ordering is the whole discipline: readers first, then writers.

## 6. What to do, in practice

- **Adding a step, changing an edge, renaming a node** → a new version. Deploy handlers for both
  versions (usually the same class, unscoped), let the old instances drain.
- **Changing a handler's *body*** → not a version at all. The graph is unchanged, so there is nothing
  to register; just deploy. In-flight instances pick the new code up on their next step, which is
  either what you want or a reason to scope by version.
- **Moving a step to another service** → scope both workers by version and hand over in stages.
- **Adding a context field** → deploy an `@Decode` upcast first, then start writing the field.
- **Removing a context field** → keep decoding it until no in-flight instance predates its removal.

Everything on this page is asserted by `VersioningTest` in the project's own suite — the
fingerprint, the no-op re-registration, the refusal to redefine a published version, the gate on
`force`, the in-flight pin, and both worker scopings.
