# Wiggle — Python client

An idiomatic Python client and worker for the [Wiggle](../../README.md) workflow engine. It speaks
the same gRPC control plane as the Java client, so **Python and Java workers interoperate** on the
same server: define a workflow in either language, and any worker that registers the matching
handlers can run its steps.

- **Control client** — register workflows, start and track instances, deliver signals, manage schedules.
- **Worker** — pull tasks you have capacity for, run handlers, report results; automatic lease heartbeats and retries.
- **Fluent DSL** — build a workflow as a chain of `step`/`gate`/`sleep`/`await_signal`, with optional per-step `retry` and `queue`.

## Install

```bash
pip install grpcio protobuf
# from a checkout:
pip install clients/python            # or: pip install -e clients/python
```

Requires Python 3.9+ and a running Wiggle server (`docker run … hadielmougy/wiggle`, or `./gradlew :dist:run`).

## Quick start

```python
from wiggle import Workflow, Retry, WiggleClient, Worker

# 1. Define a workflow. The context is a plain dict; a step returns the whole context.
wf = (Workflow("order")
      .step("validate", lambda o: {**o, "status": "VALIDATED"})
      .gate("in-stock", lambda o: o["quantity"] > 0)                 # false -> ends as gated:in-stock
      .step("charge", charge, queue="payments", retry=Retry.exponential(5, 0.1))
      .sleep("cool-off", seconds=1)
      .effect("notify", lambda o: print("shipped", o["orderId"]))
      .build())

with WiggleClient("localhost:8080") as client:
    client.register(wf)
    worker = Worker(client, "worker-1").register(wf).start()   # background threads
    try:
        iid = client.start(wf, {"orderId": "A-1", "quantity": 3})
        view = client.await_completion(iid, timeout_s=30)
        print(view.status, view.context)                       # COMPLETED {...}
    finally:
        worker.stop()
```

Run the bundled example against a server on `:8080`:

```bash
PYTHONPATH=clients/python python clients/python/examples/order.py
```

## The DSL

| Operator | Meaning |
|---|---|
| `step(name, fn, *, queue=None, retry=None)` | run `fn(ctx) -> ctx` on a worker; the returned context is merged back |
| `then(name, fn, ...)` | alias for `step`, reads well when sequencing |
| `effect(name, fn, ...)` | run `fn(ctx)` for a side effect; context unchanged |
| `gate(name, test, ...)` | continue only while `test(ctx)` is true; false ends the instance as `gated:<name>` |
| `fork(Branch.of(name, body), …)` | run branches **in parallel**, then wait for all of them (join) |
| `fork_each(name, items_key, item_key, body)` | runtime fan-out: one parallel branch per element of the list at `items_key` |
| `choose(Case.when(name, guard, body), …, Case.otherwise(name, body))` | exclusive choice: the first matching guard's branch runs |
| `do_while(name, cond, body)` | run `body`, then repeat while `cond(ctx)` holds (body runs at least once) |
| `sub_workflow(name, child)` | run another workflow (a `Blueprint`, `Workflow`, or name) as a child; its result merges back |
| `sleep(name, *, seconds=, millis=)` | server-side timer; no worker is held |
| `await_signal(name, *, timeout_s=0)` | wait for a signal delivered via `client.signal(...)` |
| `default_queue(q)` | queue for every step that doesn't set its own (defaults to the workflow name) |
| `build()` | produce a `Blueprint` to register and serve |

A branch/case body is a function that receives a nested builder and chains onto it:

```python
wf = (Workflow("order")
      .step("validate", validate)
      .fork(                                              # parallel, joined
          Branch.of("payment",  lambda b: b.step("charge", charge)),
          Branch.of("shipping", lambda b: b.step("reserve", reserve).step("label", label)))
      .choose(                                            # exactly one arm runs
          Case.when("vip", lambda o: o.get("vip"), lambda b: b.step("concierge", concierge)),
          Case.otherwise("standard", lambda b: b.step("thanks", thanks)))
      .step("notify", notify)
      .build())
```

Branches touching different fields merge cleanly; if two write the same key, the later write wins.
A `gate` inside a branch short-circuits to that fork's join (not the whole instance).

Runtime fan-out spawns one branch per list element, each seeing its element (and index):

```python
wf = (Workflow("charge")
      .fork_each("charge-items", "items", "item", lambda b: b
          .step("price", lambda o: {**o, f"priced-{o['itemIndex']}": o["item"] * 10}))
      .step("summarise", summarise)
      .build())
# start(wf, {"items": [1, 2, 3]}) -> priced-0..2 ; an empty/missing list skips straight through
```

Branches share one context, so put per-element results under per-element keys (use the index).

`Retry.exponential(attempts, initial_s)`, `Retry.fixed(attempts, backoff_s)`, `Retry.none()`,
`Retry.forever()`. Raise `wiggle.PermanentError` from a handler to fail a step **without** retrying.

The builder covers the full operator set —
`step`/`gate`/`fork`/`fork_each`/`choose`/`do_while`/`sub_workflow`/`sleep`/`await_signal` — matching
the Java DSL. A `sub_workflow`'s child must be registered separately (`client.register(child)`), and a
worker must serve the child's handlers too (`Worker(...).register(child).register(parent)`). Because
handlers are keyed by activity name (`"<workflow>#<step>"`), Python and Java workers interoperate:
either can run the other's steps.

## Client API

```python
client.register(blueprint) -> int                     # version
client.start(blueprint_or_name, context, *, version=None, correlation_id=None) -> instance_id
client.instance(id) -> InstanceView                   # .status .context .termination_reason .error
client.await_completion(id, timeout_s=30) -> InstanceView
client.list_instances(workflow=None, status=None, limit=100)
client.cancel(id, reason="cancelled")
client.signal(id, name, payload=None)
client.create_schedule(workflow, every_s=..|cron=.., context=None) / list_schedules() / delete_schedule(id)
client.health() / client.cluster()
```

## Versioning

By default a workflow's version is a **content hash of its structure** — node kinds, names,
activities, queues, retries, and the edge topology — *independent of internal node-id numbering*.
So the same structure always yields the same version: re-registering is idempotent, the server
de-duplicates, and changing the graph mints a new version (in-flight instances keep running on the
old one). This is the safe, content-addressed default; you never set a number.

Pin an explicit version when you want a stable, human-meaningful one (or to match another client):

```python
wf = Workflow("order", version=3).step("validate", validate)....build()
```

With an explicit version **you** own bumping it when the graph changes — the server overwrites the
stored graph for a reused `name:version`, which affects instances already running on it. Leave it
unset unless you have a specific reason.

> Note: `client.start("name", …)` uses the **latest** registered version. Don't register the *same*
> workflow name from two clients/definitions with different graphs; give them distinct names, or
> define the workflow in one place. (The Python content hash is its own — it does not equal the Java
> client's number for the "same" workflow; cross-language interop is by activity name, not version.)

## Tests

Offline tests (no server needed) cover the DSL graph shapes and the wire conversions:

```bash
pip install grpcio protobuf pytest
cd clients/python && pytest -q
```

They're also wired into Gradle — `./gradlew build` (or `:clients:python:pyTest`) runs them, and
**skips cleanly** (a warning, not a failure) when Python 3 / grpcio / pytest are absent, or with
`-PskipPython`.

## Notes & limits

- **Execution mode:** Python-defined workflows run in `SERVER` mode. The worker does not implement
  the `LOCAL_SYNC`/`LOCAL_ASYNC` (client-side chaining) protocol, so serve those with a Java worker.
- **Numbers:** context travels as protobuf `Value` (doubles). Whole numbers come back as `int`;
  fractional values as `float` — the same JSON-number reality as the rest of the system.
- **TLS:** pass `WiggleClient(target, credentials=grpc.ssl_channel_credentials(...))`.
- **Regenerating stubs:** `pip install grpcio-tools && ./codegen.sh` after changing the proto.
