# Declarative workflows (YAML)

An alternative to the fluent Java DSL for authoring a workflow's **topology**. A YAML file
describes the graph — steps, branches, loops — and is registered with the server using the `wiggle`
command-line tool. The step *logic* is not in the file: every `task`, `gate`, and predicate is a
**named node**, and its handler is bound separately by name on a worker
(`worker.handle("<workflow>", "<step>", fn)`). Topology is data; logic is code.

This pairs with name-only binding: the file is the single source of truth for the shape and version,
and any worker — in any language — implements steps by activity name (`"<workflow>#<step>"`). On
`start()` the worker reconciles its handlers against the registered graph, so a mistyped or
wrong-kind step fails fast.

> This page is the schema reference (v1). A YAML file compiles to exactly the same graph the Java DSL
> produces; it is one front-end over the same `register` + `handle` machinery, not a separate engine.

## Document shape

One workflow per file: top-level metadata plus an ordered `steps` list (the root pipeline).

```yaml
workflow: order-fulfilment     # required — the registered name; activities are "<workflow>#<step>"
version: 3                     # optional — omit to use the content-hash version
defaultQueue: order-fulfilment # optional — defaults to the workflow name
steps:                         # required, >= 1 node
  - <node>
  - <node>
```

## Node catalog

Every item in a `steps` (or `body`/`then`/`escalation`) list is a **map with exactly one operator
key**, plus optional sibling attributes.

| operator | value | attributes | graph node(s) | handler? |
|---|---|---|---|---|
| `task` | step name | `queue`, `retry` | `TASK` | yes — `fn(ctx) -> ctx` |
| `effect` | step name | `queue`, `retry` | `TASK` | yes — `fn(ctx)`, ctx unchanged |
| `gate` | predicate name | `queue`, `retry` | `PREDICATE` | yes — `fn(ctx) -> bool` |
| `sleep` | duration | `name` | `SLEEP` | no |
| `await_signal` | signal name | `timeout`, `escalation` | `SIGNAL` | no |
| `sub_workflow` | see below | — | `SUB_WORKFLOW` | no (child runs itself) |
| `fork` | branch map | — | `FORK` + `JOIN` | no (branch steps do) |
| `fork_each` | see below | — | `DYN_FORK` + `JOIN` | no |
| `choose` | case list | — | `PREDICATE` cascade | guards do |
| `do_while` | see below | — | cycle + `PREDICATE` | condition does |

### Linear nodes

```yaml
- task: validate
- task: charge
  queue: payments
  retry: { max: 5, backoff: 100ms, multiplier: 2, maxBackoff: 5m, jitter: 0.2 }
- effect: notify
- gate: in-stock                # false -> ends the instance as gated:in-stock
                                #          (or short-circuits to the enclosing join, if nested)
```

### sleep

`name` is optional — auto-generated from the duration when omitted (it has no handler; the name is
only for describe/UI and the content hash).

```yaml
- sleep: 100ms                                # auto-named
- sleep: { name: await-warehouse, for: 2s }   # explicit name
```

### await_signal

Delivered via `client.signal(instanceId, "<name>", payload)`; the payload merges into the context
like a task result. Without `escalation`, a `timeout` fails the instance; with it, the escalation
branch runs on timeout and then rejoins the flow (exactly one of delivery / escalation happens).
`escalation` requires a positive `timeout`. `name` is auto-generated if the signal name is given via
a map form; in the shorthand the value *is* the name.

```yaml
- await_signal: approval
  timeout: 24h
  escalation:
    - task: auto-approve
```

### sub_workflow

Runs another registered workflow as a child; its final context merges back. The child must be
registered separately.

```yaml
- sub_workflow: { name: place-suborder, workflow: fulfil-backorder }
- sub_workflow: fulfil-backorder     # shorthand: node name defaults to the child name
```

### fork / join (static parallel, implicit join)

The `fork:` block *is* the fan-out + join unit — whatever follows runs after **all** branches
complete. There is no explicit `join` keyword, so a fork can never be unbalanced. Needs >= 2
branches. Branch outputs shallow-merge into one context (**last write wins per key**), so put
per-branch results under per-branch keys.

```yaml
- fork:
    payment:                    # branch name (label only)
      - task: charge
        queue: payments
    shipping:
      - task:  reserve-stock
      - sleep: 100ms
      - task:  print-label
- effect: notify                # runs after the join
```

Compiles to `FORK -> {branch starts} -> JOIN(expected=2) -> notify`.

### fork_each (dynamic fan-out)

At run time the engine reads the list at `over` and spawns one branch per element, running `body`
with the element injected under `as` (and its position under `as + "Index"`). All branches join
before the flow continues; an empty or missing list skips straight through.

```yaml
- fork_each:
    over: lineItems             # itemsKey — a list already in the context
    as:   item                  # each element under `item` (+ `itemIndex`)
    name: price-each            # optional label for the DYN_FORK node
    body:
      - task: price-item
- task: sum-prices
```

Compiles to `DYN_FORK(itemsKey, itemKey) -> [template] -> JOIN(dynamic)`.

### choose (exclusive choice, no join)

The first matching guard's branch runs; the rest are skipped. A cascade of predicates — nothing runs
in parallel and there is no join. An optional `otherwise` (must be last) handles no match; without
one, no match skips the whole `choose`. Needs >= 1 guarded case.

```yaml
- choose:
    - when: is-vip              # predicate name — first match wins
      then:
        - effect: concierge
    - when: is-returning
      then:
        - effect: loyalty-bonus
    - otherwise:                # optional; must be last
        then:
          - effect: thanks
```

Compiles to a chain of `PREDICATE` nodes, each `false` edge -> the next case.

### do_while (loop, body-first)

Runs `body` once, then evaluates the `while` predicate on a worker; while it holds, the body runs
again. Compiles to a plain cycle, so the body always runs **at least once**. (Body-first is the
engine's native loop; a condition-first "run zero or more times" loop is not provided — put a `gate`
before the loop if you need it.)

```yaml
- do_while:
    body:
      - task: fetch-page
      - task: process-page
    while: has-more             # predicate name; true -> back to body start
- task: finalize
```

Compiles to `fetch-page -> process-page -> PREDICATE(has-more)`, with `has-more.true -> fetch-page`
and `has-more.false -> finalize`.

## Value grammars

### duration

`<number><unit>`, unit one of `ms | s | m | h` (e.g. `250ms`, `2s`, `24h`). The unit is required.

### retry

Either a preset string or a map:

```yaml
retry: none                    # 1 attempt, no retry
retry: forever                 # retry indefinitely
retry: { max: 5, backoff: 100ms, multiplier: 2, maxBackoff: 5m, jitter: 0.2 }
```

Map keys: `max` (int, required), `backoff` (duration, required), `multiplier` (float, default `1`),
`maxBackoff` (duration, optional), `jitter` (`0..1`, optional). Omitted → the workflow default.

## Full example

```yaml
workflow: order-fulfilment
defaultQueue: order-fulfilment
steps:
  - task: validate
  - gate: in-stock
  - fork:
      payment:
        - task: charge
          queue: payments
          retry: { max: 5, backoff: 100ms, multiplier: 2 }
      shipping:
        - task:  reserve-stock
        - sleep: 100ms
        - task:  print-label
  - await_signal: fraud-review
    timeout: 2h
    escalation:
      - task: auto-clear
  - do_while:
      body:
        - task: notify-attempt
      while: delivery-unconfirmed
  - effect: audit
```

Register the topology with the CLI, then implement the steps by name on a worker:

```bash
wiggle validate order-fulfilment.workflow.yaml            # compile + validate, no server
wiggle register order-fulfilment.workflow.yaml --server localhost:8080
```

```java
new Worker(client, "fulfilment")
        .handle("order-fulfilment", "validate", ...)
        .handleGate("order-fulfilment", "in-stock", ...)
        .handle("order-fulfilment", "charge", ...)
        .handle("order-fulfilment", "reserve-stock", ...)
        .handle("order-fulfilment", "print-label", ...)
        .handle("order-fulfilment", "auto-clear", ...)
        .handle("order-fulfilment", "notify-attempt", ...)
        .handleGate("order-fulfilment", "delivery-unconfirmed", ...)
        .handleEffect("order-fulfilment", "audit", ...)
        .start();   // reconciliation verifies every declared step has a handler of the right kind
```

## Validation (at load time)

- `workflow` is required; `steps` is non-empty.
- **Step names are unique** across the whole file — they become activity ids; duplicates are rejected.
- A node map must have **exactly one** operator key; unknown or multiple operator keys are an error.
- `fork` needs **>= 2** branches; each branch is non-empty.
- `choose` needs **>= 1** guarded case; `otherwise` must be the last case.
- `do_while.body` is non-empty and `while` is present.
- `fork_each` requires `over`, `as`, and a non-empty `body`.
- `await_signal.escalation` requires a positive `timeout`.
- A `duration` without a unit is an error.

## The `wiggle` CLI

The `cli` module builds a `wiggle` command that compiles this YAML with the same builder the Java DSL
uses (so it produces the identical graph and content-hash version) and registers it — no application
code required.

```bash
wiggle validate <file.yaml>                       # compile + run all validations offline, no server
wiggle register <file.yaml> [--server host:port]  # validate, then register (default: $WIGGLE_URL, else localhost:8080)
```

`validate` is a pure, offline check — ideal in CI to reject a bad spec before deploy. `register` is
the deploy-time step that publishes the topology; workers then bind their handlers by name and
reconcile against it on start. Build/run it with `./gradlew :cli:installDist` (produces
`cli/build/install/wiggle/bin/wiggle`) or `./gradlew :cli:run --args="validate path/to/file.yaml"`.

**TLS.** `register` reads the same `WIGGLE_TLS_*` environment as the server and workers, so a TLS
server needs no extra flags — set `WIGGLE_TLS_TRUSTSTORE` (+ password) to verify it, and
`WIGGLE_TLS_KEYSTORE` for mTLS. The equivalent flags override the environment per field, and `--tls`
forces TLS using the JVM default trust store (for a certificate signed by a public CA):

```bash
# private CA / self-signed
wiggle register order.yaml --server prod:8080 --tls-truststore /certs/trust.p12 --tls-truststore-password ...
# public CA — just switch TLS on
wiggle register order.yaml --server prod:8080 --tls
```

Password flags are `interactive` — pass them with no value to be prompted instead of putting the
secret on the command line.

### Installing the CLI

The CLI is a JVM application, so it needs a **recent JDK** on the machine. Three ways to get it:

**Release archive** (recommended). Download the archive attached to the GitHub Release, unpack it,
and put `wiggle` on your `PATH`:

```bash
V=2.1.6
curl -L "https://github.com/hadielmougy/wiggle/releases/download/v$V/wiggle-$V.tar" | tar -x
sudo ln -sf "$PWD/wiggle-$V/bin/wiggle" /usr/local/bin/wiggle
wiggle validate order.yaml
```

**Homebrew** (macOS/Linux):

```bash
brew tap hadielmougy/wiggle https://github.com/hadielmougy/wiggle
brew install hadielmougy/wiggle/wiggle
```

**From source** (contributors): `./gradlew :cli:installDist` produces
`cli/build/install/wiggle/bin/wiggle`.

## See also

- The fluent Java DSL — [DSL cookbook](dsl-cookbook.md).
- Name-only handler binding and reconciliation — the "Name-only binding" section of the
  [README](../README.md).
