# Reddit launch post

Written for r/java (tweak the greeting for other subreddits — see posting notes at the bottom).

## Title options

1. *I built Wiggle — a durable workflow engine for Java where the workflow is data, not replayed
   code. Looking for contributors*
2. *Wiggle: an Apache-2.0 workflow engine with cell-based sharding (Java 21, gRPC, Go/Python
   workers) — feedback & contributors welcome*

## Body

Hey everyone — I've been building **Wiggle**, an open-source durable workflow engine, and it's at
the point where I'd love feedback and contributors.

**What it is:** you describe a business process as a graph with a small `java.util.stream`-style
DSL, and Wiggle runs it as a durable state machine — it survives crashes, retries failures, waits
days for a human signal, and resumes exactly where it left off. Steps execute on pull-based
workers over gRPC (your services, no inbound ports, no broker).

```java
Blueprint orders = Workflow.define("order-fulfilment")
        .step("validate")
        .gate("in-stock")
        .fork(Branch.of("payment",  s -> s.step("authorise").step("capture")),
              Branch.of("shipping", s -> s.step("reserve-stock").step("print-label")))
        .combine("merge")
        .step("notify")
        .build();
```

The logic is plain methods in a `@Handlers` class, matched to steps by name — typed records or
raw maps, your pick per step.

**Two design choices I think are worth your attention:**

1. **The workflow is data, not replayed code.** Engines like Temporal replay your workflow
   function, so the code must be deterministic — no `Random`, no clocks, version-patching rules.
   Wiggle compiles the graph server-side and moves tokens over it, so there's no determinism
   contract to get wrong. (Trade-off: your control flow is limited to what the DSL expresses —
   `fork`, `choose`, `doWhile`, `forkEach`, signals, timers, sub-workflows.)
2. **Cellular sharding.** A namespace maps to *cells* — each cell is its own cluster with its
   **own database**. A Raft coordinator (embedded Ratis+RocksDB, no external store) spreads
   instances across cells by consistent hashing over epochs; the instance id carries its own
   routing (`orders.e0.s3.01J…`), and resharding never migrates data. Physical per-tenant
   isolation, and you scale by adding cells.

**Honest numbers:** ~310 workflow starts/sec (≈2,400 durable step executions/sec) sustained
through *one* cell — one server node + one Postgres — on my M2 Pro laptop with everything (k8s,
DB, worker, submitter) sharing 10 cores. Methodology and charts are in the README.

**What ships today:** clustering on a shared DB (Postgres/MySQL/Oracle/SQL Server), embedded
in-JVM mode for tests, a web ops console (live instance trace over the workflow diagram, signals,
cron schedules), a CLI for the control plane, and interoperable **Go and Python** workers — one
instance can have steps served by three languages.

**What it doesn't do (yet):** no saga/compensation helpers (a failed instance stops, it doesn't
roll back), signals aren't buffered, authorization on the gRPC plane is TLS/mTLS only, and it's
had one pair of eyes on it — mine.

**Which is the ask:** I'm looking for contributors and honest reviewers. Good entry points: the
console's topology/placement view, a `PendingSignals` RPC, compensation patterns, more client
languages, storage backends, or just trying it and filing sharp issues.

Repo: https://github.com/hadielmougy/wiggle (Apache-2.0, Java 21,
`io.github.hadielmougy:wiggle-client` on Maven Central)

Happy to answer anything about the internals — the epoch/resharding design and the no-replay
execution model are the parts I'd most like challenged.

## Posting notes

- **r/java** is the natural home — the body above is tuned for it, and library/project posts are
  on-topic there. Also good fits: **r/opensource** (explicitly welcomes projects seeking
  contributors — lead with the ask), **r/coolgithubprojects**, and a **Show HN** on Hacker News
  (same body works nearly verbatim).
- **Do NOT post this to r/programming** — "I made this" project/product demos are off-topic
  there and will be removed. The way to reach that audience later is a technical *article*
  submitted as a link — e.g. a deep-dive like "resharding without migrating data: consistent
  hashing over epochs" or "durable workflows without replay determinism" — where the project is
  the example, not the headline.
- **r/golang** / **r/Python** only work reframed around the client ("Go workers for a Java-hosted
  workflow engine") — hold those until a contributor asks.
- The "what it doesn't do" section is load-bearing — Reddit rewards it, and it pre-empts the top
  critical comments.
- Have the "why not Temporal?" reply ready ([why-not-temporal-reply.md](why-not-temporal-reply.md))
  — it's the inevitable first question.
- Post only after the landing README (with the benchmark section) is merged to `main`, so the
  repo link delivers what the post promises.
