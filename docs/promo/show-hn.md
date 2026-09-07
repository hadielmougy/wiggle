# Show HN post

Submit the **repo URL** (https://github.com/hadielmougy/wiggle) as the link, and paste the text
below into the text field. HN formatting: no bold/headers/markdown — paragraphs separated by
blank lines, code indented by 2+ spaces, `*word*` for italics. Keep it exactly this plain.

## Title options (HN cap: 80 chars)

1. `Show HN: Wiggle – a durable workflow engine without replay determinism`
2. `Show HN: Wiggle – durable workflows where the workflow is data, not code`
3. `Show HN: Wiggle – a cell-sharded durable workflow engine for the JVM`

## Text

Hey HN — I've spent the last months building Wiggle, an open-source durable workflow engine
(Java 21, Apache-2.0), and I'd love technical feedback and contributors.

You describe a process as a graph with a small stream-style DSL; Wiggle runs it as a durable
state machine — it survives crashes, retries failures, waits days for a human signal, and
resumes where it left off. Steps run on pull-based workers over gRPC (your services; no inbound
ports, no broker). Interoperable Go and Python workers exist — one instance can have its steps
served by three languages.

      Blueprint orders = Workflow.define("order-fulfilment")
              .step("validate")
              .gate("in-stock")
              .fork(Branch.of("payment",  s -> s.step("authorise").step("capture")),
                    Branch.of("shipping", s -> s.step("reserve-stock").step("print-label")))
              .combine("merge")
              .step("notify")
              .build();

Two design bets:

1. The workflow is data, not replayed code. Temporal-style engines replay your workflow
function, so it must be deterministic — no Random, no clocks, patch-style versioning of
in-flight runs. Wiggle compiles the graph server-side and moves tokens over it; handlers are
plain methods that are never replayed, so there is no determinism contract anywhere in the
programming model. The trade-off is real: control flow is bounded by the DSL's operators
(fork/choose/doWhile/forkEach, signals, timers, sub-workflows). If you need control flow only a
Turing-complete function can express, use Temporal.

2. Cellular sharding. A namespace maps to cells — each cell is its own cluster with its own
database. A small Raft coordinator (embedded Ratis+RocksDB, no external store) spreads instances
across cells by consistent hashing over epochs; the instance id carries its own routing, and
resharding publishes a new epoch instead of migrating data. Physical blast-radius isolation:
tenant A's database melting down cannot touch tenant B's.

The whole thing is one JAR plus a JDBC database (Postgres/MySQL/Oracle/SQL Server), and it
embeds in-process for tests — you can read the entire engine in an afternoon. Honest numbers:
~310 workflow starts/sec (~2,400 durable step executions/sec) through one cell — one server
node, one Postgres — on my laptop, with methodology and charts in the README.

What it doesn't do yet: no saga/compensation helpers (a failed instance stops, it doesn't roll
back), signals aren't buffered, authorization on the gRPC plane is mTLS-only, and it's had one
pair of eyes on it — mine. That's why I'm posting: the epoch/resharding design and the no-replay
execution model are the parts I'd most like challenged.

## Posting notes

- Post Tue–Thu, ~8–10am US Eastern. Be in the comments for the first 3–4 hours — Show HN lives
  or dies on author engagement.
- The "why not Temporal?" answer ([why-not-temporal-reply.md](why-not-temporal-reply.md)) works
  on HN verbatim; expect that question within the first five comments.
- Don't game votes (no asking friends to upvote — HN detects voting rings and kills the post).
  If it doesn't take, a repost weeks later after meaningful changes is acceptable HN etiquette.
- Expect and welcome deep technical pushback on exactly-once claims, the epoch model, and
  Postgres-as-a-queue — answer with specifics from the code, not adjectives.
