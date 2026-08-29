# Scaling durable workflows by cells

*How Wiggle adds throughput by adding a database + cluster — not a bigger box. With numbers, a
repeatable harness, and the caveats that keep them honest.*

## TL;DR

Wiggle is a durable, **cellular** state-machine platform: a namespace can be split into *cells*, and a
cell is its own database and its own cluster. In a controlled ramp on Postgres, moving a namespace from
one cell to two **roughly doubled** the sustainable throughput — from saturating at ~100 processes/sec
to still-healthy at 100/sec and saturating around 150 — with tail latency staying flat until the
ceiling. The absolute numbers are laptop-grade and mean little; the **ratio** is the point, and it's
reproducible in one command.

## The problem: one cluster, one database, one ceiling

Durable execution engines persist every workflow's state so it survives crashes and resumes exactly
where it left off. That durability lives in a database, and the database is where you eventually hit a
wall: claim contention, write throughput, lock pressure. Most systems answer this by scaling *up* (a
bigger database, internal sharding inside one cluster) and by making tenancy *logical* — many
namespaces sharing one persistence layer.

That's fine until it isn't: a noisy tenant, a hot shard, or a compliance requirement for
database-per-customer, and suddenly "logical isolation on shared storage" is the thing biting you.

## The idea: a namespace is a cell

Wiggle takes a different tack, borrowed from cellular architecture. A **namespace becomes a cell** —
its own database *and* its own cluster of server nodes. An optional **coordinator** maps namespaces to
cells and places instances across them by consistent hashing over epochs. Two properties make this
practical:

- **Directory-free routing.** An instance id *carries its own placement* (`ns.e{epoch}.s{shard}.{ulid}`),
  so any client can route to the owning cell by parsing the id — no lookup table to keep consistent.
- **Drain, don't migrate.** To rebalance, you open a new epoch; the old one drains as its instances
  finish and is retired once empty. No data is moved.

The consequence for scaling is the interesting part: **adding a cell adds a whole independent stack** —
another database, another set of workers, another failure domain. You scale by *partitioning the whole
system*, not by growing one piece of it. A coordinator-aware worker (`NamespaceWorker`) fans its polling
across every active cell and shifts as the ring rebalances, so the work follows the cells automatically.

## The experiment

Everything below is one script, [`scripts/loadtest.sh`](../../scripts/loadtest.sh). It runs in-process
workers against a target, submits new processes at a paced open-loop rate, and records **end-to-end
latency** — from the client calling `start()` to the flow's step actually executing on a worker,
captured worker-side so there's no polling overhead skewing the measurement. It ramps through a series
of rates, printing per-step throughput and p50/p95/p99/max, and stops the ramp when it detects overload
(backlog growing faster than it drains).

The workload is deliberately minimal — a one-step flow whose handler just records its own latency — so
we're measuring the engine's dispatch path, not business logic.

Setup, via the bundled playground:

```bash
scripts/playground.sh up          # a coordinator + several cells on Postgres, in Docker

# same ramp, same knobs — the only variable is cell count
scripts/loadtest.sh --coordinator 127.0.0.1:8099 --namespace ns1     --concurrency 8   # 1 cell
scripts/loadtest.sh --coordinator 127.0.0.1:8099 --namespace orders  --concurrency 8   # 2 cells
```

Both runs go through the coordinator and use the same `NamespaceWorker` path; the two-cell run
round-robins new starts across its cells so both databases are actually exercised.

## Results

Single-cell namespace (`ns1`, one database):

```
  rate  start/s   done/s     p50     p95     p99     max   backlog
    25     25.0     25.0      63     118     120     129         0
    50     50.0     49.4      63     111     116     120         0
   100    100.0     37.8    6726   13020   13573   13761         0   ← saturated
   150    150.0     38.0    8515   16539   17220   17388       352   ← overloaded, ramp stops
```

Two-cell namespace (`orders`, two databases):

```
  rate  start/s   done/s     p50     p95     p99     max   backlog
    25     25.0     25.0      62     114     118     119         0
    50     50.0     49.8      60     112     117     124         0
   100    100.0     99.5      61     110     115     120         0   ← still healthy
   150    150.0     74.0    4010    7849    8171    8264         0   ← saturating
   200    200.0     75.4    6654   12921   13471   13706         0
```

Side by side, at the rate where it matters:

| offered | 1 cell — done/s · p99 | 2 cells — done/s · p99 |
|--:|:--|:--|
| 50/s  | 49 · 116 ms | 50 · 117 ms |
| 100/s | 38 · **13.6 s** (saturated) | 100 · **115 ms** |
| 150/s | overloaded | 74 · 8.2 s (saturating) |

The single-cell namespace is comfortable to ~50/sec and falls over by 100. The two-cell namespace
sails through 100/sec at 115 ms p99 with zero backlog, and only begins to saturate around 150. The
clearest single number: **at 100/sec, one cell's p99 is 13.6 seconds; two cells' is 115 milliseconds.**

Adding a cell roughly doubled the ceiling.

## What this does — and does not — prove

I'd rather you trust the shape than the digits, so here's what's really going on:

- **It's a ratio, not a spec sheet.** This is a laptop, a Docker Postgres, and a trivial one-step flow.
  Your absolute numbers will differ by orders of magnitude in either direction depending on hardware,
  database, and workload. Run the harness against *your* setup — that's why it's one command.
- **The two "databases" shared one Postgres container.** In the playground, each cell gets its own
  *logical* database, but they live in the **same Postgres process**. So this result isolates the gains
  from splitting the queue and tables across databases and from doubling the worker pool — it does *not*
  yet include the bigger win of separate database *hosts*. A production two-cell deployment would put
  cells on independent instances, which is where physical isolation and independent hardware scaling
  actually land. That the ceiling still roughly doubled while sharing one Postgres process is, if
  anything, the more surprising half of the result.
- **A cell scales the whole unit.** The two-cell namespace has 2× the databases *and* 2× the per-cell
  workers. That's not double-counting — it's the definition of a cell. Scaling by cells means scaling
  the entire stack together, on purpose.
- **There's a latency floor.** Both configurations sit around ~100–120 ms p99 at low rates, independent
  of load — that's the worker poll/dispatch cadence, not contention. Comfortably below most needs, but
  it's the number to beat if you're chasing tail latency, and it won't shrink by adding cells.

And a correctness note, because throughput without correctness is worthless: well below these ceilings,
behavior is boring in the best way — steady load, flat latency, no backlog, every instance finishing.

## Reproduce it

```bash
git clone <repo> && cd wiggle
scripts/playground.sh up
scripts/loadtest.sh --coordinator 127.0.0.1:8099 --namespace ns1     --rates 25,50,100,150 --concurrency 8
scripts/loadtest.sh --coordinator 127.0.0.1:8099 --namespace orders  --rates 25,50,100,150,200 --concurrency 8
scripts/playground.sh down
```

Point `--server host:port` at a single node for a direct baseline, or `--coordinator … --namespace …`
at your own cells. Tune `--rates`, `--step`, and `--concurrency` to find where *your* deployment bends.

## The takeaway

The headline isn't a throughput number — it's a scaling *shape*. When the database is your ceiling, you
can either buy a bigger one, or split the namespace into cells and get another database, another worker
pool, and another failure domain in the bargain. Wiggle is built so that split costs no data migration
and no routing directory: open an epoch, let the old one drain, and the workers follow.

Cells are the scaling unit. Add one when you need more.
