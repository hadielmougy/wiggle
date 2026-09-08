# Show HN draft

**Title** (80 chars max, no marketing tone — HN strips/punishes it):

> Show HN: Wiggle – a durable workflow engine with no replay and no determinism rules

**URL:** https://wiggle.sh

**Text** (posted as the first comment, per Show HN convention — personal, technical, honest):

---

Hi HN — I built Wiggle because I kept hitting the same wall with replay-based workflow engines
(Temporal/Cadence lineage): the determinism contract. Workflow code there must re-execute
byte-identically against recorded history, which outlaws clock reads, unordered iteration, and —
most painfully — ordinary code changes while instances are in flight. The failure mode is the
worst kind: it detonates during recovery, in production, weeks after the mistake.

Wiggle's bet is that the engine shouldn't replay code at all. You describe the process as a
graph (a small Java DSL — steps, gates, fork/join, forEach, timers, signal waits, sub-workflows),
and the *graph* is what the server owns. A running instance is tokens on that graph, each token a
database row. Recovery is reading the rows. Handlers are plain methods matched by name — they can
use clocks, randomness, any library, and be redeployed at will. Go and Python workers speak the
same protocol, so one instance can have steps served by three languages.

The trade-off, stated honestly: your control flow is bounded by the DSL's operators. If your
orchestration is genuinely algorithmic, a replay engine's Turing-complete workflow code is the
right tool and its tax is fair. For the order flows / approval chains / onboarding pipelines that
make up most workflow traffic, the vocabulary covers it — and you get versioning for free (a
definition's version is the content hash of its graph; old instances finish on the graph they
started with).

The second design bet is cellular sharding: a namespace maps to cells, each a complete deployment
with its own database, behind a small Raft coordinator (embedded Ratis+RocksDB, no external
store). Resharding publishes a new hash ring under a new epoch — new work follows the new ring,
in-flight work drains in place, no data migration. Instance ids embed their routing
(orders.e0.s3.01J…) so there's no directory service on the request path.

Numbers, all from one M2 Pro laptop (methodology in the repo): ~91k durable step completions/sec
embedded in one JVM; ~300 workflow starts/sec sustained with sub-second completion latency on a
kind cluster with PostgreSQL-backed cells; SIGKILL-ing the coordinator under load costs a 5.4s
window on *new* starts only — running work doesn't notice, and state recovers exactly.

It's one JAR plus a database (or in-memory for tests — the server embeds in a JVM for integration
tests, which is how the engine's own suite works). Apache-2.0. I'd genuinely value skeptical
questions about the model's limits — and if you try it and something is confusing in the first
ten minutes, that's a bug in the docs I want to hear about.

Site: https://wiggle.sh · Code: https://github.com/hadielmougy/wiggle

---

**Posting notes**
- Post morning US Eastern, Tue–Thu. Don't submit and vanish: the first 2 hours of replies decide
  the thread.
- Expect (and welcome) the "why not Temporal?" question — the prepared reply is in
  `why-not-temporal-reply.md`. Never disparage; concede their strengths explicitly.
- If asked about production readiness: be precise about what's tested (the benchmark + resiliency
  methodology) and what's young (the honest roadmap is public).
