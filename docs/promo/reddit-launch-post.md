# Reddit launch post

**Where:** r/java (flair: "Open Source"). NOT r/programming — "I made this" project posts are
off-topic there and get removed. Also suitable, lightly adapted: r/softwarearchitecture,
r/ExperiencedDevs (only in a relevant discussion, never as a bare plug).

**Title:**

> Wiggle — an open-source durable workflow engine where the workflow is data, not replayed code (looking for contributors)

**Body:**

---

I've been building **[Wiggle](https://wiggle.sh)** — an Apache-2.0 durable workflow engine — and
it's at the point where it needs more eyes and hands than mine.

**The one-paragraph pitch:** you define a business process as a graph with a small Java DSL
(steps, gates, fork/join with *explicit* merges, dynamic fan-out, timers, human-approval signal
waits with deadlines, sub-workflows). The compiled graph is what the server owns; a running
instance is tokens on the graph, each one a database row. So processes survive crashes, restarts,
and worker death with **no event-sourced replay and no determinism rules** — handlers are plain
Java/Go/Python methods that can use clocks, randomness, and any library, and be redeployed at
will. Versioning is the content hash of the graph.

What I think is genuinely different:

- **No implicit state merging, anywhere.** A step's return *replaces* the context; parallel
  branches run isolated and rejoin only through a combine step you write. An entire class of
  "why does my context look like this?" bugs is structurally absent.
- **Cellular sharding.** A namespace maps to cells — each a full cluster with *its own database* —
  behind a small Raft coordinator. Resharding is an epoch bump on a hash ring: no data migration,
  ever. Physical per-tenant blast-radius isolation, not logical namespacing.
- **Small operational surface.** One JAR + one RDBMS (Postgres/MySQL/Oracle/SQL Server, or
  in-memory). The server embeds in a JVM for tests. Coordinator optional. No Elasticsearch, no
  sidecars.

Honest numbers from one laptop: ~91k durable steps/sec embedded; ~300 starts/sec sustained
sub-second end-to-end on a kind+Postgres cluster; killing the coordinator under load costs a
~5s window on new starts only. Methodology and tools are in the repo — run them yourself.

**Where I'd love help:** the console's topology view, saga/compensation helpers, buffered
signals, more storage backends, and battle-testing the Go/Python clients. The roadmap is honest
and public. If you've operated Temporal/Camunda/Airflow and have opinions about what they get
wrong (or right!), your review of the model would be worth a lot.

Site & docs: **https://wiggle.sh** · Code: https://github.com/hadielmougy/wiggle

---

**Posting notes**
- Answer every substantive comment in the first hours; concede trade-offs early ("control flow
  is bounded by the DSL — if your orchestration is truly algorithmic, use a replay engine").
- Don't cross-post the same day; stagger communities a week apart.
