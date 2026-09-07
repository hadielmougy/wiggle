# "Why not Temporal?" — prepared reply

The inevitable first comment under the launch post. Lead with respect (the fastest way to lose
the thread is to punch at Temporal unfairly), then three concrete differences, then an honest
"when you should still pick Temporal".

## Reply

Fair question — Temporal is excellent and battle-tested, and for a lot of teams it's the right
answer. Wiggle isn't a "Temporal killer"; it makes three different bets:

**1. No replay, no determinism contract.** Temporal executes your workflow *code* and replays it
against event history, so that code must be deterministic — no `Random`, no clocks, no
map-iteration surprises, and versioning in-flight workflows means `patched()`-style APIs. It's a
fair price for arbitrary code-defined control flow, but it's a whole class of production bugs.
Wiggle stores the workflow as a **compiled graph** server-side and moves tokens over it —
handlers are plain methods that can do anything, because they're never replayed. The trade-off is
real and I'll state it plainly: your control flow is bounded by the DSL's operators
(`fork`/`choose`/`doWhile`/`forkEach`, signals, timers, sub-workflows). If you need control flow
that only a Turing-complete function can express, Temporal wins.

**2. Operational footprint.** A Wiggle deployment is one JAR plus one JDBC database
(Postgres/MySQL/Oracle/SQL Server) — and it embeds in-process for tests. No visibility store, no
Elasticsearch, no separate frontend/history/matching services. You can read the whole engine in
an afternoon.

**3. Physical sharding, not logical namespaces.** Temporal scales one logical cluster over one
persistence layer; namespaces are logical. In Wiggle a namespace maps to **cells** — each with
its *own database and its own cluster* — and a small Raft coordinator spreads instances by
consistent hashing over epochs. The instance id carries its own routing, and resharding publishes
a new epoch instead of migrating data. That's blast-radius isolation you can point to: tenant A's
database melting down cannot touch tenant B's.

**When you should still pick Temporal:** you want a managed cloud, a mature ecosystem with years
of production hardening, dynamic code-defined workflows, or features Wiggle doesn't have yet (I
keep an honest list in the README — no saga/compensation helpers, signals aren't buffered, authz
is mTLS-only today). Wiggle is young and has had one pair of eyes on it — that's literally why
I'm posting.

## Usage notes

- Keep the "when to pick Temporal" paragraph even if the thread gets friendly — it's what makes
  the rest credible.
- If someone pushes back with "activities in Temporal aren't replayed either, only workflow
  code" — concede it straight away; it's true. The point stands that Wiggle has *no* replayed
  layer at all, so there's no determinism-constrained code anywhere in the programming model.
