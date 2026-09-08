# "Why not just use Temporal?" — the prepared reply

For HN/Reddit threads. Tone: respectful, concede strengths first, be concrete about the
difference, never dunk. Adapt length to context; the short form usually wins.

## Short form

---

Fair question — Temporal is excellent, and if your orchestration logic is genuinely algorithmic
(control flow computed at runtime in ways no fixed operator set expresses), it's the right tool.

The difference is what gets persisted. Temporal makes your workflow *code* durable via
event-sourced replay, which requires that code to be deterministic: same history in, same
decisions out, forever — across deploys. That outlaws clock reads, unordered iteration, and
unversioned code changes inside workflow code, and the failure mode is a replay divergence in
production, often long after the mistake.

Wiggle persists the workflow as a *graph* the server owns — a running instance is tokens on that
graph, each a database row. Nothing is ever replayed, so there's no determinism contract:
handlers are plain methods that can use clocks, randomness, any library, and be redeployed
freely. The price is a bounded vocabulary (steps, gates, fork/join, forEach, timers, signals,
sub-workflows, loops) — Turing-complete orchestration is exactly what you give up.

So: replay engines make code durable and tax it with determinism; graph engines make data durable
and tax it with a vocabulary. Neither tax is avoidable — you pick the one your workloads can
afford. For order flows, approval chains, and onboarding pipelines, the vocabulary covers it and
the operational surface is much smaller (one JAR + one RDBMS — no history shards, no separate
frontend/matching/history services).

More on the design: https://wiggle.sh/why/

---

## Extra points, if pressed

- **"Temporal has versioning APIs for that."** Yes — `patched()`/worker versioning work, but they
  are discipline you must maintain forever, and the penalty for a miss is a production replay
  failure. Wiggle's versioning is structural: a definition's version *is* the content hash of its
  graph; in-flight instances finish on the graph they started with. There is no patching because
  there is nothing to keep replay-compatible.
- **"Replay is what gives you full history."** Wiggle keeps per-step execution records for its
  console trace (who ran what, when, attempts, context) — you lose the *time-travel debugger*
  style of replaying into a workflow function, which is genuinely nice in Temporal. Conceded.
- **"Is a DSL graph expressive enough?"** Operators: step, gate, choose, fork/combine, forEach
  (runtime fan-out), doWhile, sleep, awaitSignal (deadline + escalation), sub-workflows, retry
  policies, queues. What it cannot express: control flow computed from data in unbounded ways.
  If you need that, use Temporal — sincerely.
- **"Scale?"** Different model: Temporal scales one logical cluster via internal sharding; Wiggle
  scales by *cells* — each namespace slice is a complete deployment with its own database behind
  a Raft coordinator, and resharding never migrates data (epoch-versioned hash rings). It's
  blast-radius isolation first, throughput second.
- **Never claim** Wiggle is "Temporal but better." It's a different point in the design space
  with different taxes. Teams with heavy dynamic orchestration should not switch.
