# 60 — The event log

← [Sagas](50-sagas.md) · [Index](00-index.md) · Next: [Control-plane API](70-api.md)

An append-only, durable outbound feed: one entry per instance lifecycle transition, plus whatever a
handler chooses to put on it, each written in the transaction that made the change. Consumers pull and
acknowledge; nothing is pushed and nothing is reconstructed after the fact.

It is the outbound direction only.

## 1. What is written

**WGL-EVT-001** (MUST) Each lifecycle transition MUST append exactly one entry, in the **same
transaction** as the transition, so a reader of the log and a reader of the instance row can never
disagree.

**WGL-EVT-002** (MUST) The lifecycle types MUST be exactly:

| Type | When |
|---|---|
| `wf.started` | an instance is created |
| `wf.completed` | it ran out of flow at an `END` with nothing left |
| `wf.failed` | it failed with nothing to undo |
| `wf.cancelled` | someone cancelled it |
| `wf.compensating` | it failed with compensations recorded and the reverse pass took over |
| `wf.compensated` | the reverse pass undid everything |
| `wf.compensation_failed` | the reverse pass could not finish |

**WGL-EVT-003** (MUST) The `wf.` prefix MUST be reserved for the engine.

**WGL-EVT-004** (MUST) Every entry MUST carry: `seq`, instance id, workflow, version, correlation id,
type, creation time, payload, and the node id of the step that emitted it (empty for a lifecycle
entry).

**WGL-EVT-005** (MUST) A lifecycle payload MUST hold the transition's reason or error, versioned as
envelope 1 and upcast on read like an instance context.

**WGL-EVT-006** (MUST) `seq` MUST be strictly increasing and MUST NOT be gapless: a rolled-back
transaction leaves its seq unused. Order is promised; density is not.

*Verified by:* `tests/EventLogTest`, `tests/EventStoreTest`.

## 2. Handler-emitted events

**WGL-EVT-010** (MUST) A handler MAY emit its own facts with `Step.emit(type, payload)` from inside a
step.

**WGL-EVT-011** (MUST) An emitted event MUST be buffered on the worker, ride the step's completion
report, and be appended by the server **in the transaction that settles the token**. An attempt that
throws after emitting MUST leave nothing behind, and its retry MUST emit fresh.

**WGL-EVT-012** (MUST) There MUST be no window in which a consumer sees an event for work that was
rolled back, and none in which work is committed but its event lost.

**WGL-EVT-013** (MUST) A consumer sees an emitted event at **step completion**, not at the `emit` call —
under `LOCAL_ASYNC`, at the next batch flush.

**WGL-EVT-014** (MUST) The type MUST NOT start with `wf.`, and the payload MUST be an object (a record
or a map). Both MUST be enforced at the call **and again** at the server; breaking either fails the
step like any other bad argument.

**WGL-EVT-015** (MUST) Each emitted entry MUST record the node it came from, because a retry or a batch
makes the step ambiguous from outside.

**WGL-EVT-016** (MUST) Instance, workflow, correlation id, node and seq MUST be filled in server-side
from the token, so a worker cannot claim an event belongs to another run.

**WGL-EVT-017** (MUST) Emitted events MUST preserve emission order within a step.

*Verified by:* `tests/EmittedEventTest`.

## 3. Reading: pull and ack

**WGL-EVT-020** (MUST) A consumer MUST be a **named cursor**, not a subscription. It polls for what lies
beyond its cursor and acknowledges what it handled; the cursor moves only on the ack.

**WGL-EVT-021** (MUST) Delivery MUST be at-least-once: a consumer that dies mid-batch is served the same
entries again. Keying on `shard` and `seq` MUST be enough to deduplicate
([WGL-SHARD-135](85-sharding.md#10-event-log)).

**WGL-EVT-022** (MUST) `PollEvents(consumer, max, waitMillis, startFrom)` MUST long-poll exactly as a
worker poll does, clamped by the server's long-poll maximum, and MUST carry the same backpressure hint
when the server is shedding load.

**WGL-EVT-023** (MUST) `startFrom` MUST apply only when the poll **registers** the consumer: `0` = tail
(only what is appended from now on), `-1` = the earliest entry still retained, any other value = resume
after that seq (refused on a sharded log, [WGL-SHARD-136](85-sharding.md#10-event-log)). Once a cursor
exists `startFrom` MUST be ignored, so a restarting consumer keeps its place with no special case in its
own code.

**WGL-EVT-024** (MUST) `AckEvents(consumer, ackedCursor)`, with the cursor a served entry carries, MUST
be cumulative, MUST never move backwards on any shard, and MUST clamp a position past a shard's head
to that head. `AckEvents(consumer, ackedSeq)` behaves the same on a log that is not sharded
([WGL-SHARD-134](85-sharding.md#10-event-log)).

**WGL-EVT-025** (MUST) Consumers MUST NOT interfere: each has its own cursor and its own pace.

**WGL-EVT-026** (MUST) The feed MUST hold back entries younger than `WIGGLE_EVENTS_VISIBILITY_MILLIS`
(default 50). Two appends can be assigned seqs in one order and commit in the other; a consumer served
the later seq immediately would step over the earlier one forever, since its cursor has already passed
it. The cost is tens of milliseconds of latency; the alternative is silently dropped entries.

*Verified by:* `tests/EventFeedTest`, `tests/EventStoreTest`.

## 4. Retention

**WGL-EVT-030** (MUST) The leader's retention sweep MUST trim an entry only when it is older than
`WIGGLE_EVENTS_RETENTION_MILLIS` (default 7 days) **and** every registered consumer has acknowledged
it.

**WGL-EVT-031** (MUST) With no consumer registered, age alone MUST decide.

**WGL-EVT-032** (MUST) A merely slow consumer MUST keep its backlog for as long as the age cap allows,
and an abandoned consumer MUST NOT be able to pin the log forever.

**WGL-EVT-033** (MUST) Retiring a consumer MUST require nothing but ceasing to poll.

*Verified by:* `tests/EventStoreTest`.

## 5. Storage and wire

**WGL-EVT-040** (MUST) Each shard's `wf_event` holds its log keyed by a store-generated `seq`, indexed by
instance and by creation time (the access paths of the retention sweep and the visibility window).
`wf_event_cursor`, on the home shard, holds one row per consumer: its acknowledged seq on the home shard
and when it last polled; `wf_event_cursor_shard` holds its position on every other shard.

**WGL-EVT-041** (MUST) Emitted events MUST travel on the report a worker already sends
(`StepResult.events`), carrying only a type and a payload.

**WGL-EVT-042** (MUST) The feed MUST be exposed as `PollEvents` and `AckEvents` on the control plane
([WGL-API-080](70-api.md)).
