# The event log

Status: **implemented** — the log and its lifecycle entries (§2), the pull-and-ack feed (§3)

## 1. What it is

Wiggle already knows when an instance starts, completes, fails, is cancelled, or unwinds. The
event log is that knowledge made durable and consumable: one append-only entry per lifecycle
transition, written **in the transaction that made the transition**, so a reader of the log and
a reader of `wf_instance` can never disagree. Nothing is reconstructed after the fact and
nothing is lost when a node dies between the change and the notification.

It is an outbound feed. Other systems react to what Wiggle decided, without polling instances,
tailing logs, or being handed a webhook that may or may not arrive. It is not the inbound
direction: reports of steps your services ran themselves are
[observed execution](observed-execution.md).

## 2. What is in it

| Type | When |
|---|---|
| `wf.started` | an instance is created, including an observed run's first report |
| `wf.completed` | it ran out of flow at an END with nothing left |
| `wf.failed` | it failed with nothing to undo |
| `wf.cancelled` | someone cancelled it |
| `wf.compensating` | it failed with compensations recorded, and the reverse pass took over |
| `wf.compensated` | the reverse pass undid everything |
| `wf.compensation_failed` | the reverse pass could not finish; the instance needs a human |

The `wf.` prefix is reserved for the engine. Every entry carries the instance, workflow,
version, correlation id, and a payload holding the transition's `reason` or `error`, versioned
as envelope 1 and upcast on read like an instance context.

`seq` orders the log and only goes forward. It is **not** gapless: a transaction that rolls
back leaves its seq unused, and a consumer that assumes `seq + 1` will stall on the first
rollback. Order is what the log promises; density is not.

## 3. Reading it: pull and ack

A consumer is a **named cursor**, not a subscription. It polls for what lies beyond its cursor
and acknowledges what it has handled; the cursor moves only on the ack. So a consumer that dies
mid-batch is served the same entries again on its next poll — **at-least-once**, and handlers
must be idempotent. Keying on instance id and `seq` is enough.

<!-- snippet: event-log/consume -->
```java
while (true) {
    List<EventView> batch = client.pollEvents("billing", 100, 20_000, -1);
    if (batch.isEmpty()) continue;                 // the long poll expired: ask again
    for (EventView e : batch) {
        handle(e);                                 // your side of it, idempotent by instance + seq
    }
    client.ackEvents("billing", batch.getLast().seq());
}
```

`pollEvents(consumer, max, waitMillis, startFrom)` long-polls exactly as a worker poll does,
clamped by `WIGGLE_LONGPOLL_MAX_MILLIS`. `startFrom` applies only when the poll **registers** a
consumer: `0` starts at the tail, so only what is appended from now on; `-1` starts at the
earliest entry still retained; any other value resumes after that seq. Once a cursor exists,
`startFrom` is ignored, so a restarting consumer keeps its place with no special case in its
own code.

`ackEvents(consumer, ackedSeq)` is cumulative and never moves backwards, so a replayed ack is
harmless and an ack past the log's head is clamped to it. Two consumers never interfere: each
has its own cursor and its own pace.

### Why an entry is not served the instant it is written

`seq` is assigned by the database, and two appends can be assigned seqs in one order while
committing in the other. A consumer served the later seq immediately would step over the
earlier one forever, since its cursor has already passed it. So the feed holds back entries
younger than `WIGGLE_EVENTS_VISIBILITY_MILLIS` (default 50), which is longer than an append
stays in flight. The cost is a few tens of milliseconds of latency; the alternative is silently
dropped entries.

## 4. Retention

The log is trimmed by the leader's retention sweep: an entry goes once it is older than
`WIGGLE_EVENTS_RETENTION_MILLIS` (default seven days) **and** every consumer has acknowledged
it. With no consumer registered, age alone decides.

That combination is deliberate. A consumer that is merely slow keeps its backlog for as long as
the age cap allows and nothing is lost behind its back; a consumer that is abandoned cannot pin
the log forever, because the age cap eventually passes it. If you retire a consumer, nothing
more is needed than to stop polling: the age cap collects what it left.

| Env var | System property | Default | Meaning |
|---|---|---|---|
| `WIGGLE_EVENTS_RETENTION_MILLIS` | `wiggle.events.retentionMillis` | `604800000` | how long an acknowledged entry is kept |
| `WIGGLE_EVENTS_VISIBILITY_MILLIS` | `wiggle.events.visibilityMillis` | `50` | how long an entry is held back from the feed |

## 5. Storage

Migration 16. `wf_event` holds the log, keyed by a store-generated `seq`, indexed by instance
and by `created_at` (the retention sweep's and the visibility window's access path).
`wf_event_cursor` holds one row per consumer: its acknowledged seq and when it last polled.

## 6. Wire protocol

`PollEvents(PollEventsRequest) -> EventList` and `AckEvents(AckEventsRequest) -> Ack`, mirroring
the worker poll surface: the same long-poll clamping, the same backpressure hint on
`EventList.retry_after_millis` when the server is shedding load under memory pressure.
