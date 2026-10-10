# 87 — Partition ownership (proposed, spike)

← [Sharding](85-sharding.md) · [Index](00-index.md) · Next: [Operations](90-ops.md)

**Status: proposed, spike.** Nothing in this chapter is implemented. Every requirement here is
*unverified* ([WGL-GEN-001](00-index.md)) until the change that builds it names its test. Where this
chapter and the rest of the suite disagree, the rest of the suite describes the code as it is today.
The chapter is written to be built as a spike behind a flag ([§8](#8-the-spike)); whether it becomes
the default is decided by the spike's results ([§9](#9-go--no-go)).

On a 16 vCPU Cloud SQL primary the ceiling is 9,600 steps/s (1,200 instances/s) with the database at
only 63% CPU. The limit there is time spent waiting on instance locks: the two branches of a fork
finish together, and the second one's report waits for the first one's commit (`README.md`, "The
database sets the ceiling"). This chapter specifies **partition ownership**: every write to an
instance is applied by the one server node that owns the instance's partition, in order, with many
writes committed per transaction. The database stays the source of truth for execution, for the
portal and for search; what changes is that writers to one instance no longer contend for its row
lock, because there is only one.

## 1. Model

| Term | Meaning |
|---|---|
| **partition** | A fixed slice of one instance shard's instances, numbered `0 .. P-1`. Every instance minted while partitioning is on belongs to exactly one. |
| **owner** | The one server node allowed to apply writes to a partition's instances. |
| **epoch** | A counter on a partition's ownership row, advanced on every change of owner. |
| **fence** | The check, at the start of every owner transaction, that the owner's epoch is still the partition's epoch. |
| **command** | One write request routed to an owner: start, report, fail, signal, cancel, or a sweep item. |
| **mailbox** | The owner's in-memory queue of commands for one partition. |
| **batch** | The commands an owner drains from one mailbox and applies in one transaction. |
| **legacy instance** | An instance whose id carries no partition: minted before partitioning was on. |

**WGL-PART-001** (MUST) With partitioning off (`WIGGLE_PARTITIONS=0`, the default) the server MUST
behave exactly as it does today: no partition segment is minted, no ownership rows are read, and
every write takes the instance lock ([WGL-STOR-003](80-storage.md)).

**WGL-PART-002** (MUST) With partitioning on, every write to a partitioned instance — the instance
row, its tokens, its comp-log, its events — MUST be applied by that partition's owner, inside a
fenced owner transaction ([§3](#3-ownership-and-fencing)). No other path may write those rows,
except the token-row writes listed in [WGL-PART-042](#5-routing).

**WGL-PART-003** (MUST) The database MUST remain the source of truth. An owner MUST NOT answer a
command before the transaction that applied it has committed, and MUST NOT keep any state between
batches that a new owner would need to read from anywhere but the database.

**WGL-PART-004** (MUST) Partitioning MUST NOT change the engine semantics of
[chapter 30](30-engine.md): a workflow run under an owner produces the same tokens, transitions,
events and outcomes as the same run under the instance lock.

**WGL-PART-005** (MUST) This chapter amends [WGL-SHARD-005](85-sharding.md#1-model) for partitioned
instances: any node still accepts any request, but only the owner applies it. It amends
[WGL-OPS-026](90-ops.md) only if the spike shows that nodes then raise throughput.

## 2. Partitioned ids

**WGL-PART-010** (MUST) A partitioned id MUST be `{prefix}.s{shard}.p{partition}.{ulid}`: the
shard segment of [WGL-SHARD-010](85-sharding.md#21-format), then the partition in decimal without
padding. An id without a `p` segment is a legacy id.

**WGL-PART-011** (MUST) A token's partition MUST equal its instance's, and a child instance MUST be
minted in its parent's partition, as [WGL-SHARD-013](85-sharding.md#21-format) and
[WGL-SHARD-014](85-sharding.md#21-format) already require for the shard. A parent and its children
therefore always share an owner, so completing a child may still resume its parent in the same
transaction.

**WGL-PART-012** (MUST) A root instance started on a node MUST be minted into a partition that node
owns on the chosen shard, so a start is never forwarded. A node that owns no partition on the chosen
shard MUST forward the start to an owner of one.

**WGL-PART-013** (MUST) The partition in an id MUST be read back verbatim on every route. Routing
MUST NOT consult the database to find an id's partition.

**WGL-PART-014** (MUST) `P` MUST be fixed per shard for the shard's lifetime and recorded on the
home shard. A node configured with a different `P` for a shard MUST refuse to start. Changing `P` is
out of scope ([§10](#10-out-of-scope)).

**WGL-PART-015** (MUST) Client libraries that read ids MUST accept the `p` segment and MUST NOT
interpret it. The shared fixtures (`conformance/shard-ids-v1.json`) MUST gain cases for it.

**WGL-PART-016** (MUST) Legacy instances MUST keep the locked path: any node applies their writes
under the instance lock, as today, until they finish or are purged.

## 3. Ownership and fencing

The ownership table lives on each instance shard, beside the instances it governs:

```sql
CREATE TABLE wf_partition_owner (
  partition   INT          NOT NULL PRIMARY KEY,
  owner_node  VARCHAR(128),
  epoch       BIGINT       NOT NULL,
  lease_until BIGINT       NOT NULL
);
```

**WGL-PART-020** (MUST) The leader ([chapter 90 §2](90-ops.md)) MUST assign every partition of
every instance shard to a live node from `wf_node`, by rendezvous hashing over the live node ids, so
a node joining or leaving moves only its own share.

**WGL-PART-021** (MUST) Assigning a partition MUST set `owner_node`, advance `epoch` by one, and set
`lease_until`, in one statement. The owner MUST renew `lease_until` before it lapses
(`WIGGLE_PARTITION_LEASE_MILLIS`, default 10,000; renewed every third of it). A partition whose
lease has lapsed MUST be reassigned by the leader.

**WGL-PART-022** (MUST) Every owner transaction MUST begin with the fence:
`SELECT epoch FROM wf_partition_owner WHERE partition = ? FOR SHARE`, and MUST roll back without
writing when the epoch read differs from the one the owner holds.

**WGL-PART-023** (MUST) A reassignment's `UPDATE` MUST conflict with the fence's share lock, so it
waits for any owner transaction in flight and every later transaction of the old owner fails its
fence. Two owners MUST NOT both commit writes for one partition under different epochs.

**WGL-PART-024** (MUST) An owner whose fence fails, or whose renewal fails, MUST stop taking batches
for that partition at once and answer every command in its mailbox with a retryable error carrying
the partition, so the caller retries through the router.

**WGL-PART-025** (SHOULD) The fence SHOULD be the only statement partitioning adds to a batch, so its
cost falls per batch, not per step.

## 4. The owner

**WGL-PART-030** (MUST) An owner MUST keep one mailbox per owned partition and apply at most one
batch per partition at a time. Batches of different partitions MAY run concurrently.

**WGL-PART-031** (MUST) A batch MUST be: drain up to `WIGGLE_PARTITION_BATCH_MAX` commands (default
64), waiting at most `WIGGLE_PARTITION_LINGER_MICROS` (default 0) for more; open one transaction on
the partition's shard; fence; apply each command in arrival order through a write-buffering view
(`BufferedTx`); flush; commit; then answer every command of the batch.

**WGL-PART-032** (MUST) Commands in one batch MUST each see the writes of the commands before them,
including two commands for the same instance. The batch MUST NOT refuse a command for sharing an
instance, a mode or a parent with another, as the lock-based cross-instance batch does today
(`LocalAsyncBatch`).

**WGL-PART-033** (MUST) A command the engine rejects (not found, lease mismatch, conflict) MUST get
its answer without rolling the batch back. A batch that throws for any other reason MUST roll back
and be replayed one command per transaction, each fenced, so the broken command fails alone.

**WGL-PART-034** (MUST) The engine's write entry points — report, fail, start, signal, cancel, and
the sweep actions of [§6](#6-sweeps) — MUST each be callable against a transaction the caller
already holds, so an owner applies a batch without opening a nested transaction.

**WGL-PART-035** (MAY) Inside a fenced owner transaction, `Tx.lockInstance` MAY read the instance
without a row lock ([§8](#8-the-spike), variant C2). It MUST keep the row lock on the legacy path.

## 5. Routing

**WGL-PART-040** (MUST) A node that receives a command for a partitioned id MUST read the partition
from the id and either enqueue the command on its own mailbox, when it owns the partition, or
forward it to the owner over gRPC.

**WGL-PART-041** (MUST) A forwarded call MUST carry `wiggle-forwarded: <epoch>` metadata. A node
that receives a forwarded call for a partition it does not own at that epoch MUST answer with the
retryable error of [WGL-PART-024](#3-ownership-and-fencing) and MUST NOT forward it again.

**WGL-PART-042** (MUST) These writes stay unrouted, protected by the token row lock they already
take:

| Call | Why it stays |
|---|---|
| `PollTasks` claim | `FOR UPDATE SKIP LOCKED` on `READY` token rows ([chapter 80](80-storage.md)) |
| `ExtendLease` / heartbeat | writes one `RUNNING` token row's lease |

Every other write of a partitioned instance — `ReportSteps`, `FailTask`, `SignalInstance`,
`CancelInstance`, the portal's cancel and retry, event triggers — MUST be routed.

**WGL-PART-043** (MUST) Each `wf_node` row MUST carry the address other nodes forward to.

## 6. Sweeps

**WGL-PART-050** (MUST) For partitioned instances, the owner MUST run the clock-driven sweeps of its
own partitions — due timers, due retries, due signal deadlines, expired leases — as commands on the
partition's mailbox. The leader MUST run them only for legacy instances.

**WGL-PART-051** (MUST) `wf_token` MUST carry the partition, indexed with the columns each sweep
scans, so an owner's sweep reads only its own partitions.

**WGL-PART-052** (MUST) Schedules, event retention, search indexing and instance retention MUST stay
with the leader: they start new instances or delete finished ones, and neither needs an owner.

## 7. Configuration

| Variable | Default | Meaning |
|---|---|---|
| `WIGGLE_PARTITIONS` | `0` | Partitions per instance shard. `0` turns partitioning off. |
| `WIGGLE_PARTITION_LEASE_MILLIS` | `10000` | Ownership lease. |
| `WIGGLE_PARTITION_BATCH_MAX` | `64` | Commands per batch. |
| `WIGGLE_PARTITION_LINGER_MICROS` | `0` | How long a batch waits for more commands. |
| `WIGGLE_PARTITION_VARIANT` | `c2` | Spike only: `c0`, `c1` or `c2` ([§8](#8-the-spike)). |

## 8. The spike

*Non-normative.* What the spike builds and measures, and in what order.

### 8.1 Variants

| Variant | Behaviour | Isolates |
|---|---|---|
| **C0** | Routing and mailboxes; one command per transaction; `lockInstance` keeps `FOR UPDATE`. | Removing lock waits. |
| **C1** | C0 with batches of many commands ([WGL-PART-031](#4-the-owner)). | Fewer commits. |
| **C2** | C1 with `lockInstance` as a plain read under the fence ([WGL-PART-035](#4-the-owner)). | Fewer statements per step. |

### 8.2 Steps

1. **Correctness.** The full test suite and the `conformance` suite with `WIGGLE_PARTITIONS=16` on
   Postgres (testcontainers), plus new tests: a fork whose branches report together; a sub-workflow
   tree; a reassignment in the middle of a batch, where the old owner is fenced and no step is lost or
   applied twice; a mix of legacy and partitioned instances.
2. **Statement counts.** `StepStatementsTest` gains C1 and C2, reporting statements **per step** at
   batch sizes 1, 8 and 64, against today's report 5 / start 3 / claim 1.
3. **Local smoke.** 1–3 nodes, linear and fork/combine workflows, to catch regressions only; the
   serving build is checked on every run.
4. **GCP ceiling.** `ceiling.sh` on 16 vCPU Cloud SQL, with 1, 2 and 3 server nodes, for each of
   C0, C1 and C2. Recorded: steps/s; p99 latency; database CPU; lock-wait time
   (`wait_event_type = 'Lock'`); commits/s; batch size distribution; share of commands forwarded
   and the forward's latency.
5. **Chaos.** Kill an owner at 70% of the ceiling; record the pause until its partitions run again,
   and audit for lost or duplicated steps.

### 8.3 Order of work

1. Partitioned ids and the migration (`wf_partition_owner`, `wf_token.partition`,
   `wf_node.address`, `P` on the home shard).
2. Ownership assignment, leases and the fence.
3. Engine write entry points that take an open `Tx`.
4. Mailboxes and batches.
5. Routing in `GrpcApi`.
6. Sweeps per partition.
7. The variant switch.

## 9. Go / no-go

**Go**, and this chapter moves towards default-on, when all of these hold on 16 vCPU Cloud SQL:

- at least 1.5× the current ceiling (14,400 steps/s or more), or the database CPU-bound at clearly
  fewer statements per step;
- p99 latency at moderate load no worse than today's flat ≈260 ms;
- a killed owner's partitions resume within 2× `WIGGLE_PARTITION_LEASE_MILLIS`;
- the conformance suite and the chaos audit pass.

**No-go** when C0 shows lock waits were not the limit *and* C1 and C2 do not move the ceiling: the
database's commit throughput is then the ceiling, and the alternatives in [§11](#11-alternatives-considered)
are reconsidered.

## 10. Out of scope

- Databases other than Postgres, for the spike.
- Changing `P` on a live shard.
- Client-side routing: workers connecting straight to an owner.
- Handing a continuation to a waiting worker in memory. An owner knows the next token as soon as it
  mints it, which makes layer 2 of `docs/archive/in-memory-dispatch.md` the natural next step.
- More than one shard, beyond each shard keeping its own partitions and ownership table.

## 11. Alternatives considered

| Alternative | Removes the lock | Replicated state | Engine change | New infrastructure |
|---|---|---|---|---|
| Kafka as transport only, database unchanged | no | database | none | Kafka |
| Kafka Streams, state in RocksDB, database as a projection | yes | changelog topic | small (`Tx` over a state store) | Kafka |
| Aeron transport, state in local RocksDB | yes | **none** | small | Aeron |
| Aeron Cluster, Raft-replicated state machine | yes | Raft log | **large**: clock, ids and timers must come from the log | Aeron Cluster per partition group |
| **Partition ownership (this chapter)** | yes (waits) | database | small to medium | none |

Partition ownership is spiked first because it needs no new infrastructure and tests directly
whether lock contention is the bottleneck. A log-based design (Kafka Streams, or a Zeebe-style
replicated log) is the next step only if the database's commit throughput turns out to be the
ceiling. Zeebe uses that pattern — single writers per partition, RocksDB, an exported read model —
on its own Raft-replicated log, not on Kafka.
