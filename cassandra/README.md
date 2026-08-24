# wiggle-cassandra

A Cassandra-backed `Storage` for the Wiggle workflow engine. Unlike the SQL modules
(`wiggle-postgres`, `wiggle-mysql`, `wiggle-oracle`) this is **not** a JDBC dialect: Cassandra has
no cross-partition transactions and no `SELECT ... FOR UPDATE`, so it implements the `Storage` SPI
directly on the DataStax CQL driver and uses partition-local lightweight transactions (Paxos) for
the same guarantees.

## Configuration

Point `WIGGLE_JDBC_URL` at a `cassandra://` URL; the engine is detected from the scheme.

```
WIGGLE_JDBC_URL=cassandra://host1:9042,host2:9042/wiggle?dc=<localDatacenter>&rf=<replicationFactor>
WIGGLE_JDBC_USER=<cql-user>        # optional
WIGGLE_JDBC_PASSWORD=<cql-pass>    # optional
```

`dc` defaults to `datacenter1`, `rf` to `1`. The keyspace is created (`SimpleStrategy`) if absent;
for production use `NetworkTopologyStrategy` and a real replication factor.

## How it maps a transactional engine onto Cassandra

The engine serialises all mutations of an instance behind `Tx.lockInstance` and expects each `inTx`
unit to be atomic. Cassandra provides neither directly, so:

- **Instance = one partition.** An instance row and all of its token rows live in a single
  `instance_state` partition (`PRIMARY KEY ((instance_id), row_kind, row_id)`). Reading an instance
  or its tokens, and advancing it, all touch exactly one partition.
- **Optimistic serialisation.** A transaction's writes are **buffered** and flushed at commit as a
  single-partition `LOGGED` batch whose instance-row update is an LWT conditioned on the instance
  `revision` (`IF revision = ?`). A losing writer's batch does not apply and `inTx` retries the whole
  unit — the guarantee a row lock gives the SQL backends, without blocking.
- **Exactly-once claim.** Dispatch is discovered through a `dispatch` index partitioned by `queue`;
  the authoritative hand-out is always a per-token LWT (`IF status = 'READY'`), so a stale index hint
  can never cause a token to run twice. Timers, leases and signal waits work the same way through
  `timer` / `lease` / `signal_wait`, sharded to bound partition size; the leader's sweeps re-validate
  and self-clean stale hints.

Every hot-path query targets a single partition by its full partition key — there is **no
`ALLOW FILTERING`** anywhere in the store.

## Known limits (by design, not silent)

- **Sub-workflows** advance a *parent* instance inside the *child's* completing transaction — two
  partitions. Cassandra has no multi-partition atomic transaction, so that step is flushed as two
  batches (the revision-CAS ones first, so a conflict aborts before any new-instance insert). It is
  atomic only when uncontended; a parent being modified concurrently (e.g. cancelled) at the exact
  moment its child completes is the one window where the two writes are not atomic.
- **Very wide fan-outs** (`forkEach` over a huge list) concentrate many token rows in one instance
  partition. Keep per-instance token counts within Cassandra's partition guidance.
- **`listInstances` / `countInstances`** read one bounded index partition (`instance_index`,
  `bucket = 0`), kept small by retention/purge. Bucket it by time for very high instance volumes.
- **`countProcessedSince`** (a lag-monitor throughput signal) is a best-effort per-minute counter;
  `queueDepth` may include not-yet-reaped stale dispatch hints. Both are monitoring aids, not
  correctness inputs.

## Verified

Against Cassandra 5.0 (`cassandra:5.0`): end-to-end linear, fork/join, sleep-timer and sub-workflow
runs (real server + worker), and a concurrent 40-token / 5-worker exactly-once claim. See
`tests/src/test/java/dev/wiggle/cassandra/CassandraStoreTest.java` (opt-in via
`WIGGLE_TEST_CASSANDRA_URL`).
