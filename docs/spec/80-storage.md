# 80 — Storage

← [Control-plane API](70-api.md) · [Index](00-index.md) · Next: [Operations](90-ops.md)

The whole durable state is a handful of `wf_*` tables. The engine reaches them through one narrow SPI
whose only atomicity requirement is "a transaction is a unit, and an instance can be locked". This
chapter specifies the SPI, the backends, the schema and its migration discipline, and the claim
mechanics each dialect uses.

## 1. The storage SPI

**WGL-STOR-001** (MUST) The engine MUST reach storage only through `Storage`: `migrate()`,
`inTx(work)` and an optional `fingerprint()`.

**WGL-STOR-002** (MUST) Every engine mutation MUST run inside `inTx`, and an implementation MUST make
that unit atomic.

**WGL-STOR-003** (MUST) An implementation MUST honour `Tx.lockInstance` as a mutual-exclusion point for
one workflow instance, held for the rest of the transaction.

**WGL-STOR-004** *Withdrawn: the cell coordinator was removed ([chapter 85 §15](85-sharding.md#15-dropping-the-coordinator)).*

**WGL-STOR-005** (MUST) `fingerprint()` MUST be a stable identity of the underlying storage, so two
cells that reuse a cell id are still distinguishable.

**WGL-STOR-006** (MUST) A server node MUST obtain its store from an injected `StorageFactory` — a
functional interface — and MUST NOT discover it by `ServiceLoader`.

**WGL-STOR-007** (MUST) `PostgresStorageFactory` MUST map the JDBC URL scheme to a backend:
`jdbc:postgresql:` → the PostgreSQL dialect, `jdbc:h2:` → the H2 dialect, no URL → the in-memory
store. Any other URL MUST fail with a message naming what is supported.

*Verified by:* `server/store/StorageContract` (run against every backend), `tests/StorageFactoryTest`, `tests/DialectTest`.

## 2. Backends

**WGL-STOR-010** (MUST) **In-memory** MUST be the backend when no JDBC URL is configured: single node,
for development and tests. It MUST implement the same `Tx` contract, including instance locking.

**WGL-STOR-011** (MUST) **PostgreSQL** MUST be the supported deployment backend.

**WGL-STOR-012** (MUST) **H2** MUST run in PostgreSQL-compatibility mode, take the same schema, and MUST
NOT be a deployment target: it cannot run the `FOR UPDATE SKIP LOCKED` claim a real cluster depends on.

**WGL-STOR-013** (MUST) One image / one artifact MUST bundle every backend, so the URL scheme selects at
runtime and no per-database build exists.

**WGL-STOR-014** (MUST) JDBC connections MUST come from a HikariCP pool sized by
`WIGGLE_JDBC_POOL_SIZE`.

*Verified by:* `server/store/StorageContract`, one subclass per backend (`InMemoryStorageContractTest`, `JdbcStorageContractTest` -- H2 by default, whatever `WIGGLE_TEST_DB_URL` names when one is set).

## 3. Dialect seam

**WGL-STOR-020** (MUST) A dialect MUST declare: its id, whether `FOR UPDATE SKIP LOCKED` can drive the
claim, whether `UPDATE … RETURNING` is available, how to make an insert ignore a primary-key collision,
how to recognise a duplicate-key exception, the `wf_schedule` upsert, and how to acquire the migration
lock.

**WGL-STOR-021** (MUST) On PostgreSQL the claim MUST be a single statement using `SKIP LOCKED` (with
`RETURNING` where available), so concurrent pollers never block each other or claim the same token.

**WGL-STOR-022** (MUST) On a dialect without `SKIP LOCKED` the claim MUST fall back to compare-and-set
per candidate token, preserving exactly-once dispatch at lower throughput.

**WGL-STOR-023** (MUST) A duplicate-key insert MUST be an idempotent no-op where the SPI says so
(definition re-registration), never an error surfaced to the caller.

**WGL-STOR-024** (MUST) A dialect MUST also recognise a *momentary* failure (`isTransient`): one where
the transaction is gone and the same work would likely succeed on a fresh connection — connection loss,
a pool timeout, a deadlock victim, a serialization failure, a server restarting. It MUST read the
driver's whole `getNextException` chain, and MUST recognise the failure both from the typed JDBC
exceptions (`SQLTransientException`, `SQLRecoverableException`) and from the SQL state, since a driver
may report only one of the two (pgjdbc raises a plain `PSQLException` for everything).

**WGL-STOR-025** (MUST NOT) `isTransient` MUST NOT claim a failure the caller could mistake for "nothing
was applied" when work may in fact have landed. A failed commit is never its answer to give — what the
store does with the answer is [section 9](#9-failure-classification-and-replay).

*Verified by:* `server/store/StorageContract` (every statement a dialect spells, run), `postgres/PostgresClaimTest`, `tests/DialectTest`, `tests/JdbcGraphTest`.

## 4. Schema

**WGL-STOR-030** (MUST) The tables MUST be exactly:

| Table | Holds |
|---|---|
| `wf_definition` | the submitted graph JSON per `(name, version)`, plus its fingerprint and algorithm |
| `wf_graph_node` | one row per node of a registered graph: kind, name, activity, queue, retry JSON, sleep millis, expected, success, reason, start flag, items/item keys, arm names, collect key |
| `wf_graph_edge` | one row per edge: from, to, condition, ordinal |
| `wf_instance` | id, workflow, version, correlation id, status, termination reason, error, context, timestamps, revision |
| `wf_token` | id, instance, workflow, version, node, kind, status, activity, queue, attempt, `available_at`, lease owner and expiry, join stack, payload, last error, timestamps, comp seq, `started_at`/`finished_at` |
| `wf_comp_log` | one row per completed compensable step: seq, node, activity, queue, both snapshots, compensated flag |
| `wf_schedule` | one row per workflow: interval or cron, context, next fire time |
| `wf_event` | the event log, keyed by a store-generated `seq`, with payload envelope version and node id |
| `wf_event_cursor` | one row per consumer: acknowledged seq, last poll, creation |
| `wf_node` | cluster membership: node id, name, first/last heartbeat, worker count, leader flag |
| `wf_auth_user` · `wf_auth_role` · `wf_auth_user_role` | portal accounts (name, PBKDF2 hash, salt, rounds, disabled), roles as permission sets, and grants; on the auth shard ([WGL-SHARD-181](85-sharding.md#13-users-and-authorization)) |
| `wf_auth_session` · `wf_auth_audit` | sessions by token hash with their account and expiry, and every change to accounts, roles and sessions by a store-generated `seq` |
| `wf_auth_credential` | machine credentials (API key hash or certificate subject) bound to a role |
| `wf_search_doc` | one search document per instance on a search shard: identity, status, text, times, and on PostgreSQL a generated `tsvector` with a GIN index |
| `wf_schema_version` | applied migrations: version, name, time, source checksum |

**WGL-STOR-031** (MUST) A graph MUST be stored **twice**: the raw submitted blob (write-once, the source
of truth for audit and replay) and normalised node/edge rows the engine reads one node at a time.

**WGL-STOR-032** (MUST) The hot path MUST NOT materialise a whole graph. A definition small enough
(currently ≤ 40 nodes) MAY be held whole in memory; anything larger MUST be read node by node.

**WGL-STOR-033** (MUST) Which of the two a graph handle is MUST be decided once per handle, not once per
node, so one drive pass reads a consistent source.

**WGL-STOR-034** (MUST) Instance id columns MUST be `VARCHAR(128)`, and an id formatter MUST refuse
anything longer.

**WGL-STOR-035** (MUST) `wf_token.join_stack` MUST be unbounded (`TEXT`): its length is proportional to
nesting depth, and a bounded column caps nesting (`VARCHAR(1000)` capped it at roughly 38 levels).

**WGL-STOR-036** (MUST) `wf_instance.status` MUST be wide enough for every status name, including
`COMPENSATION_FAILED` (19 characters).

**WGL-STOR-037** (MUST) Indexes MUST cover the engine's access paths: token dispatch
`(status, queue, available_at)`, token by instance, lease expiry `(status, lease_expires)`, throughput
`(kind, status, updated_at)`, the join barrier `(instance_id, node_id, status)`, the duration sample
`(workflow, version, status, finished_at)`, instance status and correlation, event by instance and by
creation time. (Migration 26 cancelled any still-running observed run and dropped `settle_at`,
`wf_token.seq` and `wf_anomaly` when OBSERVED execution was removed.)

*Verified by:* `postgres/SchemaMigrationTest`, `tests/JdbcMigrationTest`,
`server/store/InstanceStatusWidthTest`, `client/flow/DeepNestingTest`.

## 5. Migrations

**WGL-STOR-040** (MUST) Migrations MUST be a versioned, forward-only list, each a
`Migration(version, name, sql)`, tracked in `wf_schema_version`.

**WGL-STOR-041** (MUST) They MUST run under a cross-node lock acquired by the dialect, and MUST be
atomic on PostgreSQL.

**WGL-STOR-042** (MUST) A released migration MUST NOT be edited. Each applied migration records a
SHA-256 checksum of its source, and a mismatch on any later startup MUST fail loudly as **drift**,
naming the version and telling the operator to add a new migration instead.

**WGL-STOR-043** (MUST) A legacy row with no recorded checksum MUST be backfilled in apply mode and left
alone in verify mode.

**WGL-STOR-044** (MUST) A schema change SHOULD be backward-compatible so a rolling deploy can run both
versions of the code (additive, nullable columns).

**WGL-STOR-045** (MUST) `WIGGLE_SCHEMA_MODE=verify` MUST apply nothing and MUST fail fast when any
migration is pending, naming how many and how to apply them.

**WGL-STOR-046** (MUST) `WIGGLE_MIGRATE_ONLY=true` MUST apply pending migrations and exit, forcing apply
even when the environment otherwise pins verify — so it can run as an init container or a CI step.

**WGL-STOR-047** (MUST) A database initialised under one baseline MUST NOT be migrated as another: a
baseline-name mismatch MUST fail with a message saying each schema needs its own database.

*Verified by:* `postgres/SchemaMigrationTest`, `tests/JdbcMigrationTest`.

## 6. Token payloads

**WGL-STOR-050** (MUST) A token payload MUST be JSON in exactly one place (the payload codec); the engine
above it MUST never see the text.

**WGL-STOR-051** (MUST) The payload format MUST be one object carrying the scope stack under
`__scopes__`, the loop counts under `__loops__`, and the staged combine inputs as ordinary keys beside
them.

**WGL-STOR-052** (MUST) A null or blank payload MUST decode to the empty payload — what a token outside
every scope carries.

**WGL-STOR-053** (MUST) Token payloads MUST NOT be versioned reference data: they are live runtime state,
so an in-flight instance keeps decoding across an upgrade.

*Verified by:* `server/store/PayloadCodecTest`, `server/store/RowsTest`.

## 7. Retention

**WGL-STOR-060** (MUST) The leader MUST purge **terminal** instances older than `WIGGLE_RETENTION_MILLIS`
(default 24 h), in bounded batches, together with their tokens.

**WGL-STOR-061** (MUST) "Terminal" MUST be the same test everywhere it is encoded (the view type, the
JDBC purge query, the in-memory purge).

**WGL-STOR-062** (MUST) The same sweep MUST trim the event log per [WGL-EVT-030](60-event-log.md).

**WGL-STOR-063** (SHOULD) Retention and purge cadence SHOULD be treated as part of capacity planning:
accumulated history measurably raises latency at the throughput ceiling.

*Verified by:* `server/cluster/HousekeeperTest`, `tests/EventStoreTest`.

## 8. Reads the engine needs

**WGL-STOR-070** (MUST) `Tx` MUST provide, beyond the CRUD on instances and tokens: batched insert /
find / update of tokens; instances by correlation id (newest first); due timers; signal waits (oldest
first) and those whose deadline passed; sub-workflow children of a parent token's instance; the
schedule for a workflow and the schedules that are due; expired leases; a backlog snapshot for lag monitoring; the join-stack list at a barrier; the compensation
log in seq order; cancelling every active token of an instance; and the event
log's append, read, head, cursor register / advance / minimum.

**WGL-STOR-071** (MUST) A batched variant MUST be semantically identical to looping the single-row form;
a JDBC backend overrides it only for efficiency.

**WGL-STOR-072** (MUST) The barrier count query MUST be answerable without loading the whole instance's
tokens where the dialect allows it.

*Verified by:* `server/store/StorageContract` -- each read, and each batched variant against the single-row form it has to match.

## 9. Failure classification and replay

**WGL-STOR-080** (MUST) Every storage failure MUST reach the engine as a `StorageException` carrying one
of three classifications, which state what the caller may assume about the attempt's durable effect:

| classification | the rows | may the call be repeated |
|---|---|---|
| `TRANSIENT` | nothing was applied, and nothing can have been | yes, idempotent or not |
| `AMBIGUOUS` | the commit's outcome is unknown | no, unless the caller knows the work is idempotent |
| `PERMANENT` | nothing was applied and a repeat fails identically | pointless |

**WGL-STOR-081** (MUST) A backend that cannot tell MUST answer `PERMANENT`: it costs a recoverable
failure its retry, where guessing `TRANSIENT` would re-apply work that may already be durable.

**WGL-STOR-082** (MUST) A failed commit MUST be `AMBIGUOUS`. A rollback after one proves nothing: the
commit may have reached the database and only its acknowledgement been lost.

**WGL-STOR-083** (MUST) A JDBC backend MUST replay a transaction that rolled back on a `TRANSIENT`
failure, bounded by `wiggle.jdbc.txAttempts` / `WIGGLE_JDBC_TX_ATTEMPTS` (default 3; 1 disables) and
paced by `wiggle.jdbc.txRetryDelayMillis` / `WIGGLE_JDBC_TX_RETRY_DELAY_MILLIS` (default 50, multiplied
by the attempt). It MUST NOT replay anything else, and each replay MUST run on a freshly borrowed
connection. An interrupt MUST abandon the replay and surface the original failure, with the thread's
interrupt flag restored.

**WGL-STOR-084** (MUST) A transaction body MUST therefore be re-runnable against a fresh `Tx`: it may
read, write and throw, but MUST NOT depend on in-process state it mutated on a previous attempt. Engine
bodies satisfy this by re-reading the rows they work from.

**WGL-STOR-085** (MUST) Exhausted replays MUST surface the `TRANSIENT` classification unchanged, so the
layer above can still tell "nothing was applied" from "unknown" — see
[WGL-API-007](70-api.md) for what the API does with each.

*Verified by:* `tests/TransientFailureTest`, `tests/DialectTest`,
`postgres/PostgresDeadlockRetryTest` (a real `40P01` from a live PostgreSQL),
`tests/StorageFailureStatusTest`.
