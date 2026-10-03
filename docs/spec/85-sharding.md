# 85 — Sharding (proposed)

← [Storage](80-storage.md) · [Index](00-index.md) · Next: [Operations](90-ops.md)

**Status: proposed.** Nothing in this chapter is implemented. Every requirement here is *unverified*
([WGL-GEN-001](00-index.md)) until the change that builds it names its test. Where this chapter and
the rest of the suite disagree, the rest of the suite describes the code as it is today.

One database is the ceiling. On GCP Cloud SQL a 16 vCPU primary sustains about 7,200 steps/s
(900 instances/s) at ~75% database CPU while the server sits at 25–40%
([chapter 90 §8](90-ops.md)); adding server nodes does not move it ([WGL-OPS-026](90-ops.md)).
This chapter specifies how one Wiggle cluster spreads its instances over several databases
(**shards**), how each shard serves read-heavy queries from its own **read replicas**, how shards are
added and retired without moving data, and how the optional cell coordinator is removed in favour of
it. It also specifies the data that does not shard by instance: the portal moves into the server,
users and roles move into a database on a dedicated **auth shard**, and vector search gets its own
**search shards**.

## 1. Model

| Term | Meaning |
|---|---|
| **shard** | One primary database, plus zero or more read replicas of it. Identified by a permanent non-negative integer, and carrying one or more roles. |
| **role** | What a shard holds: `instances` (workflow runtime state, minted onto by weight), `home` (cluster-global rows), `auth` (users, roles, sessions, credentials), `search` (derived search documents and vector indexes). |
| **home shard** | The one shard with the `home` role: the node table, schedules, event cursors and the shard registry. |
| **auth shard** | The one shard with the `auth` role. Defaults to the home shard. |
| **search shard** | A shard with the `search` role. There may be none, one, or several. |
| **topology** | The configured list of shards, their state, their weight and their connections. |
| **generation** | One version of the topology's placement (weights and states), effective from an `activeFrom` instant. |
| **replica** | A read-only streaming replica of a shard's primary, used only for reads that tolerate bounded staleness. |

**WGL-SHARD-001** (MUST) Every row that belongs to an instance — the instance, its tokens, its
comp-log, its anomalies, its events — MUST live on that instance's shard. No engine transaction may
span two shards.

**WGL-SHARD-002** (MUST) An instance's shard MUST be decided once, when its id is minted, and MUST be
readable from the id alone. Routing MUST NOT consult a directory, a ring or the database.

**WGL-SHARD-003** (MUST) Shard ids MUST be permanent: never renumbered, never reused, never removed
from the topology while the registry ([§3.3](#33-the-shard-registry)) still lists them as not
`RETIRED`.

**WGL-SHARD-004** (MUST) A deployment with one shard and no replicas MUST behave exactly as Wiggle
does today, and MUST be configurable with the existing `WIGGLE_JDBC_*` variables alone.

**WGL-SHARD-005** (MUST) Clustering MUST remain nothing but shared databases: every server node
connects to every shard, any node may serve any instance, and nodes still multiply availability and
API capacity. Shards multiply database throughput.

**WGL-SHARD-006** (MUST) Exactly one shard MUST carry `home` and exactly one MUST carry `auth`; they
MAY be the same shard. Any number of shards MAY carry `instances` or `search`. A shard MAY carry
several roles, so a one-database deployment is one shard with all four.

**WGL-SHARD-007** (MUST) Only an `instances` shard may take a mint weight. Each role's schema MUST be
migrated only onto the shards that carry the role: `wf_*` runtime tables on `instances`, the global
tables on `home`, `wf_auth_*` on `auth`, `wf_search_*` (and the `vector` extension) on `search`.

**WGL-SHARD-008** (MUST) The engine's hot path (start, claim, report, fail, heartbeat, signal, the
leader's sweeps) MUST NOT read or write the auth or search shards. Either can be down while
workflows keep running.

## 2. Ids

**Status: implemented**, except shard choice by weight ([WGL-SHARD-020](#22-choosing-a-shard)):
until the topology lands, every root instance is minted on shard `0`. `ShardIds` (in `core`) is the
codec; `InstanceIds` mints instance ids. Verified by `core/ShardIdsConformanceTest` (the shared
fixture) and `server/engine/ShardIdsEngineTest`.

### 2.1 Format

**WGL-SHARD-010** (MUST) A sharded id MUST be `{prefix}.s{shard}.{ulid}`:

| Id | Example | Shard |
|---|---|---|
| instance | `wfi.s3.01k6…` | minted ([§2.2](#22-choosing-a-shard)) |
| token | `tok.s3.01k6…` | its instance's |
| child instance (sub-workflow) | `wfi.s3.01k6…` | its parent instance's |
| observed run | `wfo.s3.<digest>` | derived from the key ([§9](#9-observed-runs)) |
| anomaly | `anm.s3.…` | its instance's |

Comp-log entries and events are keyed by `(instance id, seq)` and need no id of their own. Schedule and
node ids are global rows on the home shard and stay bare (`sched_…`, `node_…`).

**WGL-SHARD-011** (MUST) The shard segment MUST sit before the ulid, because a ulid is the only
segment that may grow.

**WGL-SHARD-012** (MUST) A formatted id MUST fit the schema's 128-character id columns.

**WGL-SHARD-013** (MUST) A token's shard MUST equal its instance's. The report, fail and heartbeat
paths receive only a task (token) id, and MUST route on it without reading anything first.

**WGL-SHARD-014** (MUST) A child instance MUST be minted on its parent's shard, so the parent/child
join ([chapter 30](30-engine.md)) stays inside one transaction on one database.

**WGL-SHARD-015** (MUST) An id derived from another (a token from its instance, a child from the token
that starts it, an anomaly from its instance) MUST inherit the owner's **form**: the owner's shard when
its id carries one, and the bare form (`{prefix}_…`) when it carries none. A bare id belongs to the
home shard, so the derived id lands with its owner without the minter knowing which shard is home.

### 2.2 Choosing a shard

**WGL-SHARD-020** (MUST) A root instance's shard MUST be chosen at mint time among the `ACTIVE`
shards of the current generation, with probability proportional to each shard's `weight`.

**WGL-SHARD-021** (MUST) Once minted, the shard in an id MUST be read back verbatim on every route
and MUST NOT be recomputed from the topology.

### 2.3 Ids from before sharding

**WGL-SHARD-030** (MUST) A legacy bare id (`wfi_…`, `tok_…`, `wfo_…`) MUST route to the home
shard. Upgrading a single-database deployment therefore rewrites no rows: its database becomes the
home shard.

**WGL-SHARD-031** (MUST) A namespaced id minted under the coordinator
(`{ns}[.c{cell}].e{epoch}.s{shard}.{ulid}`, [WGL-COORD-010](90-ops.md)) carries no shard and MUST route
to the home shard, cell label or not. Its `.s` segment was a ring position, not a database, and the
tokens of such an instance were always minted bare, so no label could route a report anyway. See
[§15](#15-dropping-the-coordinator).

## 3. Topology and configuration

### 3.1 The topology document

**WGL-SHARD-040** (MUST) A sharded deployment MUST be configured by a JSON document named by
`WIGGLE_STORAGE_TOPOLOGY`: a file path, or the document inline when the value starts with `{`.

```json
{
  "defaults": {
    "user": "${WIGGLE_JDBC_USER}",
    "password": "${WIGGLE_JDBC_PASSWORD}",
    "pool": 32,
    "replicaPool": 16,
    "maxReplicaLagMillis": 5000,
    "replicaFallback": "primary"
  },
  "generations": [
    { "id": 1, "activeFrom": "2026-10-01T00:00:00Z", "weights": { "0": 1, "1": 1 } },
    { "id": 2, "activeFrom": "2026-11-02T09:00:00Z", "weights": { "0": 1, "1": 1, "2": 3 } }
  ],
  "shards": [
    {
      "id": 0, "state": "ACTIVE", "roles": ["instances", "home"],
      "primary":  { "url": "jdbc:postgresql://pg-s0:5432/wiggle" },
      "replicas": [
        { "url": "jdbc:postgresql://pg-s0-r1:5432/wiggle" },
        { "url": "jdbc:postgresql://pg-s0-r2:5432/wiggle", "pool": 8 }
      ]
    },
    {
      "id": 1, "state": "ACTIVE", "roles": ["instances"],
      "password": "${WIGGLE_S1_PASSWORD}",
      "primary":  { "url": "jdbc:postgresql://pg-s1:5432/wiggle" },
      "replicas": []
    },
    {
      "id": 2, "state": "ACTIVE", "roles": ["instances"],
      "primary":  { "url": "jdbc:postgresql://pg-s2:5432/wiggle" },
      "replicas": [ { "url": "jdbc:postgresql://pg-s2-r1:5432/wiggle" } ]
    },
    {
      "id": 10, "state": "ACTIVE", "roles": ["auth"],
      "primary":  { "url": "jdbc:postgresql://pg-auth:5432/wiggle" },
      "replicas": [ { "url": "jdbc:postgresql://pg-auth-r1:5432/wiggle" } ]
    },
    {
      "id": 20, "state": "ACTIVE", "roles": ["search"],
      "primary":  { "url": "jdbc:postgresql://pg-search-a:5432/wiggle" },
      "replicas": [ { "url": "jdbc:postgresql://pg-search-a-r1:5432/wiggle" } ]
    }
  ]
}
```

**WGL-SHARD-041** (MUST) Any connection setting (`user`, `password`, `pool`, `replicaPool`,
`maxReplicaLagMillis`, `replicaFallback`) MUST be settable in `defaults`, overridable per shard, and
overridable again per primary or per replica.

**WGL-SHARD-042** (MUST) A `${NAME}` in a string value MUST be replaced by the environment variable
`NAME`, and a reference to an unset variable MUST fail startup. Nothing else is interpolated. This
lets the document be a ConfigMap while credentials stay in Secrets.

**WGL-SHARD-043** (MUST) When `WIGGLE_STORAGE_TOPOLOGY` is unset, the server MUST build a one-shard
topology, carrying all four roles, from `WIGGLE_JDBC_URL`, `WIGGLE_JDBC_USER`, `WIGGLE_JDBC_PASSWORD` and
`WIGGLE_JDBC_POOL_SIZE`, with replicas taken from:

| Variable | Default | Meaning |
|---|---|---|
| `WIGGLE_JDBC_REPLICA_URLS` | *(unset)* | comma-separated replica URLs of the single shard |
| `WIGGLE_JDBC_REPLICA_POOL_SIZE` | `16` | pool per replica |
| `WIGGLE_JDBC_MAX_REPLICA_LAG_MILLIS` | `5000` | a replica further behind is not used |
| `WIGGLE_JDBC_REPLICA_FALLBACK` | `primary` | `primary` or `fail` ([§8](#8-read-replicas)) |

**WGL-SHARD-044** (MUST) Setting both `WIGGLE_STORAGE_TOPOLOGY` and `WIGGLE_JDBC_URL` MUST fail
startup.

### 3.2 Validation at startup

**WGL-SHARD-050** (MUST) A node MUST refuse to start when the topology:

- lists a shard id twice, or does not have exactly one `home` and exactly one `auth` shard;
- has a generation weighting a shard that is not `ACTIVE` or does not carry `instances`, or no
  generation with a positive weight;
- has generations whose `id` or `activeFrom` do not both strictly increase;
- omits a shard the registry lists as not `RETIRED` ([§3.3](#33-the-shard-registry));
- points a shard's primary at a database whose `wf_shard` row names another shard id.

**WGL-SHARD-051** (MUST) `migrate()` MUST migrate every shard's primary and MUST write the shard's id
into that database's `wf_shard` row the first time; a later run MUST only verify it.

**WGL-SHARD-052** (MUST) `Storage.fingerprint()` MUST become a hash over the ordered list of
`(shard id, wf_shard identity)`, so two clusters built on overlapping databases are told apart.

### 3.3 The shard registry

**WGL-SHARD-060** (MUST) The home shard MUST hold `wf_shard_registry(shard_id, state, first_seen,
retired_at)`: every shard id the cluster has ever used.

**WGL-SHARD-061** (MUST) A shard MUST enter the registry when a node first migrates it, and its
state MUST only move forward: `ACTIVE → DRAINING → RETIRED`.

**WGL-SHARD-062** (MUST) A route to a `RETIRED` shard MUST answer *not found* without opening a
connection.

## 4. Storage SPI

**Status: implemented.** Every engine transaction is routed, and `Transactions` (the engine's wrapper)
has no unrouted entry point left. `ShardedStorage` routes over one `Storage` per shard. Verified by:

- `tests/RoutedConformanceTest`: every engine scenario on a store that refuses an unrouted `inTx`;
- `tests/ShardedConformanceTest`: every engine scenario on two in-memory shards, the first instance
  off the home shard;
- `server/store/ShardedStorageTest` and `server/engine/ShardedEngineTest`: routing, unknown shards,
  and each path that only branches with more than one shard (claims, report batches, merged reads,
  sweeps, registration, schedule fires).

Until a later step:

- **Placement is round-robin.** Root instances take the instance shards in turn
  ([WGL-SHARD-020](#22-choosing-a-shard)'s weights come with the topology), and every observed run is
  minted on the first instance shard ([§9](#9-observed-runs)).
- **The event log is home-only.** The feed reads, acknowledges and trims on the home shard, so events
  appended on any other shard are not served until [§10](#10-event-log).
- **A schedule fires on home.** The fire claims the schedule and starts its instance in one home
  transaction, minting that instance on home, until [WGL-SHARD-104](#7-global-data) can mint it
  elsewhere with an idempotent id.

### 4.1 Routed transactions

**WGL-SHARD-070** (MUST) `Storage` MUST gain routed entry points. The existing single-database
implementations satisfy them through the defaults:

```java
default int home() { return 0; }
default List<Integer> instanceShards() { return List.of(home()); }
default int shardOf(String id) { return ShardIds.shardOf(id).orElse(home()); }
default <R> R inShard(int shard, Function<Tx, R> work) { return inTx(work); }
default <R> R inTxFor(String id, Function<Tx, R> work) { return inShard(shardOf(id), work); }
default <R> R inHome(Function<Tx, R> work) { return inShard(home(), work); }
default <R> R readShard(int shard, Freshness f, Function<ReadTx, R> work) { return inShard(shard, work::apply); }
default <R> R readFor(String id, Freshness f, Function<ReadTx, R> work) { return readShard(shardOf(id), f, work); }
```

Fan-out runs over `instanceShards()`, not a count: shard ids are permanent and may be sparse.

**WGL-SHARD-071** (MUST) *Implemented.* `ShardedStorage` MUST live in the `server` module, MUST wrap one `Storage`
per shard primary, and MUST depend on no JDBC type, so it runs over in-memory shards in tests.

**WGL-SHARD-072** (MUST) *Implemented.* `ShardedStorage.inTx` (unrouted) MUST throw
`IllegalStateException`. A
call site that was not moved to a routed entry point then fails every test that runs over two
shards, instead of silently writing to the wrong database.

**WGL-SHARD-073** (MUST) Each shard keeps the failure classification and replay rules of
[chapter 80 §9](80-storage.md) unchanged; a transaction is replayed only on the shard it ran on.

### 4.2 Read-only transactions

**WGL-SHARD-080** (MUST) The read-only part of `Tx` MUST be split out as `ReadTx`, and `Tx` MUST
extend it. A replica connection MUST be opened read-only and MUST be handed out only as a `ReadTx`, so
a write through a replica does not compile.

**WGL-SHARD-081** (MUST) `ReadTx` MUST contain at least: `findInstance`, `findToken`, `tokensOf`,
`listInstances`, `findByCorrelation`, `countInstances`, `pendingSignals`, `backlogByVersion`,
`queueDepth`, `countProcessedSince`, `childInstanceIds`, `schedules`, `nodes`, `anomalies`,
`compensationLog`, `stepDurations`, the event-log reads, and the graph reads (`GraphReads`, split
out of `GraphStore`). Locking reads (`lockInstance`, `lockTask`, `definitionFingerprint`) stay on
`Tx`.

**WGL-SHARD-082** (MUST) `Freshness` MUST be `PRIMARY` or `REPLICA_OK`, chosen **per call site**,
not per method: the same `findInstance` is a primary read inside start and a replica read in the
console.

## 5. Where each operation goes

**WGL-SHARD-090** (MUST) Operations MUST route as follows:

| Operation | Route | Freshness |
|---|---|---|
| start, report, fail, heartbeat, signal, cancel, observe | `inTxFor(id)` | primary |
| claim | rotation over shards ([§6](#6-claims)) | primary |
| leader sweeps: timers, retries, signal deadlines, lease reclaim, observed settle, retention | each shard, in parallel | primary |
| register workflow (definition and `wf_graph_*` rows) | every shard ([WGL-SHARD-100](#7-global-data)) | primary |
| schedules, node table, leadership, event cursors, shard registry | `inHome` | primary |
| event feed poll | every shard, merged ([§10](#10-event-log)) | primary |
| portal: instance lists, search, correlation lookup, counts, backlog, step table | fan out with `readShard` | `REPLICA_OK` |
| portal: the view right after an action (cancel, retry, signal) | `readFor(id)` | `PRIMARY` |
| queue-lag monitor, `StabilityController` backlog | each shard | `PRIMARY` |
| sign-in, credential and session checks on a cache miss; user and role writes | auth shard ([§13](#13-users-and-authorization)) | `PRIMARY` |
| user and role listings in the portal | auth shard | `REPLICA_OK` |
| search indexing | search shards ([§14](#14-search-shards)) | primary |
| full-text and vector search | every search shard, merged | `REPLICA_OK` |

**WGL-SHARD-091** (MUST) A fan-out read MUST run its per-shard queries concurrently and merge them on
a keyset `(created_at, id)`. A page cursor MUST carry that key, not an offset. Counts MUST be summed.

**WGL-SHARD-092** (MUST) The control-plane API MUST let a caller ask for a `PRIMARY` read on the
read RPCs (`GetInstance`, `ListInstances`), so a console can show read-your-writes after an action.

## 6. Claims

**WGL-SHARD-095** (MUST) *Implemented with the routed SPI.* A claim MUST NOT scatter-gather. It MUST try one shard at a time, starting
from a per-node rotating cursor, and MUST return the first non-empty result.

**WGL-SHARD-096** (SHOULD) The dispatch notifier SHOULD carry the shard that produced a ready token,
and a poll woken by it SHOULD try that shard first.

**WGL-SHARD-097** (MUST) Within one shard, claim ordering stays as specified in
[chapter 80](80-storage.md) (oldest instance first). Across shards there is no global order.

**WGL-SHARD-098** (SHOULD) An idle poll costs up to one empty claim per shard per fallback tick; the
per-node single claimer of `docs/performance-plan.md` §2 SHOULD land before or with sharding.

## 7. Global data

**WGL-SHARD-100** (MUST) Registering a workflow MUST write the definition and its graph rows to
every non-`RETIRED` shard, idempotently, so a hot transaction still reads the graph and mutates
runtime state on one database ([chapter 80](80-storage.md), lazy graph loading). A registration
that fails on any shard MUST fail as a whole and MUST be safe to retry.

**WGL-SHARD-101** (MUST) A shard added later MUST receive every registered definition before it
takes a positive weight.

**WGL-SHARD-102** (MUST) The node table, and therefore leader election
([WGL-OPS-022](90-ops.md)), MUST live on the home shard. There is one leader for the whole cluster.

**WGL-SHARD-103** (MUST) The leader MUST sweep every non-`RETIRED` shard, one sweeper per shard,
concurrently. A shard that fails its sweep MUST NOT stop the others.

**WGL-SHARD-104** (MUST) Schedules MUST live on the home shard; an instance a schedule fires is
minted like any other root instance ([WGL-SHARD-020](#22-choosing-a-shard)).

## 8. Read replicas

**WGL-SHARD-110** (MUST) Each shard MUST keep its own pool per replica, in addition to its primary
pool.

**WGL-SHARD-111** (MUST) Replica lag MUST be measured from a heartbeat row. The leader writes
`wf_shard.beat_at = now` on every shard primary once per second, and each node reads it back from
each replica: lag = `now − beat_at`. This works on an idle primary, where
`pg_last_xact_replay_timestamp()` goes stale, and on any database.

**WGL-SHARD-112** (MUST) A `REPLICA_OK` read MUST pick, round-robin, a replica whose last probe
succeeded and whose lag is at most `maxReplicaLagMillis`. A replica whose query fails MUST be ejected
and re-probed with backoff.

**WGL-SHARD-113** (MUST) With no healthy replica, `replicaFallback` decides: `primary` serves the
read from the primary; `fail` refuses it as `UNAVAILABLE`, which protects the primary from search
load when replicas are down.

**WGL-SHARD-114** (SHOULD) Replicas that serve console search SHOULD set
`max_standby_streaming_delay` (or `hot_standby_feedback = on`) and a `statement_timeout`, so long
searches are neither cancelled by replay nor left running forever.

**WGL-SHARD-115** (MUST) The connections one node opens MUST be reported at startup: per shard,
`pool + Σ replicaPool`. Operators size each database's `max_connections` from it times the node
count.

## 9. Observed runs

An observed run's id is derived from its correlation key, so that every reporter lands on the same
instance ([WGL-OBS-010](40-execution-modes.md)). A derived id cannot be minted by weighted random
choice, and it must stay findable when the set of shards changes.

**WGL-SHARD-120** (MUST) An observed run's shard MUST be chosen by **weighted rendezvous hashing**
of `(workflow, key)` over the `ACTIVE` shards of a generation: the shard with the highest
`weight / −ln(hash(workflow, key, shard) / 2^64)` wins. Adding a shard then moves only the keys it
wins, about its share of the weight.

**WGL-SHARD-121** (MUST) The id MUST be `wfo.s{shard}.{digest(workflow + ":" + key)}`.

**WGL-SHARD-122** (MUST) To find a run by key, the server MUST compute the candidate shard under
the current generation, then under each earlier generation still inside the lookup horizon, newest
first, skipping candidates already tried. It MUST use the first instance found.

**WGL-SHARD-123** (MUST) A run that is not found MUST be created on the **current** generation's
candidate. The concurrent-create collision rule of [WGL-OBS-011](40-execution-modes.md) then holds
on that shard unchanged.

**WGL-SHARD-124** (MUST) The lookup horizon MUST be at least the stall threshold
(`WIGGLE_OBSERVE_STALL_MILLIS`) plus the largest expected gap between two reports of one run. A
generation older than the horizon MUST be dropped from the lookup. A run whose reports straddle more
than the horizon MAY be split in two; this MUST be stated in the operator documentation.

**WGL-SHARD-125** (SHOULD) The observe client SHOULD remember the instance id returned by a run's
first report and pass it on later reports (the API already accepts it), which bypasses the lookup.

**WGL-SHARD-126** (MUST) A legacy `wfo_…` id MUST still be found: the lookup's last candidate is
the home shard with the legacy id. *Implemented with shard-carrying ids*, since that change is what
gave observed runs a new id.

## 10. Event log

`seq` is one store-generated sequence per database today ([WGL-EVT-040](60-event-log.md)). Over
several shards there is no single sequence, and a time-ordered substitute cannot be made safe:
node clocks disagree by more than the visibility window ([WGL-EVT-026](60-event-log.md)), so a
consumer would step over entries forever.

**WGL-SHARD-130** (MUST) Each shard MUST keep its own `wf_event` and its own `seq`. Events MUST be
written in the instance's transaction on its shard, as today.

**WGL-SHARD-131** (MUST) A consumer cursor MUST be a vector: one acknowledged seq per shard, stored
on the home shard as `wf_event_cursor_shard(consumer, shard_id, acked_seq)`.

**WGL-SHARD-132** (MUST) `PollEvents` MUST read every shard beyond that shard's position, apply the
visibility window per shard, and merge the results by `created_at`.

**WGL-SHARD-133** (MUST) The wire contract MUST gain an opaque cursor:

| Message | New field | Meaning |
|---|---|---|
| `EventView` | `string cursor` | the consumer's position just after this entry, all shards included |
| `EventView` | `int32 shard` | the shard the entry came from |
| `AckEventsRequest` | `string acked_cursor` | cumulative ack: the `cursor` of the last entry handled |

**WGL-SHARD-134** (MUST) With one shard, `acked_seq` MUST keep working unchanged. With more than one,
an ack carrying only `acked_seq` MUST be refused as `INVALID_ARGUMENT`.

**WGL-SHARD-135** (MUST) Deduplication MUST key on `(shard, seq)`. `(instance id, seq)` still works,
because an instance lives on one shard.

**WGL-SHARD-136** (MUST) `start_from` MUST apply per shard on registration: `0` = each shard's tail,
`-1` = each shard's earliest retained entry. A plain seq is meaningful only with one shard; with more,
it MUST be refused.

**WGL-SHARD-137** (MUST) Retention MUST trim each shard against the lowest position any consumer
holds **for that shard** ([WGL-EVT-030](60-event-log.md)).

## 11. Adding, draining and retiring shards

### 11.1 Generations

**WGL-SHARD-140** (MUST) A change to weights or to the set of `ACTIVE` shards MUST be a new
generation with an `activeFrom` in the future. A node MUST keep minting under the previous
generation until `activeFrom`, then switch.

**WGL-SHARD-141** (MUST) Every node MUST publish the newest generation it has loaded in its node
row. At `activeFrom`, the leader MUST raise an anomaly naming every live node that has not loaded
that generation.

**WGL-SHARD-142** (MUST) *Implemented in `ShardedStorage` (a `TRANSIENT` storage failure).* A node
asked to route an id to a shard it does not know MUST answer
`UNAVAILABLE` (retryable), never *not found*, because the shard may only be missing from that node's
older topology.

### 11.2 Adding a shard

1. Provision the database and its replicas.
2. Add the shard to `shards` as `ACTIVE`, with no weight yet. Roll the topology out to every node.
   Migrations run on it, it enters the registry, and it receives every registered definition
   ([WGL-SHARD-101](#7-global-data)). Every node can now route to it; nothing is minted there.
3. Append a generation that gives it a weight, with `activeFrom` after the rollout will be
   complete, and roll that out. At `activeFrom`, new root instances and new observed runs start
   landing on it.

**WGL-SHARD-145** (MUST) No existing row moves. Instances stay on the shard they were minted on until
they finish and retention purges them; load evens out within about one instance lifetime. A new,
empty shard MAY be given a higher weight to catch up sooner.

**WGL-SHARD-146** (MAY) Moving a long-running instance to another shard MAY be added later as a
per-instance copy-and-forward tool, since every row belongs to one instance. Growth MUST NOT depend
on it.

### 11.3 Draining and retiring

1. Append a generation without the shard, and set its state to `DRAINING`. It mints nothing and
   keeps serving every id it holds.
2. Wait until it holds no non-terminal instance, and retention has purged the terminal ones
   (the console shows its live count).
3. Set its state to `RETIRED`. The registry records it; routes to it answer *not found*
   ([WGL-SHARD-062](#33-the-shard-registry)).
4. Decommission the database. The shard id is never reused.

**WGL-SHARD-150** (MUST) A shard MUST NOT move to `RETIRED` while it holds a non-terminal instance or
an event some consumer has not acknowledged.

**WGL-SHARD-151** (MUST) The home and auth shards MUST NOT be drained. Moving either role to
another database is out of scope.

## 12. The portal in the server

Today the console is a separate process and a pure gRPC client ([WGL-OPS-040](90-ops.md)), and server
nodes serve no UI ([WGL-OPS-041](90-ops.md)). Over shards that split costs twice: every console read
becomes an RPC that the server then fans out, and the gRPC surface lacks reads the console needs
(pending signals, [WGL-OPS-042a](90-ops.md)). The server already connects to every shard and its
replicas, and the `DashboardData` seam already has an in-process adapter (`EngineDashboardData`).

**WGL-SHARD-170** (MUST) The portal MUST be served by the server process, on its own HTTP port
(`WIGGLE_PORTAL_PORT`, `0` = off), separate from the gRPC port. `GET /healthz` stays on the
existing `WIGGLE_DASHBOARD_PORT` contract until that variable is retired.

**WGL-SHARD-171** (MUST) The portal MUST read through `DashboardData` backed by the engine in
process, using the routes and freshness of [§5](#5-where-each-operation-goes). It MUST NOT dial the
gRPC API of its own node or of another node.

**WGL-SHARD-172** (MUST) Any node with the portal enabled MUST be able to serve any portal request.
Sessions MUST live on the auth shard ([§13](#13-users-and-authorization)), so a load balancer needs
no sticky sessions.

**WGL-SHARD-173** (MUST) The portal's mutations (cancel, signal, schedules, retry) MUST call the
engine directly, not the gRPC API, and MUST be checked against the signed-in principal's
permissions before they run.

**WGL-SHARD-174** (SHOULD) A deployment SHOULD be able to run portal-only nodes (portal on, no
claims served) so heavy portal use does not share a node with the gRPC hot path. These are ordinary
server nodes with the gRPC port closed to workers.

**WGL-SHARD-175** (MUST) The standalone `console` module MUST be retired once the in-server portal
matches its views ([WGL-OPS-042](90-ops.md)). WGL-OPS-040 and WGL-OPS-041 are then marked
*withdrawn*.

## 13. Users and authorization

Users, roles and credentials do not shard by instance. A sign-in names a user, not an instance, and
a role assignment must be read the same way by every node. They live on one shard with the `auth`
role.

**WGL-SHARD-180** (MUST) Accounts MUST move from the console-owned file
([WGL-OPS-047](90-ops.md)) to the auth shard. At first start on a deployment that has
`WIGGLE_CONSOLE_USERS_FILE`, the server MUST import it once, keeping the PBKDF2 hashes as they are
([WGL-OPS-048](90-ops.md)), and MUST log that the file is no longer read.

**WGL-SHARD-181** (MUST) The auth schema MUST hold at least: `wf_auth_user` (name, hash, salt,
disabled), `wf_auth_role` (name, set of permissions), `wf_auth_user_role`, `wf_auth_session`
(id hash, user, expiry), `wf_auth_credential` (machine credentials: API key hash or mTLS subject,
bound to a role), and `wf_auth_audit` (who changed what, when).

**WGL-SHARD-182** (MUST) A role MUST be a named set of permissions, not a fixed enum. The built-in
roles `ADMIN` and `VIEWER` MUST exist and keep their current meaning
([WGL-OPS-043](90-ops.md)). Permissions MUST be scoped by action and MAY be scoped by workflow or
queue (`instance.cancel`, `schedule.write`, `user.manage`, `task.poll:<queue>`, `instance.start:<workflow>`).

**WGL-SHARD-183** (MUST) Built-in accounts from the environment ([WGL-OPS-046](90-ops.md)) and the
reachability rules ([WGL-OPS-049](90-ops.md)) MUST keep working. A built-in admin is the way back in
when the auth shard is unreachable or its admins are lost.

**WGL-SHARD-184** (MUST) Credential and session checks MUST be served from a per-node cache.
A cache miss reads the auth **primary**, so a revocation is never hidden by replica lag. A cached
entry MUST expire within `WIGGLE_AUTH_CACHE_MILLIS` (default 30 s), which bounds how long a revoked
session or key keeps working. A password change, a role change and a deletion MUST also signal
every node to drop the entry.

**WGL-SHARD-185** (MUST) With the auth shard unreachable, cached principals MUST keep working, new
sign-ins MUST fail as `UNAVAILABLE`, and the engine MUST keep running
([WGL-SHARD-008](#1-model)).

**WGL-SHARD-186** (MUST NOT) No gRPC RPC may read or write accounts, roles or credentials until the
control plane enforces per-RPC authorization. The reason WGL-OPS-047 kept accounts out of the
database was that any process able to dial the server could reach whatever sits behind the control
plane. Account management stays on the portal's authenticated HTTP surface.

**WGL-SHARD-187** (SHOULD) Per-RPC authorization on the gRPC API SHOULD follow, resolving each call's
API key or mTLS subject through `wf_auth_credential` and the cache of WGL-SHARD-184, so the hot path
never reads the auth shard per call. Until it ships, gRPC remains open to any trusted peer, as today
([chapter 00 §7](00-index.md)).

## 14. Search shards

Today search means exact lookups: by instance id, by correlation id, by workflow and status. Those
stay on the instance shards' replicas. Full-text and vector search over instance data (context, step
input and output, errors) go to dedicated search shards.

### 14.1 Why search does not live on the instance shards

- **Write cost on the hot path.** An HNSW vector index costs far more per insert than the B-tree
  indexes on `wf_token`, and the instance shards are what reach the database ceiling. Indexing in
  the engine transaction would lower steps/s for every workload, searched or not.
- **Replicas don't help.** Physical replicas carry the primary's indexes. A vector index that serves
  queries from a replica has still been maintained on the primary.
- **Embeddings can't be computed in the transaction anyway.** They come from a model call that is
  slow and can fail. Indexing has to be asynchronous whatever the storage, so separate storage costs
  no consistency.
- **Churn.** Instance rows are short-lived and purged by retention. HNSW handles deletes poorly
  (tombstones, rebuilds). Search documents can have their own, longer retention.
- **Different scaling.** Search load grows with query volume and history size; instance load grows
  with steps/s. Separate shards scale each one on its own.

### 14.2 Requirements

**WGL-SHARD-190** (MUST) A search document MUST be derived data: built from an instance shard, never
the source of truth, and always rebuildable from the instance shards while the instances are
retained.

**WGL-SHARD-191** (MUST) An indexer MUST feed the search shards asynchronously, as a named consumer
of the event log ([§10](#10-event-log)). For each event it reads the instance from its shard's
replica, builds the document (identity, workflow, status, timestamps, the text fields chosen for
indexing, and the embedding), and upserts it by instance id. Delivery is at-least-once, so the
upsert MUST be idempotent and MUST NOT replace a document with an older `updated_at`.

**WGL-SHARD-192** (MUST) Embeddings MUST come from a pluggable `Embedder` SPI. The model id and
dimension MUST be stored with each document. Changing the model MUST build a new index alongside
the old one and switch queries only once it is complete.

**WGL-SHARD-193** (MUST) A document's search shard MUST be chosen by rendezvous hashing of its
instance id over the `ACTIVE` search shards. Search shards carry no weight in the mint generations.
Adding or removing a search shard MUST trigger a background rebalance of the documents whose winner
changed. Until it completes, queries MUST deduplicate by instance id, keeping the newest.

**WGL-SHARD-194** (MUST) A query MUST run on every search shard concurrently, on replicas where
healthy ([§8](#8-read-replicas)). Each shard returns its top *k* under the query's filters, and the
merge keeps the global top *k* by score. Filters (workflow, status, time range) MUST be applied in
each shard's query, not after the merge.

**WGL-SHARD-195** (MUST) Search results MUST be filtered by the caller's permissions
([WGL-SHARD-182](#13-users-and-authorization)) before they are returned.

**WGL-SHARD-196** (MUST) Search documents MUST have their own retention
(`WIGGLE_SEARCH_RETENTION_MILLIS`). A hit whose instance has been purged MUST be shown as purged,
not as an error. Deleting an instance on request (as opposed to retention) MUST delete its search
document too.

**WGL-SHARD-197** (MAY) A small deployment MAY give one shard both `instances` and `search`. The
indexing path is the same asynchronous one. The server MUST warn at startup that search shares the
instance shard's write capacity.

**WGL-SHARD-198** (MUST) An unreachable search shard MUST fail searches (or return partial results
marked partial, at the caller's choice) and MUST NOT affect the engine
([WGL-SHARD-008](#1-model)). Indexing resumes from the consumer's cursor.

## 15. Dropping the coordinator

**Status: implemented** (the first step of [§16](#16-delivery-plan)); WGL-SHARD-163 is verified by
`dist/RemovedSettingsTest` and `dist/RoleTest`. The `wiggle` CLI, whose only commands managed the
coordinator, was removed with it. WGL-SHARD-165 (the shard-id fixture) landed with shard-carrying ids.

Sharding replaces the cell coordinator as the way to scale past one database. Cells, namespaces,
epochs and the coordinator go, and so does [chapter 90 §9](90-ops.md).

**WGL-SHARD-160** (MUST) These modules and files MUST be removed:

| Where | What goes |
|---|---|
| `settings.gradle.kts` | the `placement` and `coordinator` modules (`election` stays: it serves [WGL-OPS-022](90-ops.md)) |
| `proto` | `coordinator.proto` and the `CellCoordinator` service |
| `server` | the coordinator link and the namespace/cell id minter in `ServerBundle`, `ServerConfig.namespace`/`cellId` and their copies, coordinator references in `WiggleServer`, `WorkflowEngine`, `Queries` and `Storage` |
| `client` | `CoordinatedConnection`, `NamespaceWorker`, the coordinator factory on `WiggleConnection`, and the epoch-retry and endpoint-rewrite parts of `RpcRetry` / `EndpointRewriter` |
| `dist` | the `coord` package, `EmbeddedCellDeployer`, and the coordinator `Role` |
| `cli` | the coordinator target kind and the namespace / epoch commands ([WGL-COORD-040](90-ops.md)) |
| `console` | the coordinator mode of `ConsoleBackend` / `DashboardData`; the module itself is retired later by [WGL-SHARD-175](#12-the-portal-in-the-server) |
| `conformance` | `placement-v1.json`, replaced by a shard id fixture ([WGL-SHARD-165](#15-dropping-the-coordinator)) |
| config | `WIGGLE_COORDINATOR_URL`, `WIGGLE_NAMESPACE`, `WIGGLE_CELL_ID`, `WIGGLE_ADVERTISE_HOST`, `WIGGLE_REGION`, `WIGGLE_COORD_STORE`, `WIGGLE_COORD_JDBC_*`, `WIGGLE_ENDPOINT_REWRITE` |
| Helm / deploy | coordinator templates and values |

**WGL-SHARD-161** (MUST) The id parser MUST keep accepting namespaced ids forever
([WGL-SHARD-031](#23-ids-from-before-sharding)). Only the minter goes.

**WGL-SHARD-162** (MUST) A multi-cell deployment MUST drain every cell but one before converting; that
cell's database becomes the home shard. Every id the others minted would route home
([WGL-SHARD-031](#23-ids-from-before-sharding)): their tokens were minted bare, so nothing in a report
says which database holds it.

**WGL-SHARD-163** (MUST) Setting a removed variable MUST fail startup with a message naming its
replacement, rather than being silently ignored.

**WGL-SHARD-164** (MUST) Removal is a wire change for the separate Go and Python clients: their
coordinated-connection mode goes too. It MUST ship in a release whose notes say so.

**WGL-SHARD-165** (MUST) `conformance/shard-ids-v1.json` MUST replace the placement fixture: id
formatting and parsing, legacy and namespaced ids, the shard each carries, and how derived ids inherit
it. The observed-run rendezvous choice ([WGL-SHARD-120](#9-observed-runs)) joins it with §9. Every
client that mints or parses ids MUST pass it ([WGL-GEN-004](00-index.md)).

**WGL-SHARD-166** (MUST) The server⊥coordinator source rule ([WGL-COORD-001](90-ops.md)) is
withdrawn with the coordinator, and chapter 90 §9 is marked *withdrawn*, not deleted
([chapter 00 §2](00-index.md)).

## 16. Delivery plan

One PR each, in this order. Each leaves the build green and a one-shard deployment unchanged.

1. **Drop the coordinator** ([§15](#15-dropping-the-coordinator)). Simplifies the id and config code
   the later steps change.
2. **Portal in the server** ([§12](#12-the-portal-in-the-server)), still on one database and still
   with file accounts. Retire the `console` module once it matches.
3. **Shard-carrying ids**: the new format, tokens and children inheriting their parent's shard,
   legacy and namespaced parsing, and the conformance fixture. Everything still mints shard `0`.
4. **Routed SPI**: `inTxFor` / `inShard` / `inHome` / `readFor` / `readShard`, `ReadTx`, and every
   engine call site moved onto them. No behaviour change at one shard.
5. **`ShardedStorage`** over in-memory shards, with the engine test suite run over two shards.
6. **Topology document** with roles, the registry, `wf_shard`, startup validation, and generations.
7. **Claims, sweeps, registration fan-out and portal fan-out.**
8. **Read replicas**: pools, the lag probe, fallback, and the per-call-site freshness table.
9. **Observed runs** under rendezvous hashing.
10. **Event log** per-shard cursors and the wire fields.
11. **Measure**: `deploy/gcp/ceiling.sh` on two and on four Cloud SQL shards, steps/s first.
12. **Accounts on the auth shard** ([§13](#13-users-and-authorization)): schema, file import,
    permission-set roles, the cache, and portal enforcement.
13. **Per-RPC authorization** on gRPC ([WGL-SHARD-187](#13-users-and-authorization)).
14. **Search shards** ([§14](#14-search-shards)): indexer, `Embedder` SPI, full-text first, then
    vector.

## 17. Open questions

- **Cross-shard sub-workflows by design.** [WGL-SHARD-014](#21-format) pins a child to its parent's
  shard, so a fan-out of thousands of children loads one shard. Spreading them would need a
  cross-shard join protocol (outbox and signal). Out of scope until a workload needs it.
- **Correlation lookups** (`findByCorrelation`) fan out to every shard. A start-time idempotency
  key, if one is added, would need its own routing rule like [§9](#9-observed-runs).
- **Home shard load.** Schedules, node heartbeats, event cursors and the leader's beat writes all
  land on the home shard. They are small, but should be measured at high shard counts.
- **Which instance fields are searchable.** Context and step I/O may hold personal data. Whether
  indexing is opt-in per workflow (a topology flag) or per field is undecided.
- **A search engine other than Postgres.** WGL-SHARD-190 to 198 do not depend on pgvector. A
  `SearchStore` SPI would let OpenSearch or a dedicated vector database stand in for search shards;
  pgvector first keeps one operational stack.
- **Multi-tenancy.** If tenants arrive, the auth shard's permission scopes and the search filters
  are where tenant isolation would attach.
