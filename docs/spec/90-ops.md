# 90 — Operations

← [Storage](80-storage.md) · [Index](00-index.md)

Everything a deployment decides: how settings are supplied, what the cluster does with them, what the
portal shows, how the channel is secured, and what the distribution runs. §9, the cell coordinator,
is withdrawn and kept for its requirement ids.

## 1. Configuration

**WGL-OPS-001** (MUST) A server setting MUST be read from a **system property first, then an
environment variable**, then a default, so `-Dwiggle.port=9090` and `WIGGLE_PORT=9090` are equivalent.

**WGL-OPS-002** (MUST) A worker MUST be configured programmatically through `WorkerOptions`
([WGL-WRK-090](20-worker.md)); the `WIGGLE_*` worker variables in the example module are that module's
conventions, not the library's.

**WGL-OPS-003** (MUST) The server's settings MUST be exactly these, with these defaults:

### 1.1 Core and storage

| Variable | Property | Default | Meaning |
|---|---|---|---|
| `WIGGLE_ROLE` | — | `server` | which process the image runs: only `server` (`cell` is the old name for it; `console` and `coordinator` are refused) |
| `WIGGLE_PORT` | `wiggle.port` | `8080` | gRPC port; `0` picks a free one |
| `WIGGLE_NODE_NAME` | `wiggle.node.name` | hostname | name in cluster membership |
| `WIGGLE_JDBC_URL` | `wiggle.jdbc.url` | *(unset)* | unset = in-memory single node; set to cluster on a database |
| `WIGGLE_JDBC_USER` / `WIGGLE_JDBC_PASSWORD` | `wiggle.jdbc.user` / `.password` | *(unset)* | credentials |
| `WIGGLE_JDBC_POOL_SIZE` | `wiggle.jdbc.poolSize` | `10` | HikariCP max pool size |
| `WIGGLE_JDBC_TX_ATTEMPTS` | `wiggle.jdbc.txAttempts` | `3` | attempts for a transaction that rolled back on a momentary failure; `1` disables the replay |
| `WIGGLE_JDBC_TX_RETRY_DELAY_MILLIS` | `wiggle.jdbc.txRetryDelayMillis` | `50` | pause before a replay, multiplied by the attempt |
| `WIGGLE_SCHEMA_MODE` | — | `apply` | `apply` migrates on startup; `verify` applies nothing and fails fast if behind or drifted |
| `WIGGLE_MIGRATE_ONLY` | — | `false` | apply migrations and exit (forces apply even when the env pins verify) |

### 1.2 Engine, cluster and housekeeping

| Variable | Property | Default | Meaning |
|---|---|---|---|
| `WIGGLE_LEASE_MILLIS` | `wiggle.lease.millis` | `30000` | default task lease before a stalled step is reclaimed |
| `WIGGLE_LONGPOLL_MAX_MILLIS` | `wiggle.longpoll.maxMillis` | `20000` | cap on how long a poll may block server-side |
| `WIGGLE_POLL_INTERVAL_MILLIS` | `wiggle.poll.intervalMillis` | `1000` | housekeeping tick cadence (floored at 200 ms) |
| `WIGGLE_HOUSEKEEPING_BATCH` | `wiggle.housekeeping.batch` | `100` | items swept per pass |
| `WIGGLE_ADAPTIVE_HOUSEKEEPING` | `wiggle.adaptive.housekeeping` | `false` | a sweep that fills its batch runs again immediately (drain mode) |
| `WIGGLE_ADAPTIVE_FALLBACK_POLL` | `wiggle.adaptive.fallback` | `false` | freshly-parked long-polls re-claim fast (fallback ÷ 4, floor 10 ms) and decay to the configured interval |
| `WIGGLE_FALLBACK_POLL_MILLIS` | — | `100` | long-poll fallback re-claim interval |
| `WIGGLE_DISPATCH_LINGER_MILLIS` | — | `5` | wake-on-produce batch linger; `0` claims immediately |
| `WIGGLE_HEARTBEAT_INTERVAL_MILLIS` | `wiggle.heartbeat.intervalMillis` | `5000` | node heartbeat / election interval |
| `WIGGLE_MISSED_HEARTBEATS` | `wiggle.heartbeat.missedBeforeDead` | `3` | missed beats before a node is dead |
| `WIGGLE_LOOP_MAX_ITERATIONS` | `wiggle.loop.max.iterations` | `10000` | default `repeatWhile` budget |
| `WIGGLE_RETENTION_MILLIS` | `wiggle.retention.millis` | `86400000` | how long finished instances are kept |
| `WIGGLE_EVENTS_RETENTION_MILLIS` | `wiggle.events.retentionMillis` | `604800000` | how long an acknowledged event is kept |
| `WIGGLE_EVENTS_VISIBILITY_MILLIS` | `wiggle.events.visibilityMillis` | `50` | how long an appended event is held back from the feed |
| `WIGGLE_QUEUE_LAG_CHECK_INTERVAL_MILLIS` | `wiggle.queueLag.checkIntervalMillis` | `5000` | backlog check cadence |
| `WIGGLE_QUEUE_LAG_WARN_MILLIS` | `wiggle.queueLag.warnThresholdMillis` | `10000` | WARN when the backlog will not drain within this budget |
| `WIGGLE_ALLOW_GRAPH_REPLACE` | `wiggle.allowGraphReplace` | `false` | development only: honour a forced re-registration |
| `WIGGLE_DASHBOARD_PORT` | `wiggle.dashboard.port` | `0` (off) | port for the `/healthz` probe on a server node |
| `WIGGLE_PORTAL_PORT` | — | `8070` | port the node serves the portal on, `0` for none ([§4](#4-the-portal)) |

### 1.3 Memory admission control

| Variable | Default | Meaning |
|---|---|---|
| `WIGGLE_MEMORY_SHEDDING_ENABLED` | `false` | turn admission control on |
| `WIGGLE_MEMORY_THRESHOLD` | `0.90` | heap utilisation above which the node is "under pressure" |
| `WIGGLE_MEMORY_REJECT_RATIO` | `0.10` | fraction of polls rejected while under pressure |
| `WIGGLE_MEMORY_RETRY_MILLIS` | `2000` | hold-off handed to a rejected poll |
| `WIGGLE_MEMORY_RETRY_JITTER_MILLIS` | `1000` | per-response jitter added to it |

### 1.4 TLS and logging

| Variable | Property | Default | Meaning |
|---|---|---|---|
| `WIGGLE_TLS_KEYSTORE` (+`_PASSWORD`) | `wiggle.tls.keystore[.password]` | *(unset)* | keystore ⇒ TLS on; unset ⇒ plaintext |
| `WIGGLE_TLS_TRUSTSTORE` (+`_PASSWORD`) | `wiggle.tls.truststore[.password]` | *(unset)* | on a server ⇒ require client certificates (mTLS); on a client ⇒ the certs it presents |
| `WIGGLE_LOG_FILE` | — | *(unset)* | also log to a rotating file (5 × 10 MB) |
| `WIGGLE_LOG_LEVEL` | — | `INFO` | file level in `System.Logger` names |

### 1.5 Coordinator and placement (withdrawn)

*Withdrawn: the cell coordinator was removed ([chapter 85 §15](85-sharding.md#15-dropping-the-coordinator)).* A node refuses to start while any of these is set, and names it
([WGL-SHARD-163](85-sharding.md#15-dropping-the-coordinator)).

| Variable | Default | Meaning |
|---|---|---|
| `WIGGLE_COORDINATOR_URL` | *(unset)* | unset ⇒ standalone; set ⇒ config comes from the coordinator and the node announces itself |
| `WIGGLE_NAMESPACE` | *(unset)* | the placement namespace; set ⇒ the node mints epoch-aware ids |
| `WIGGLE_CELL_ID` | *(unset)* | the cell this node belongs to; stamped into every id it mints, and required of a coordinated node |
| `WIGGLE_ADVERTISE_HOST` | *(unset)* | the host a node advertises to the coordinator |
| `WIGGLE_REGION` | *(unset)* | the region a node's data lives in / a caller's region |
| `WIGGLE_COORD_STORE` | *(unset)* | `jdbc:<url>` gives the coordinator its own durable store; unset = in-memory, not durable |
| `WIGGLE_COORD_JDBC_USER` / `_PASSWORD` / `_POOL` | *(unset)* / `4` | that store's credentials and pool |
| `WIGGLE_ENDPOINT_REWRITE` | *(unset)* | client-side `MATCH=REPLACEMENT` list rewriting resolved endpoints (port-forwarded or NAT'd clusters) |

### 1.6 Portal

Read by a server node that serves the portal.

| Variable | Default | Meaning |
|---|---|---|
| `WIGGLE_DASHBOARD_USER` / `_PASSWORD` | `admin` / *(unset)* | built-in admin; unset password = first-run setup of the admin account ([WGL-OPS-051](#4-the-portal)) |
| `WIGGLE_DASHBOARD_VIEWER_USER` / `_PASSWORD` | `viewer` / *(unset)* | optional built-in read-only account |
| `WIGGLE_CONSOLE_USERS_FILE` | `wiggle-users.json` | a console users file from before the auth shard, imported once ([WGL-SHARD-180](85-sharding.md#13-users-and-authorization)) |
| `WIGGLE_AUTH_CACHE_MILLIS` | `30000` | how long a node serves a cached account, session or credential before reading it again |
| `WIGGLE_GRPC_AUTH` | `off` | per-RPC authorization of the gRPC API: `off`, `log` or `enforce` ([chapter 70 §11](70-api.md#11-per-rpc-authorization)) |
| `WIGGLE_API_KEY` | *(unset)* | on a client or worker, the API key it presents |
| `WIGGLE_SEARCH_ENABLED` | `false` | full-text search on the one database (a topology enables it with a `search` shard instead) |
| `WIGGLE_SEARCH_RETENTION_MILLIS` | `2592000000` | how long a search document outlives its instance's last change (30 days) |
| `WIGGLE_SEARCH_WORKFLOWS` | *(all)* | comma-separated workflows to index; unset indexes every one |
| `WIGGLE_SEARCH_UPKEEP_MILLIS` | `60000` | how often the leader applies search retention, rebalances, backfills vectors and moves the embedding model on |
| `WIGGLE_EMBEDDER` | `none` | semantic search: `none`, `hashing` (no model; development) or `http` (an OpenAI-compatible `/embeddings` API) |
| `WIGGLE_EMBEDDER_URL` / `_MODEL` / `_DIMENSION` / `_API_KEY` | *(unset)* | the `http` embedder's API root, model, vector size and key (`_DIMENSION` alone sizes `hashing`, default 256) |
| `WIGGLE_EMBEDDER_PREVIOUS_MODEL` / `_DIMENSION` | *(unset)* | the model being replaced, on the same service, so semantic queries keep working while the new index builds |
| `WIGGLE_TLS_*` | *(unset)* | the server's keystore and truststore serve the portal's HTTPS too |

**WGL-OPS-004** (MUST) A definition's `DEFAULT` execution mode MUST resolve to `SERVER`. There is
**no** server-wide execution-mode setting: `WIGGLE_EXECUTION_MODE` is read only by the example
module's benchmark, so a deployment-level default is chosen per workflow with `executeIn*()`.

*Verified by:* `tests/ServerConfigTest`, `tests/ServerRoleTest`, `dist/RoleTest`, `tests/LoggingTest`.

**WGL-OPS-010** (MUST) An unrecognised `WIGGLE_ROLE` MUST fail at startup rather than defaulting to a
server.

**WGL-OPS-011** (MUST) Logging MUST go through the JDK's `System.Logger` with no logging dependency;
`INFO` MUST be a clean lifecycle narrative (start/stop, membership, leader, registrations, failures,
purges, lag warnings) and `DEBUG` MUST add per-token/step/RPC detail.

## 2. Cluster and leadership

**WGL-OPS-020** (MUST) Clustering MUST be nothing but a shared database: several nodes pointed at one
database form a cluster, each serving the API and handing out work.

**WGL-OPS-021** (MUST) Exactly one node MUST be leader for the clock-driven duties
([WGL-ENG-120](30-engine.md)).

**WGL-OPS-022** (MUST) The election MUST be announce-and-heartbeat over the shared node table: every
process announces itself and heartbeats, the longest-running live process leads with ties broken by id,
and a process whose own heartbeat has gone stale MUST stand down before doing leader work.

**WGL-OPS-023** (MUST) No consensus protocol is required, because the shared table is the only source of
truth and the rule over it is a pure function every process evaluates identically.

**WGL-OPS-024** (MUST) Killing any node, leader included, MUST leave the rest serving.

**WGL-OPS-025** (MUST) The node row MUST also carry the worker count and the leader flag the portal
reads.

**WGL-OPS-026** (MUST) Nodes MUST multiply availability and API capacity, never database throughput.

*Verified by:* `election/LeaderElectionTest`, `tests/NodeLifecycleTest`, `server/cluster/HousekeeperTest`.

## 3. Registration control

**WGL-OPS-030** (MUST) `register(spec, force = true)` MUST be refused unless the server was started with
`WIGGLE_ALLOW_GRAPH_REPLACE=true`, so a `force` left in application code cannot rewrite a published
graph in production.

**WGL-OPS-031** (MUST) A forgotten version bump MUST therefore be a loud deploy-time error, not a graph
swapped under running instances.

## 4. The portal

The web UI is served by server nodes ([WGL-SHARD-170](85-sharding.md#12-the-portal-in-the-server)).
It was a separate console process; the requirements that described that are withdrawn and kept for
their ids.

**WGL-OPS-040** (*withdrawn*, by [WGL-SHARD-175](85-sharding.md#12-the-portal-in-the-server)) The
console MUST be a separate process and a **pure gRPC client** — no engine, no storage — pointed at a
cluster by `WIGGLE_URL`.

**WGL-OPS-041** (*withdrawn*, by [WGL-SHARD-175](85-sharding.md#12-the-portal-in-the-server)) Server
nodes MUST serve no UI. A node's only HTTP surface is `GET /healthz` returning 200 `ok`, enabled by
`WIGGLE_DASHBOARD_PORT`.

**WGL-OPS-042** (MUST) The portal MUST provide these views: **Instances** (filter, search by instance or
correlation id, a live trace overlaying token status on the workflow diagram, cancel, inline signal
delivery), **Workflows** (render any compiled graph), **Schedules** (create and delete interval and cron
schedules), **Signals** (waits pending delivery), **Backlog** (dispatchable work no live poller covers),
**Performance** (per-step p50/p95 by the handler's own clock plus queue wait, slowest first), and
**Users** (admins only).

**WGL-OPS-042a** (*withdrawn*: the portal reads the engine, which enumerates signal waits) The
**Signals** view MUST be empty against a gRPC backend today: the control plane has no RPC that
enumerates parked signal waits. Delivering a signal from the console works regardless.

**WGL-OPS-043** (MUST) There MUST be two built-in roles: **admin** does everything; **viewer** sees
everything and is refused every mutating call. Further roles are permission sets
([WGL-SHARD-182](85-sharding.md#13-users-and-authorization)).

**WGL-OPS-044** (MUST) Browsers MUST get a `/login` form setting an HttpOnly session cookie; programmatic
clients MUST be able to use HTTP Basic.

**WGL-OPS-045** (MUST) Credentials travel in cleartext over plain HTTP, so anything exposed SHOULD be
served over TLS.

**WGL-OPS-046** (MUST) Accounts come from two places: **built-in** accounts configured in the environment
of the nodes serving the portal (and unchangeable from inside it), and **managed** accounts an admin
creates in the portal.

**WGL-OPS-047** (*withdrawn*, by [WGL-SHARD-180](85-sharding.md#13-users-and-authorization): managed
accounts live on the auth shard) Managed accounts MUST live in a JSON file the console owns, **not** in
the workflow database.

**WGL-OPS-048** (MUST) Passwords MUST be stored as PBKDF2-HMAC-SHA256 hashes over a per-account random
salt, never in the clear, and a session MUST be stored only as a hash of its token.

**WGL-OPS-049** (MUST) Three rules MUST keep a portal reachable: a managed account cannot take a built-in
account's name and built-in accounts cannot be deleted or re-passworded from the portal; with no
built-in admin to fall back on, no change may leave no enabled account that can manage users;
and the first-run setup of WGL-OPS-051 is offered only while no account exists.

**WGL-OPS-050** (MUST) Any account MAY change its own password after proving the current one — the one
write a viewer is allowed. Changing or resetting a password MUST sign out that account's **other**
sessions; deleting an account MUST sign it out everywhere.

**WGL-OPS-051** (MUST) With neither a built-in password nor any managed account, the portal MUST
be in first-run setup: every page MUST lead to a screen that sets the admin account's password
(`WIGGLE_DASHBOARD_USER`, default `admin`), and every other API call MUST be refused 401. Setting it
MUST create the account with the `admin` role on the auth shard and sign it in, and the server MUST
warn at startup while setup is pending. Once any account exists, setup MUST be refused (409), so it
cannot be used to take over a portal already set up. A portal with nowhere to keep accounts (built
without the auth shard, as in tests) is open instead, and every request is an admin.

*Verified by:* `console/ConsoleWebTest`, `console/ConsoleUsersTest`, `console/ConsoleDataTest`,
`console/PortalTest`, `tests/HealthzTest`, `server/auth/AccountsTest`, `server/auth/AuthCacheTest`.

## 5. Transport security

**WGL-OPS-060** (MUST) TLS MUST be opt-in and shared by the gRPC API and the portal's HTTP: a keystore
turns TLS on; a truststore additionally requires client certificates on a server and presents a client
certificate on a client or worker. Unset means plaintext.

**WGL-OPS-061** (MUST) Stores MUST be PKCS12 by default, with a `.jks` path loaded as JKS.

**WGL-OPS-062** (MUST) Clients and workers MUST read the same variables.

**WGL-OPS-063** (MUST) TLS MUST NOT be mistaken for authorization: it secures the channel and, with mTLS,
authenticates the peer. What a peer may call is decided by per-RPC authorization when
`WIGGLE_GRPC_AUTH` is `enforce` ([chapter 70 §11](70-api.md#11-per-rpc-authorization)); with it off,
any trusted peer may call any RPC.

*Verified by:* `tests/TlsTest`.

## 6. Load protection

**WGL-OPS-070** (MUST) **Backlog coverage** MUST exist because work nothing can claim is invisible
elsewhere: a token on a queue nobody polls, or on a version every worker scoped itself out of, sits
`READY` forever while the instance reads `RUNNING` — which is exactly what healthy looks like a moment
before a worker picks it up.

**WGL-OPS-071** (MUST) A slice MUST be reported covered when some live poller polls that queue **and** is
either unscoped or names that version — the same test the claim applies — so the view reports what the
dispatcher would actually do.

**WGL-OPS-072** (MUST) Coverage MUST be reported per node, because the poller registry is in memory; a
worker that stops polling MUST stop counting as cover once its entry times out.

**WGL-OPS-073** (MUST) **Queue-lag monitoring** MUST have the leader compare the dispatchable backlog
against the cluster-wide completion rate and log a `WARNING` when it is not draining within the
configured budget.

**WGL-OPS-074** (MUST) **Memory admission control**, when enabled, MUST reject a configured fraction of
incoming polls before doing any work whenever heap utilisation crosses the threshold, answering each
rejection empty with a jittered hold-off, and MUST recover on its own once utilisation falls back.

**WGL-OPS-075** (MUST) The guard MUST use the JVM's GC-accurate heap utilisation and MUST track the
serialised bytes held by in-flight request/response cycles.

**WGL-OPS-076** (MUST) A shed poll MUST NOT be an error: the worker holds off and retries.

*Verified by:* `tests/BacklogCoverageTest`, `server/cluster/QueueLagMonitorTest`,
`server/grpc/MemoryGuardTest`, `tests/MemoryPollTest`.

## 7. Deployment postures

**WGL-OPS-080** (MUST) The same workflows MUST run unchanged in all three postures: **embedded**
(`WiggleServer` in the application's JVM, in-memory or on a database), **standalone** (one node serving
gRPC), and **cluster** (several nodes on one database).

**WGL-OPS-081** (MUST) An embedded server MUST need nothing but a config and a storage factory, and with
no database configured MUST keep state in memory — the test posture.

**WGL-OPS-082** (MUST) One container image MUST run the server, with the portal behind
`WIGGLE_PORTAL_PORT`, and MUST bundle every storage backend.

**WGL-OPS-083** (MUST) A pre-built distribution archive MUST run on a JRE 21 with no build, registry or
network access, and each release MUST attach it with signed checksums (airgapped installs).

**WGL-OPS-084** (MUST) The published image MUST be signed and multi-arch (amd64 + arm64) and MUST be
available from both Docker Hub and GHCR as the same image.

**WGL-OPS-085** (MUST) The Helm chart MUST deploy a hardened pod — distroless image, non-root, read-only
root filesystem, all capabilities dropped — that passes a *restricted* PodSecurity namespace unmodified,
with liveness and readiness probes on `/healthz`, a configurable replica count, storage settings, a
secret for credentials, a service, a service account and a pod disruption budget.

**WGL-OPS-086** (MUST) The single-VM deployment MUST bring up server, PostgreSQL and portal on one
machine with TLS on, owned by systemd, with certificates from one script (Let's Encrypt or a private CA)
and weekly renewal that restarts only on a real change.

**WGL-OPS-087** (SHOULD) The portal in that posture SHOULD bind to loopback so it is reached over an SSH
tunnel rather than exposed.

**WGL-OPS-088** (MUST) Published artifacts MUST include a BOM keeping every module and the shared
gRPC/protobuf stack version-aligned, and a **shaded** client (`wiggle-client-all`) with gRPC, protobuf
and Guava relocated so it has zero transitive dependencies.

*Verified by:* `tests/ServerRoleTest`, `dist/RoleTest`, `scripts/regression-released-image.sh`,
`scripts/kind-up.sh`.

## 8. Performance envelope

Not requirements — the measured shape of the system, so a deployment knows what to expect. Reproduce
with the tools in `example`.

**WGL-OPS-090** (SHOULD) Single-JVM engine throughput (embedded server, in-memory store, 20-step
workflow, 4 workers): `SERVER` ≈ 962 instances/s (19.2k step completions/s); `LOCAL_SYNC` ≈ 1,379
instances/s; `LOCAL_ASYNC` ≈ 4,402 instances/s (88.0k steps/s).

**WGL-OPS-091** (SHOULD) One server node on one PostgreSQL 16 on a 10-core laptop sustains ≈ 300 durable
workflow starts/s (≈ 2,400 durable step executions/s) at ≈ 1 s end-to-end completion latency, with
submit p50 ≈ 12 ms / p99 ≈ 130 ms; ≈ 350/s survives a one-minute burst before backlog compounds.

**WGL-OPS-092** (SHOULD) Adding nodes does not raise that ceiling: the database and the machine are the
limit.

**WGL-OPS-093** (SHOULD) Adaptive housekeeping raises timer promotion from the batch ÷ tick floor
(≈ 100/s at defaults) to ≈ 1,700/s; the adaptive fallback poll cuts cross-node dispatch latency from
p50 ≈ 105 ms to ≈ 28 ms, without costing the throughput ceiling.

**WGL-OPS-094** (SHOULD) Accumulated history costs latency: the same setup with ~500k retained instances
has shown ≈ 2× the latency at the ceiling, so retention cadence is part of capacity planning.

## 9. The optional cell coordinator (withdrawn)

*Withdrawn: the cell coordinator was removed ([chapter 85 §15](85-sharding.md#15-dropping-the-coordinator)).* Every requirement in this section is withdrawn; the ids are kept so they are never reused
([chapter 00 §2](00-index.md)).

Off by default. A standalone or clustered server never involves it; `WIGGLE_COORDINATOR_URL` is what
turns it on. It shards one namespace's instances across **cells** (each a cluster over its own
database) and is never on the data path.

**WGL-COORD-001** (MUST) The server and the coordinator MUST NOT depend on each other in source. The
only link between them is the gRPC contract, and they are composed in the distribution.

**WGL-COORD-002** (MUST) A coordinator MUST run no engine and no cell, and MUST use its **own** database,
separate from any cell's ([WGL-STOR-047](80-storage.md)).

**WGL-COORD-003** (MUST) The coordinator store MUST have two backends: in-memory (not durable) and JDBC
(`WIGGLE_COORD_STORE`), the latter required for HA.

**WGL-COORD-004** (MUST) A coordinator outage MUST NOT block a node's boot: config fetch and
announcement are best-effort, and with no coordinator configured the node behaves exactly as a
standalone server.

### 9.1 Placement

**WGL-COORD-010** (MUST) An instance id under a namespace MUST be
`{namespace}[.c{cell}].e{epoch}.s{shard}.{ulid}`, and an instance's cell MUST be a pure function of its
id plus the placement policy — there is no per-instance directory.

**WGL-COORD-011** (MUST) The optional `.c{cell}` segment MUST sit **before** the epoch, because a ulid may
contain dots and a trailing segment would parse ambiguously.

**WGL-COORD-012** (MUST) A legacy bare id (`wfi_…`) MUST NOT parse, and MUST be routed to the genesis
cell.

**WGL-COORD-013** (MUST) A formatted id MUST be at most 128 characters, matching the schema's column
width.

**WGL-COORD-014** (MUST) A namespace policy MUST be `(namespace, currentEpoch, epochs → ring, revision)`
and MUST be O(namespaces × epochs × cells) — never per instance.

**WGL-COORD-015** (MUST) An epoch MUST have status `OPEN`, `DRAINING` or `RETIRED`. Opening a new epoch
MUST advance the epoch by one, move the outgoing epoch to `DRAINING` while keeping its ring so ids
minted into it stay resolvable, and leave older epochs untouched. A namespace with no policy starts at
epoch 0. An epoch MUST place at least one shard.

**WGL-COORD-016** (MUST) Reconfiguration writes MUST be compare-and-set on the policy revision, retried a
bounded number of times, so several coordinators may share a store.

**WGL-COORD-017** (MUST) A `DRAINING` epoch MUST only be retired once every cell reports zero live
instances in it, where live includes `COMPENSATING`.

**WGL-COORD-018** (MUST) The id codec and the resolver MUST be held to the shared fixtures in
`conformance/placement-v1.json`, because three client languages each carry their own copy of these
rules.

*Verified by:* `placement/ConformanceTest`, `placement/EpochsTest`, `placement/PlacementsTest`,
`tests/EpochAwareIdTest`, `tests/IdCodecTest`, `server/coord/EpochRetireTest`.

### 9.2 Coordinator API

**WGL-COORD-020** (MUST) `CellCoordinator` MUST offer: node lifecycle (`FetchConfig`, `Register`,
`Heartbeat`, `Deregister`), resolution (`Resolve`, `ActiveCells`), admin (`OpenEpoch`,
`RegisterWorkflow`, `DeregisterWorkflow`, `ListWorkflows`) and `Dump` for debugging.

**WGL-COORD-021** (MUST) Config distribution MUST be **pull, not push**: a `generation` newer than the
node's tells it to re-fetch, carried on both the register response and every heartbeat.

**WGL-COORD-022** (MUST) `NodeConfig` MUST carry the namespace, generation, an engine-version / schema
guardrail, the storage spec, a sparse tuning overlay (an unset field keeps the node's default), a TTL,
and the epoch and shards this node's cell mints into.

**WGL-COORD-023** (MUST) A storage secret MUST travel as a reference the node resolves, and MUST NOT be
stored by the coordinator.

**WGL-COORD-024** (MUST) A coordinated node MUST set a cell id, and a cell id already bound to a
different cell MUST be refused.

**WGL-COORD-025** (MUST) Resolution MUST answer a gRPC **target plus TLS**, not a bare host and port, and
MUST be region-aware: a caller's region gets a region-appropriate address and never a different cell.

**WGL-COORD-026** (MUST) `Resolve` MUST take either an instance id (routed by its parsed
namespace/epoch/shard) or a namespace (the current epoch, for a new start).

**WGL-COORD-027** (MUST) `ActiveCells` MUST list the region-local cells with live work and MUST carry a
generation a worker reconciles its poll set against.

**WGL-COORD-028** (MUST) `RegisterWorkflow` MUST fan the compiled graph out to every cell of the
namespace under its declared version, and MUST report how many cells were seeded.

### 9.3 Coordinated clients

**WGL-COORD-030** (MUST) `WiggleConnection.coordinator(url[, tls[, callerRegion]])` MUST resolve through
the coordinator and cache what it resolves; the data path (poll, report) MUST go straight to a cell.

**WGL-COORD-031** (MUST) A start routed to a node that has not yet applied a new epoch MUST be retried
after an on-demand placement refresh rather than failed.

**WGL-COORD-032** (MAY) A client MAY rewrite resolved endpoints via `WIGGLE_ENDPOINT_REWRITE`
(`MATCH=REPLACEMENT`, comma-separated, `*:8080=127.0.0.1:18100` style) for port-forwarded or NAT'd
clusters.

*Verified by:* `tests/CoordinatorApiTest`, `tests/CoordinatorResolveTest`,
`tests/CoordinatorFanoutTest`, `tests/CoordinatorPlacementTest`, `tests/MultiCellResolveTest`,
`tests/CellRoutingTest`, `client/EndpointRewriterTest`.

### 9.4 The `wiggle` CLI

**WGL-COORD-040** (MUST) The CLI MUST manage a coordinator's namespace allocations and placement epochs
only. Workflows are defined and registered in Java, never from the CLI.

**WGL-COORD-041** (MUST) It MUST offer `use` (set or show the target: a coordinator or a single cell,
saved under `WIGGLE_CONFIG_HOME` / `~/.wiggle` and used by every command unless overridden),
`allocations`, `deallocate` and `open-epoch`.

**WGL-COORD-042** (MUST) An invalid target kind or a missing address MUST exit with a usage error.

*Verified by:* `cli/TargetTest`.
