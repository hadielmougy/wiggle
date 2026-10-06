# Wiggle — functional specification

This suite specifies **what Wiggle does**, as a contract: numbered requirements that an
implementation can be held to and a test can point at. It is written from the code in this
repository, not from intent — where a design document in `docs/` describes something the code does
not do, this suite follows the code and says so.

It is the specification of record for spec-driven work: a change to behaviour changes a requirement
here first, and the requirement names the test that holds it.

## Chapters

| # | Chapter | Covers | ID prefix |
|---|---|---|---|
| 10 | [Authoring](10-authoring.md) | `FlowSpec`/`WiggleFlow`, the node model, compilation, validation, versioning | `WGL-AUTH` |
| 20 | [Worker contract](20-worker.md) | `@ForFlow` binding, handler signatures, polling, leases, heartbeats, reporting | `WGL-WRK` |
| 30 | [Engine semantics](30-engine.md) | tokens over the graph, both state machines, per-node behaviour, failure paths | `WGL-ENG` |
| 35 | [Dynamic flows](35-dynamic-flows.md) *(proposed)* | branches a step creates at run time with `Step.create(...).thenApply(...)`: spawning steps, fragments, rounds, combine parameters by type (drops `@Context`), retiring `DYN_FORK` | `WGL-DYN` |
| 40 | [Execution modes](40-execution-modes.md) | `SERVER`, `LOCAL_SYNC`, `LOCAL_ASYNC`, step timing statistics; the withdrawn `OBSERVED` | `WGL-MODE`, `WGL-OBS` |
| 50 | [Sagas](50-sagas.md) | compensable steps, the comp-log, the reverse pass, terminal states | `WGL-SAGA` |
| 60 | [Event log](60-event-log.md) | lifecycle entries, handler-emitted events, the pull-and-ack feed | `WGL-EVT` |
| 70 | [Control-plane API](70-api.md) | the gRPC contract, every RPC, error mapping, the portal's HTTP surface | `WGL-API` |
| 80 | [Storage](80-storage.md) | the schema, migrations, dialects, claim mechanics, retention | `WGL-STOR` |
| 85 | [Sharding](85-sharding.md) *(proposed)* | shard-carrying ids, the topology and shard roles, read replicas, adding shards, the portal in the server, the auth shard, search shards, dropping the coordinator | `WGL-SHARD` |
| 90 | [Operations](90-ops.md) | configuration, cluster and leadership, portal, TLS, deployment; the withdrawn coordinator | `WGL-OPS`, `WGL-COORD` (withdrawn) |

## 1. Scope

**In scope.** The durable workflow engine (the `server` module), the authoring and worker library
(`client`), the wire contract (`proto`), storage
(`jdbc`, `postgres`), leader election (`election`), the runnable distribution (`dist`), and the
portal the server serves (`console`).

**Out of scope.** The Go and Python client libraries (separate repositories; they implement the same
wire contract), the example and benchmark
programs (`example`), and the site generator (separate repository).

## 2. Requirement language

| Word | Meaning |
|---|---|
| **MUST** | An absolute requirement. A build that does not do this is non-conforming. |
| **MUST NOT** | An absolute prohibition. |
| **SHOULD** | Recommended; a deployment may have reasons to differ, and the consequence is stated. |
| **MAY** | Optional, at the caller's or deployer's discretion. |

Each requirement has a stable id (`WGL-<AREA>-<n>`). Ids are never reused: a withdrawn requirement
is marked *withdrawn* and kept.

**WGL-GEN-001** (MUST) Every normative requirement in this suite is either verified by a named test
in this repository or explicitly marked *unverified*.

A *Verified by* line names test **classes**, not paths: they live either under `client/src/test` or in
the cross-module `tests` module, and a class is easiest found by name (`./gradlew test --tests
'*VersioningTest'`).

**WGL-GEN-002** (MUST) Where this suite and a document under `docs/` disagree, this suite wins for
behaviour, and the disagreement is recorded in [§6](#6-known-documentation-drift).

## 3. Terminology

| Term | Meaning |
|---|---|
| **workflow** | A named, versioned topology: nodes and the edges between them. Pure data; holds no code. |
| **definition** (`WorkflowDefinition`) | The compiled, immutable form of a workflow: `(name, version, startNode, nodes, queues, executionMode, checkpoints)`. |
| **node** | One vertex of the graph, of one `NodeKind`. Carries a name, an activity, a queue, a retry policy, and edges. |
| **step** | A worker-executed node (`TASK` or `PREDICATE`), and by extension the handler method that serves it. |
| **activity** | The dispatch key a handler binds to: `<workflow>#<step name>` for worker steps. |
| **instance** | One run of a workflow. A row in `wf_instance` plus its tokens. |
| **context** | The instance's JSON state. Opaque to the engine. |
| **token** | One unit of execution positioned on a node of the graph. A row in `wf_token`. |
| **queue** | A plain string label on a worker step; the routing key a worker polls for. |
| **lease** | A time-bounded, owner-stamped claim on a token. Only the owner may report or fail it. |
| **worker** | A process that polls for tasks, runs handlers, and reports results. Holds no durable state. |
| **submitter** | A process that only starts instances. Needs the workflow name and the context shape, nothing else. |

## 4. Architecture in one paragraph

A workflow is compiled by the client into a graph and registered with the server, which stores it
as node and edge rows. Starting an instance writes an instance row and one token at the start node.
The server drives tokens over the graph until each parks on something external: a worker step
(`READY`), a timer (`WAITING`), a signal or child instance (`AWAITING`), or a join barrier
(`JOINED`). Workers long-poll over gRPC, claim `READY` tokens for the queues they serve under a
lease, run the bound handler, and report the result; the server applies it and drives on. A single
elected leader per cluster runs the clock-driven duties (timers, signal deadlines, schedules, lease
reclaim, retention). Every mutation for one instance is serialised by an
instance write-lock, so any number of server nodes over one database may drive the same instance.

## 5. Conformance

**WGL-GEN-003** (MUST) A conforming **server** implements every RPC of
`com.wiggle.proto.WiggleControlPlane` with the semantics in [chapter 70](70-api.md), and the engine
semantics of [chapter 30](30-engine.md).

**WGL-GEN-004** (MUST) A conforming **client library** may implement any subset of the RPCs, but
whatever it implements MUST follow chapter 70, and — where it mints or reads instance ids — MUST
pass the shared fixtures in `conformance/shard-ids-v1.json`
([WGL-SHARD-165](85-sharding.md#15-dropping-the-coordinator)).
*Verified by:* `core/ShardIdsConformanceTest`.

**WGL-GEN-005** (MUST) A conforming **worker** implements the poll/report/fail/heartbeat contract of
[chapter 20](20-worker.md), including honouring the server-resolved execution mode on each task
activation and the backpressure hint on an empty poll.

**WGL-GEN-006** (SHOULD) A client library that compiles topologies locally SHOULD produce
byte-identical definition JSON — and therefore the same fingerprint — for the same topology, so a
graph authored in one language re-registers as a no-op from another.

## 6. Known documentation drift

Recorded because a reader of the repository will hit them; each is a doc defect, not a behaviour
requirement.

| Where | Claim | Reality |
|---|---|---|
| `README.md` | `new Worker(...).register(orders).registerHandler(...)` | `Worker` has no `register(FlowSpec)`; a topology is published with `WiggleClient.register(spec)`, and a worker fetches the graph by name at `start()` ([WGL-WRK-020](20-worker.md)). |
| `docs/builder-pipeline-spec.md` §3.7 | the version is a content hash (`contentVersion`) | the version is declared by the author and validated positive; the content hash is a separate *fingerprint* used only to hold a published version immutable ([WGL-AUTH-040](10-authoring.md)). |
| `docs/builder-pipeline-spec.md` §7 | a combine is "a TASK whose `itemsKey` is a JSON list of arm names" | a combine is a TASK carrying `armNames` (fork) or `collectKey` (forEach); `itemsKey` belongs to `DYN_FORK` ([WGL-AUTH-022](10-authoring.md)). |
| `docs/saga-compensation.md` §7 | `cancel(id, reason, compensate=true)` | not implemented; `CancelInstanceRequest` has no such field and cancellation never compensates ([WGL-SAGA-030](50-sagas.md)). |
| `client/…/WiggleFlow.java` javadoc | a "task step named directly" form, linked as `thenApply(String, Class)` | no such public overload exists; only combines, sleeps, signals and sub-flows take a bare name ([WGL-AUTH-061a](10-authoring.md)). |
| `README.md` "Topology without handlers" | implies steps can be authored by name | they are authored through a **contract interface the author need not implement**, which is the mechanism that story describes. |
| `README.md` / `docs/onboarding.md` | `WIGGLE_EXECUTION_MODE` as a server-wide default | the server does not read it; `DEFAULT` hard-resolves to `SERVER` and only the example benchmark reads the variable ([WGL-OPS-004](90-ops.md)). |
| `docs/local-execution.md` | `AdvanceRun` / `CompleteTask` RPCs | collapsed into one `ReportSteps` RPC; the document says so in its own header note ([WGL-API-040](70-api.md)). |

## 7. Non-goals

Stated so a reader does not look for them.

- **No exactly-once execution.** Dispatch is exactly-once; execution is at-least-once. Handlers must
  be idempotent.
- **No workflow-code determinism model.** The workflow is data the server walks, not code replayed
  to rebuild state, so there is no replay discipline, no history API, and no side-effect wrappers.
- **No authorization unless asked for.** With `WIGGLE_GRPC_AUTH` unset, any peer that can connect
  may call any control-plane RPC; set it to `enforce` for per-RPC authorization by API key or client
  certificate ([chapter 70 §11](70-api.md#11-per-rpc-authorization)).
- **No rollback by default.** A failed instance stops where it is unless the topology declares
  compensators.
- **No context schema management.** The context is opaque JSON; evolving it is the handler's job via
  `@Decode`.
- **No broker.** Queues are labels on nodes, not queue infrastructure.
