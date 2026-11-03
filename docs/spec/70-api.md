# 70 — Control-plane API

← [Event log](60-event-log.md) · [Index](00-index.md) · Next: [Storage](80-storage.md)

One set of JSON methods served over HTTP is the whole contract between a server and everything else:
submitters, workers, and the clients in other languages.
Nothing is ever pushed to a worker, so workers need no inbound connectivity.

[§13](#13-messages) is the definition of record of every message, and `com.wiggle.core.wire.ControlPlane`
lists the methods. This chapter specifies each RPC's behaviour, the shapes that carry meaning, and the
error mapping.

## 1. General rules

**WGL-API-001** (MUST) The transport MUST be HTTP/1.1 or HTTP/2 on one port (default 8080): h2 negotiated
over TLS, h2c by upgrade in plaintext, where the request that asks for the upgrade MAY be as large as any
other. It is plaintext unless TLS is configured ([WGL-OPS-060](90-ops.md)). The Java client speaks
HTTP/1.1 unless `WIGGLE_HTTP_VERSION=HTTP_2`. Every RPC MUST be a `POST /v1/<Method>` whose body is the
request message and whose `200` answer is the response message, both `application/json` objects
([§13](#13-messages)); an empty body is an empty message. A request body over 4 MiB MUST be refused
with `413`.

**WGL-API-002** (MUST) A workflow definition MUST travel as one opaque JSON object holding the
definition's JSON, rather than being re-modelled as messages. Contexts and payloads MUST travel as they
are, so any JSON value — object, array or scalar — is carried, and a whole number keeps every digit.

**WGL-API-003** (MUST) A failed call MUST answer a non-2xx status with a JSON body
`{"code": ..., "message": ...}`. Engine statuses MUST map exactly: 400 → `400 INVALID_ARGUMENT`,
404 → `404 NOT_FOUND`, 409 → `409 FAILED_PRECONDITION`, anything else → `500 INTERNAL`
(infrastructure failures excepted: WGL-API-007). An `IllegalArgumentException` from the definition
layer MUST map to `400 INVALID_ARGUMENT`. An unknown method MUST answer `404`, and any method but
`POST` `405`.

**WGL-API-004** (MUST) Long-polling RPCs (`PollTasks`, `PollEvents`) MUST clamp the caller's requested
wait to the server's maximum.

**WGL-API-005** (MUST) A `PollTasks` interrupted because its server is stopping MUST NOT claim work,
and neither may one whose worker has called `StopPolling`. A worker that disconnects without it while its
poll is parked is not seen: what the poll then claims returns to `READY` when its lease expires.

**WGL-API-006** (*withdrawn*, by [§11](#11-per-rpc-authorization)) There MUST be no per-RPC
authorization. TLS (optionally mTLS) authenticates the channel; any trusted peer may call any RPC.

**WGL-API-007** (MUST) A storage failure that applied nothing — a `StorageException` classified
`TRANSIENT`, see [WGL-STOR-080](80-storage.md) — MUST map to `503 UNAVAILABLE`, which is what makes it
retryable by [WGL-WRK-100](20-worker.md). Every other storage failure, an unknown commit (`AMBIGUOUS`)
above all, MUST map to `500 INTERNAL`: the work may be durable, and the caller MUST NOT be told it can
simply re-send.

**WGL-API-008** (MUST NOT) Neither status's description may carry the exception's text: SQL, table and
constraint names, driver codes, host and schema all leak through it. The client MUST get a short
correlation id and the server log MUST get the detail.

*Verified by:* `tests/ApiErrorMappingTest`, `tests/TlsTest`, `tests/StorageFailureStatusTest`.

## 2. Health and cluster

| RPC | Request → Response |
|---|---|
| `HealthCheck` | `Empty` → `HealthStatus{status, node, leader}` |
| `GetCluster` | `Empty` → `ClusterView{self, leader, members[]}` |

**WGL-API-010** (MUST) `GetCluster` MUST report each member's id, name, first and last heartbeat, worker
count, leader flag and liveness.

## 3. Definitions

| RPC | Request → Response |
|---|---|
| `ListWorkflows` | `Empty` → `WorkflowNames` |
| `RegisterWorkflow` | `WorkflowDefinition{definition, force}` → `RegisterWorkflowResult{name, version, nodes}` |
| `GetWorkflow` | `GetWorkflowRequest{name, version?}` → `WorkflowDefinition` |

**WGL-API-020** (MUST) `RegisterWorkflow` MUST apply the registration rules of
[WGL-AUTH-103](10-authoring.md): unknown version stored, identical graph a no-op, changed graph a
`FAILED_PRECONDITION`.

**WGL-API-021** (MUST) `force` MUST be refused with `FAILED_PRECONDITION` unless the server is configured
to allow graph replacement, and the refusal MUST name the variable that would permit it.

**WGL-API-022** (MUST) A definition naming the removed `OBSERVED` mode MUST be refused with
`INVALID_ARGUMENT`.

**WGL-API-023** (MUST) `GetWorkflow` without a version MUST return the latest; with one, that exact
version, or `NOT_FOUND`.

## 4. Instances

| RPC | Request → Response |
|---|---|
| `StartInstance` | `StartInstanceRequest{workflow, context, version?, correlationId?}` → `StartInstanceResult{instanceId, workflow}` |
| `ListInstances` | `ListInstancesRequest{workflow?, status?, limit, correlationId?}` → `InstanceList` |
| `GetInstance` | `InstanceIdRequest` → `InstanceDetail{instance, tokens[]}` |
| `CancelInstance` | `CancelInstanceRequest{instanceId, reason}` → `CancelInstanceResult` |
| `SignalInstance` | `SignalRequest{instanceId, signal, payload}` → `Ack` |

**WGL-API-030** (MUST) A submitter's whole contract MUST be the workflow **name** plus the agreed context
shape. No `FlowSpec`, shared jar or handler is required to start an instance.

**WGL-API-031** (MUST) An absent version MUST mean latest-at-start; a present version MUST pin the graph.
An unregistered workflow MUST be `NOT_FOUND`.

**WGL-API-032** (MUST) `correlationId` MUST be a business key stored on the instance and MUST be
queryable via `ListInstances.correlationId`, returning matches newest first.

**WGL-API-033** (MUST) `InstanceView` MUST carry id, workflow, version, status, termination reason,
error, context, creation and update times.

**WGL-API-034** (MUST) A token on the wire MUST carry id, node id, kind, status, activity, attempt,
`availableAt`, lease owner, last error, and — when reported — `startedAt`/`finishedAt`.

**WGL-API-035** (MUST) `SignalInstance` MUST apply [WGL-ENG-070](30-engine.md): the instance must already
be waiting on that name, and an early delivery is a `FAILED_PRECONDITION` the sender may retry.

*Verified by:* `tests/FindByCorrelationTest`, `tests/SignalTest`.

## 5. Schedules

| RPC | Request → Response |
|---|---|
| `CreateSchedule` | `CreateScheduleRequest{workflow, everyMillis \| cron, context}` → `ScheduleView` |
| `ListSchedules` | `Empty` → `ScheduleList` |
| `DeleteSchedule` | `ScheduleIdRequest` → `Ack` |

**WGL-API-040** (MUST) The cadence MUST be a `oneof`: a fixed interval in millis, or a five-field cron
expression evaluated in UTC.

**WGL-API-041** (MUST) `CreateSchedule` MUST be an upsert keyed on workflow name
([WGL-ENG-084](30-engine.md)).

**WGL-API-042** (MUST) `ScheduleView` MUST carry id, workflow, `everyMillis` (0 when cron-based), `cron`
(empty when interval-based), next fire time and creation time.

## 6. Work distribution

| RPC | Request → Response |
|---|---|
| `PollTasks` | `PollRequest{workerId, queues[], max, leaseMillis, waitMillis, versions[]}` → `TaskList{tasks[], retryAfterMillis}` |
| `ReportSteps` | `ReportStepsRequest{runs[]}` → `ReportStepsResult{results[]}` |
| `FailTask` | `TaskFailureRequest{taskId, leaseOwner, message, retryable}` → `Ack` |
| `HeartbeatTask` | `HeartbeatRequest{taskId, leaseOwner, extendMillis}` → `HeartbeatResult{leaseExpiresAt}` |
| `StopPolling` | `StopPollingRequest{workerId}` → `StopPollingResult{stopped}` |
| `GetBacklogCoverage` | `BacklogCoverageRequest{max}` → `BacklogCoverage{slices[], livePollers}` |

**WGL-API-050** (MUST) An empty `versions` list MUST mean "serve every version"; a non-empty one MUST
restrict the claim to those `(workflow, version)` pairs.

**WGL-API-051** (MUST) `max` of 0 MUST be treated as 1.

**WGL-API-052** (MUST) `TaskActivation` MUST carry task id, instance id, workflow, version, node id, step
name, activity, kind, attempt, lease expiry, lease owner, context, the **server-resolved** execution
mode, and, for a forEach item step, the base context, the item index and the item's source map key.

**WGL-API-053** (MUST) `TaskList.retryAfterMillis` MUST be a backpressure hint: when positive the server
is shedding load and the worker MUST hold off that long instead of re-polling immediately. The value MUST
already include per-response jitter.

**WGL-API-054** (MUST) `ReportSteps` MUST be the only RPC that reports finished work, and MUST answer one
`RunOutcome` per submitted run, in submission order.

**WGL-API-055** (MUST) `RunApplied.instanceStatus` MAY be any instance status, not only the terminal four;
a client MUST treat anything but `RUNNING` as "stop here".

**WGL-API-056** (MUST) `RunApplied` MUST carry the renewed lease expiry and the next task id when a
continuation was leased back, and a blank next task id when nothing was.

**WGL-API-057** (MUST) `RunOutcome.errorStatus` MUST be 0 when the run applied, and otherwise the status
the run was refused under, with the message in `error`; the call itself MUST still succeed so a batch can
say "A applied, B did not".

**WGL-API-058** (MUST) `StepResult` MUST carry a node id, exactly one of `merge` / `predicateValue` /
`error`, optional `startedAt`/`finishedAt` (0 = not reported), and the events emitted while it ran.

**WGL-API-059** (MUST) `GetBacklogCoverage` MUST report dispatchable work grouped by
`(workflow, version, queue)` with the ready count, the oldest availability time, and a `covered` flag
that is false when no worker polling **this node** serves that queue at that version. `livePollers` MUST
be this node's count.

**WGL-API-060** (MUST) `StopPolling` MUST end every `PollTasks` in progress for `workerId` on the node
that serves it: each returns at once with what it had already claimed, which is nothing unless it claimed
before the call, and claims no more. `stopped` MUST be how many there were. It does not stop the worker
from polling again.

*Verified by:* `tests/BacklogCoverageTest`, `server/engine/AdvanceManyTest`, `tests/MemoryPollTest`,
`server/engine/StopPollingTest`, `tests/WorkerCloseTest`.

## 7. Step statistics

`ObserveRun`, `ObserveMany` and `ListAnomalies` went with OBSERVED execution; their messages are gone
and `StepResult.error` (field 4) is reserved.

| RPC | Request → Response |
|---|---|
| `GetStepStats` | `StepStatsRequest{workflow, version, since, sample}` → `StepStats{workflow, version, nodes[]}` |

**WGL-API-070** (MUST) Version 0 MUST mean the latest registered version, and `sample` of 0 a server
default.

**WGL-API-071** *Withdrawn with OBSERVED execution.*

**WGL-API-072** *Withdrawn with OBSERVED execution.*

**WGL-API-073** (MUST) `NodeStats` MUST carry node id, name, count, mean, p50, p95, max, and queue-wait
p50/p95 (0 for steps that were never queued).

## 8. Event feed

| RPC | Request → Response |
|---|---|
| `PollEvents` | `PollEventsRequest{consumer, max, waitMillis, startFrom}` → `EventList{events[], retryAfterMillis}` |
| `AckEvents` | `AckEventsRequest{consumer, ackedSeq}` → `Ack` |

**WGL-API-080** (MUST) Both RPCs MUST be implemented by the server with the semantics of
[chapter 60](60-event-log.md).

**WGL-API-081** (MUST) `EventList.retryAfterMillis` MUST carry the same backpressure hint as `TaskList`.

## 9. Java client surface

**WGL-API-090** (MUST) `WiggleClient` MUST be constructible from a target, optionally with TLS options and
a flag requiring TLS, and MUST strip a `scheme://` prefix from the target.

**WGL-API-091** (MUST) The client MUST expose at least: `register(spec[, force])`, `start` (by spec or by
name, with optional version and correlation id), `instance`, `instanceDetail`, `listInstances`,
`findByCorrelation`, `awaitCompletion(id, timeout)`, `cancel`, `signal`, `workflowNames`, `getWorkflow`,
`createSchedule`, `createCronSchedule`, `schedules`, `deleteSchedule`, `poll`, `reportSteps`, `fail`,
`heartbeat`, `backlogCoverage`, `stepStats`, `pollEvents`, `ackEvents`, `cluster`.

**WGL-API-092** (MUST) `WiggleApiException` MUST carry the engine status code and MUST distinguish client
errors (4xx) from others.

**WGL-API-093** (MUST) A single-run `reportSteps` MUST read the one answer and raise
`WiggleApiException` with that status, so an ordinary one-step report reads as a plain call.

**WGL-API-094** (MUST) `WiggleConnection` MUST offer `direct(target[, tls])` for one cluster.

**WGL-API-095** (MUST) `register` MUST raise `WorkflowRegistrationException` carrying the server's
explanation when a registration is refused.

*Verified by:* `tests/ScheduleClientTest`, `tests/VersioningTest`, `tests/RpcRetryFailoverTest`.

## 10. The portal's HTTP surface

A server node serves these on `WIGGLE_PORTAL_PORT`, apart from the API port
([WGL-SHARD-170](85-sharding.md#12-the-portal-in-the-server)), through the engine in process.

**WGL-API-100** (MUST) The JSON API MUST be exactly:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/auth` | who am I, and my permissions |
| `POST` | `/api/login` · `/logout` · `GET /login` | session cookie login and the browser form |
| `GET` | `/healthz` | a probe on the portal port |
| `GET` · `POST` | `/setup` · `/api/setup` | first run only: the screen that sets the admin's password, and the call that does ([WGL-OPS-051](90-ops.md)); 409 once any account exists |
| `GET` | `/api/cluster` | cluster view |
| `GET` | `/api/workflows` · `/api/workflows/{name}` | names, and one compiled graph as JSON |
| `GET` | `/api/instances` | list, filtered by workflow/status/limit or searched by instance or correlation id |
| `GET` | `/api/instances/{id}` | instance with its tokens; a token inside a fork, forEach or created branches also carries `scope` (the token that fanned it out), `scopeKind` (`arm` or `item`), `scopeIndex` and, when keyed, `scopeKey` |
| `POST` | `/api/instances/{id}/cancel` | cancel |
| `POST` | `/api/instances/{id}/signal/{name}` | deliver a signal; the JSON body merges into the context |
| `GET` | `/api/signals` | signal waits pending delivery |
| `GET` | `/api/backlog` | backlog coverage, with uncovered slices and stranded task counts |
| `GET` | `/api/stats` | per-step duration statistics |
| `GET`/`POST`/`DELETE` | `/api/schedules[/{id}]` | list, create (interval or cron), delete |
| `GET`/`POST`/`DELETE` | `/api/users[/{name}]` | managed accounts: list, create with roles, delete |
| `POST` | `/api/users/{name}/password` · `/roles` · `/disabled` | reset a password, replace roles, disable or enable |
| `GET`/`POST`/`DELETE` | `/api/roles[/{name}]` | roles: list with the known actions, create or replace, delete |
| `GET` | `/api/audit?after=&limit=` | changes to accounts, roles and sessions, oldest first |
| `GET` | `/api/search?q=&workflow=&status=&limit=&partial=&attr=` | full-text search over instances ([§12](#12-search)); `attr=name:op:value` (repeatable; `op` is `eq`, `lt`, `le`, `gt` or `ge`) filters on an attribute the `workflow` declares, 400 for an undeclared one or a value of the wrong type; each hit carries the instance's `attributes`; 404 when search is off |
| `POST` | `/api/password` | change one's own password |

**WGL-API-101** (MUST) An unknown `/api/*` path MUST be 404; a mutating call on a GET-only endpoint MUST
be 405.

**WGL-API-102** (MUST) Every `/api/*` call except `/api/password` MUST need a permission: `read`
for a read, `user.manage` for users, roles and the audit, `instance.cancel`, `instance.signal` or
`schedule.write` for those writes, scoped to the workflow they touch, and `*` for any other write.
A call without it is 403. A viewer is therefore refused every write.

**WGL-API-103** (MUST) The backend MUST sit behind one neutral seam (`DashboardData`) carrying no engine
or storage types, so the SPA's JSON does not follow engine or storage changes.

**WGL-API-104** *Withdrawn: the cell coordinator was removed ([chapter 85 §15](85-sharding.md#15-dropping-the-coordinator)).*

**WGL-API-105** (MUST) A failed `/api/*` call MUST answer with the engine's status (400, 404, 409)
when the engine refused it, 404 for a retired shard, 503 for a storage failure that applied nothing,
and 500 otherwise. A 503 or 500 MUST NOT carry the cause's message, which can hold SQL or host
names; it carries a reference that the node's log pairs with the full cause.

*Verified by:* `console/ConsoleWebTest`, `console/ConsoleDataTest`, `console/ConsoleUsersTest`,
`console/PortalTest`.

## 11. Per-RPC authorization

Who a call is, and what it may do ([WGL-SHARD-187](85-sharding.md#13-users-and-authorization)).
Off by default, so a deployment that sets nothing behaves as before.

**WGL-API-110** (MUST) `WIGGLE_API_AUTH` MUST select the mode: `off` (default) reads no credential
and allows every call; `log` checks every call, serves it anyway, and logs each distinct refusal
`enforce` would make once; `enforce` refuses.

**WGL-API-111** (MUST) A caller MUST identify itself with an API key, sent as
an `Authorization: Bearer <key>` header, or, on an mTLS listener, with its client certificate, whose
RFC 2253 subject names a certificate credential. A key wins when both are present. Credentials are
created in the portal and hold one role ([WGL-SHARD-181](85-sharding.md#13-users-and-authorization)).

**WGL-API-112** (MUST) Under `enforce`, a call with no credential, an unknown one or an expired one
MUST fail `UNAUTHENTICATED`, and a call its role does not allow MUST fail `PERMISSION_DENIED`, before
the RPC reads anything. `HealthCheck` MUST need no credential.

**WGL-API-113** (MUST) Each RPC MUST need this permission:

| RPC | Permission |
|---|---|
| `HealthCheck` | none |
| `GetCluster` | `read` |
| `ListWorkflows`, `GetBacklogCoverage`, `ListSchedules` | `read` on any scope; only the workflows it may read are listed |
| `GetWorkflow`, `GetStepStats` | `read:<workflow>` |
| `ListInstances` | `read:<workflow>` when it names a workflow; `read` on any scope by correlation id, listing only the workflows it may read; else `read` |
| `GetInstance` | `read:<the instance's workflow>` |
| `RegisterWorkflow` | `workflow.register:<workflow>`, `workflow.register:<queue>` for each queue its steps run on, and `instance.start:<child>` for each sub-workflow node |
| `StartInstance` | `instance.start:<workflow>` |
| `CancelInstance`, `SignalInstance` | `instance.cancel` / `instance.signal` `:<the instance's workflow>` |
| `CreateSchedule`, `DeleteSchedule` | `schedule.write:<workflow>` |
| `PollTasks` | `task.poll:<queue>` for each queue it names; `task.poll` when it names none (every queue) |
| `StopPolling` | `task.poll` on any scope |
| `ReportSteps`, `FailTask`, `HeartbeatTask` | `task.poll:<the task's queue>`; a task on a queue the caller may not poll is answered as not found (a run in `ReportSteps` gets 404) |
| `PollEvents`, `AckEvents` | `event.read` |
| `SearchInstances` | `read` on any scope; the hits are narrowed to the workflows it may read |

**WGL-API-116** (MUST) A scope ending in `.*` MUST cover every name that starts with what precedes
the `*`, dot included: `read:acme.*` covers `acme.orders` and `acme.eu.orders`, but not `acme` or
`acmex.orders`. A scope MUST hold no other `*`, and `.*` alone is not a scope. A tenant whose
workflows and queues all start with its slug is isolated by its credentials' prefix scopes; the
engine has no tenant of its own. `event.read` takes no scope, so a tenant's role should not hold it.

**WGL-API-114** (MUST) A credential MUST be resolved through the node's cache
([WGL-SHARD-184](85-sharding.md#13-users-and-authorization)), so a call reads the auth shard only on
a miss; a deleted credential, or one whose role changed, MUST be refused on calls made more than a
second after the change. A call already running, such as an open long poll, finishes as it began. With the auth shard unreachable and the credential not cached, the call MUST fail
`UNAVAILABLE`.

**WGL-API-115** (MUST) The Java client MUST send the key in `WIGGLE_API_KEY` (or `-Dwiggle.api.key`) on
every call when one is set, and MUST report `UNAUTHENTICATED` as 401 and `PERMISSION_DENIED` as 403.

*Verified by:* `server/api/ApiAuthTest` (every RPC of the service is checked, so one added without
a permission fails it; two tenants on prefix scopes), `tests/TlsTest`, `server/auth/AccountsTest`,
`server/auth/AuthCacheTest`, `server/auth/PermissionsTest`.

## 12. Search

Full-text search over instances, served by the search shards
([chapter 85 §14](85-sharding.md#14-search-shards)).

**WGL-API-120** (MUST) `SearchInstances` MUST return the instances whose correlation id, context,
termination reason or error contain every word of `text` (an empty `text` matches all), filtered by
`workflow`, `status` and the bounds on when the instance last changed, best first, at most `limit`
(default 20, at most 1000). With search off it MUST fail `FAILED_PRECONDITION`.

**WGL-API-121** (MUST) A hit MUST carry the instance's identity and status as last indexed, its
score, and `purged` when the instance itself is gone.

**WGL-API-122** (MUST) The hits MUST be limited, inside each shard's query, to the workflows the
caller may read ([§11](#11-per-rpc-authorization)); asking for a workflow it may not read finds
nothing rather than failing.

**WGL-API-123** (MUST) A search shard that does not answer MUST fail the search `UNAVAILABLE`, unless
`partialOk` asks for the hits of the shards that did, with `partial` set.

**WGL-API-124** *Withdrawn with vector search.* `SearchRequest.semantic` and `SearchResult.model` (field
numbers 8 and 3) are reserved.

**WGL-API-125** (MUST) `SearchRequest.attributes` MUST keep only the instances whose search attributes
([WGL-AUTH-094](10-authoring.md)) satisfy every filter, together with the other conditions. A filter
names an attribute and compares it with `EQUAL`; a numeric one may also use `LESS_THAN`,
`LESS_THAN_OR_EQUAL`, `GREATER_THAN` or `GREATER_THAN_OR_EQUAL`. A value is text, an integer, a decimal
or a flag, and matches an attribute of that kind only; numbers compare by value, so `5` equals `5.0`.
An instance that lacks the attribute does not match. An empty `text` is allowed, to filter on the
attributes alone.

**WGL-API-126** (MUST) A search with attribute filters MUST name a `workflow`, since attributes are
declared per flow, and MUST carry at most 8 filters; otherwise it fails `INVALID_ARGUMENT` (400), as it
does for an unknown operator, a value of the wrong kind for the operator, or a missing value. The
filters are applied inside each shard's query, on a value index, after the scoping of WGL-API-122.

**WGL-API-127** (MUST) An instance's attributes MUST be those its flow's version declared, read from
its context as last indexed, so they follow the context and are replaced, not merged, each time.
Instances indexed before a version declared an attribute do not carry it.

*Verified by:* `server/search/SearchEndToEndTest`, `server/search/SearchIndexTest`,
`server/store/StorageContract` (search chapter, on every backend), `postgres/PostgresTopologyTest`.

## 13. Messages

Every message is a JSON object. A field that is absent reads as its type's zero value: `""`, `0`,
`false` or an empty list; a field marked *optional* is told apart from that value by being absent.
"One of" fields are alternatives: a message sets exactly one of them. A field of the wrong type is
`400 INVALID_ARGUMENT`. Fields a server does not know are ignored. "Any JSON" is any JSON value;
an integer is a JSON number with no fraction.

| Method | Request | Response |
|---|---|---|
| `HealthCheck` | `Empty` | `HealthStatus` |
| `GetCluster` | `Empty` | `ClusterView` |
| `ListWorkflows` | `Empty` | `WorkflowNames` |
| `GetBacklogCoverage` | `BacklogCoverageRequest` | `BacklogCoverage` |
| `RegisterWorkflow` | `WorkflowDefinition` | `RegisterWorkflowResult` |
| `GetWorkflow` | `GetWorkflowRequest` | `WorkflowDefinition` |
| `StartInstance` | `StartInstanceRequest` | `StartInstanceResult` |
| `ListInstances` | `ListInstancesRequest` | `InstanceList` |
| `GetInstance` | `InstanceIdRequest` | `InstanceDetail` |
| `CancelInstance` | `CancelInstanceRequest` | `CancelInstanceResult` |
| `SignalInstance` | `SignalRequest` | `Ack` |
| `CreateSchedule` | `CreateScheduleRequest` | `ScheduleView` |
| `ListSchedules` | `Empty` | `ScheduleList` |
| `DeleteSchedule` | `ScheduleIdRequest` | `Ack` |
| `PollTasks` | `PollRequest` | `TaskList` |
| `ReportSteps` | `ReportStepsRequest` | `ReportStepsResult` |
| `FailTask` | `TaskFailureRequest` | `Ack` |
| `HeartbeatTask` | `HeartbeatRequest` | `HeartbeatResult` |
| `StopPolling` | `StopPollingRequest` | `StopPollingResult` |
| `PollEvents` | `PollEventsRequest` | `EventList` |
| `AckEvents` | `AckEventsRequest` | `Ack` |
| `GetStepStats` | `StepStatsRequest` | `StepStats` |
| `SearchInstances` | `SearchRequest` | `SearchResult` |

### SearchRequest

| Field | Type | Notes |
|---|---|---|
| `text` | string | Every word must occur in the instance's correlation id, context, termination reason or error; empty matches every instance the filters allow. |
| `workflow` | string | *optional* |
| `status` | string | *optional* |
| `updatedFrom` | integer | Bounds on when the instance last changed (epoch millis); 0 = open. |
| `updatedTo` | integer |  |
| `limit` | integer |  |
| `partialOk` | boolean | Answer from the search shards that respond when one does not, and say so in `partial`. |
| `attributes` | list of `SearchAttributeFilter` | Keep the instances whose declared search attributes satisfy every filter. Attributes are declared per flow, so these need `workflow`. |

### SearchAttributeFilter

| Field | Type | Notes |
|---|---|---|
| `name` | string |  |
| `op` | string |  |
| `value` | `CriteriaLiteral` |  |

### SearchHit

| Field | Type | Notes |
|---|---|---|
| `instanceId` | string |  |
| `workflow` | string |  |
| `version` | integer |  |
| `status` | string |  |
| `correlationId` | string |  |
| `createdAt` | integer |  |
| `updatedAt` | integer |  |
| `score` | number |  |
| `purged` | boolean | The instance itself has been purged; its document outlives it until its own retention. |

### SearchResult

| Field | Type | Notes |
|---|---|---|
| `hits` | list of `SearchHit` |  |
| `partial` | boolean |  |

### Empty

No fields: `{}`.

### Ack

| Field | Type | Notes |
|---|---|---|
| `ok` | boolean |  |

### HealthStatus

| Field | Type | Notes |
|---|---|---|
| `status` | string |  |
| `node` | string |  |
| `leader` | boolean |  |

### ClusterMember

| Field | Type | Notes |
|---|---|---|
| `id` | string |  |
| `name` | string |  |
| `firstHeartbeat` | integer |  |
| `lastHeartbeat` | integer |  |
| `workers` | integer |  |
| `leader` | boolean |  |
| `alive` | boolean |  |

### ClusterView

| Field | Type | Notes |
|---|---|---|
| `self` | string |  |
| `leader` | boolean |  |
| `members` | list of `ClusterMember` |  |

### WorkflowDefinition

| Field | Type | Notes |
|---|---|---|
| `definition` | object |  |
| `force` | boolean | RegisterWorkflow only: replace an already-registered (name, version) whose graph differs, instead of failing. The server refuses unless it is configured to allow it. |

### WorkflowNames

| Field | Type | Notes |
|---|---|---|
| `workflows` | list of string |  |

### RegisterWorkflowResult

| Field | Type | Notes |
|---|---|---|
| `name` | string |  |
| `version` | string |  |
| `nodes` | integer |  |

### GetWorkflowRequest

| Field | Type | Notes |
|---|---|---|
| `name` | string |  |
| `version` | integer | *optional*; Absent = the latest registered version. A worker that binds handlers for one specific version asks for that version, so its signatures are checked against the graph it will actually serve. |

### WorkflowVersion

| Field | Type | Notes |
|---|---|---|
| `workflow` | string |  |
| `version` | integer |  |

### BacklogCoverageRequest

| Field | Type | Notes |
|---|---|---|
| `max` | integer | 0 = a server default |

### BacklogSlice

| Field | Type | Notes |
|---|---|---|
| `workflow` | string |  |
| `version` | integer |  |
| `queue` | string |  |
| `readyCount` | integer |  |
| `oldestAvailableAt` | integer |  |
| `covered` | boolean | False = no worker polling this node serves this queue at this version, so these tokens sit dispatchable and unclaimed. True for both COVERED and CONDITIONAL. |
| `coverage` | string | COVERED: a live worker takes every such task. CONDITIONAL: only workers whose queue criteria test properties the slice does not fix (a step, a correlation id, an attribute) poll it, so some tasks may wait for none. UNCOVERED: no live worker takes any. |
| `claimable` | integer | *optional*; Exactly how many of the slice's ready tasks some live worker would claim; unset for a conditional slice too large to count fully. |
| `criteria` | list of string | The criteria a conditional slice depends on, as an operator reads them. |

### BacklogCoverage

| Field | Type | Notes |
|---|---|---|
| `slices` | list of `BacklogSlice` |  |
| `livePollers` | integer |  |

### StartInstanceRequest

| Field | Type | Notes |
|---|---|---|
| `workflow` | string |  |
| `context` | any JSON |  |
| `version` | integer | *optional* |
| `correlationId` | string | *optional* |

### StartInstanceResult

| Field | Type | Notes |
|---|---|---|
| `instanceId` | string |  |
| `workflow` | string |  |

### InstanceView

| Field | Type | Notes |
|---|---|---|
| `id` | string |  |
| `workflow` | string |  |
| `version` | integer |  |
| `status` | string |  |
| `terminationReason` | string | *optional* |
| `error` | string | *optional* |
| `context` | any JSON |  |
| `createdAt` | integer |  |
| `updatedAt` | integer |  |

### Token

| Field | Type | Notes |
|---|---|---|
| `id` | string |  |
| `nodeId` | string |  |
| `kind` | string |  |
| `status` | string |  |
| `activity` | string |  |
| `attempt` | integer |  |
| `availableAt` | integer |  |
| `leaseOwner` | string | *optional* |
| `lastError` | string | *optional* |
| `startedAt` | integer | *optional*; When the step ran on the reporting side (LOCAL_*); absent when not reported. |
| `finishedAt` | integer | *optional* |
| `input` | any JSON | *optional*; What the step ran with and returned, where the server recorded it. A task's absent output left the context unchanged; a predicate's output is its branch. Either may be a truncation marker: {"$truncated": <length>, "head": "..."}. |
| `output` | any JSON | *optional* |
| `createdAt` | integer |  |

### InstanceDetail

| Field | Type | Notes |
|---|---|---|
| `instance` | `InstanceView` |  |
| `tokens` | list of `Token` |  |

### ListInstancesRequest

| Field | Type | Notes |
|---|---|---|
| `workflow` | string | *optional* |
| `status` | string | *optional* |
| `limit` | integer |  |
| `correlationId` | string | *optional*; When set, return only instances started with this correlation id (business key lookup). |

### InstanceList

| Field | Type | Notes |
|---|---|---|
| `instances` | list of `InstanceView` |  |

### InstanceIdRequest

| Field | Type | Notes |
|---|---|---|
| `instanceId` | string |  |

### CancelInstanceRequest

| Field | Type | Notes |
|---|---|---|
| `instanceId` | string |  |
| `reason` | string |  |

### SignalRequest

| Field | Type | Notes |
|---|---|---|
| `instanceId` | string |  |
| `signal` | string |  |
| `payload` | any JSON |  |

### CreateScheduleRequest

| Field | Type | Notes |
|---|---|---|
| `workflow` | string |  |
| `everyMillis` | integer | one of `cadence`; fixed interval |
| `cron` | string | one of `cadence`; five-field cron, evaluated in UTC |
| `context` | any JSON |  |

### ScheduleView

| Field | Type | Notes |
|---|---|---|
| `id` | string |  |
| `workflow` | string |  |
| `everyMillis` | integer | 0 when cron-based |
| `cron` | string | empty when interval-based |
| `nextFireAt` | integer |  |
| `createdAt` | integer |  |

### ScheduleList

| Field | Type | Notes |
|---|---|---|
| `schedules` | list of `ScheduleView` |  |

### ScheduleIdRequest

| Field | Type | Notes |
|---|---|---|
| `id` | string |  |

### CancelInstanceResult

| Field | Type | Notes |
|---|---|---|
| `cancelled` | string |  |

### PollRequest

| Field | Type | Notes |
|---|---|---|
| `workerId` | string |  |
| `queues` | list of string |  |
| `max` | integer |  |
| `leaseMillis` | integer |  |
| `waitMillis` | integer |  |
| `versions` | list of `WorkflowVersion` | Empty = serve every version, which is the default and what an unversioned worker sends. Non-empty = claim only these (workflow, version) pairs. |
| `criteria` | list of `QueueCriteria` | The alternative to `queues`: one entry per queue served, each with its own predicate. Set one or the other, never both; a queue named twice is rejected. |

### QueueCriteria

| Field | Type | Notes |
|---|---|---|
| `queue` | string | required |
| `where` | `CriteriaPredicate` | absent = every task on the queue |

### CriteriaPredicate

| Field | Type | Notes |
|---|---|---|
| `cmp` | `CriteriaComparison` | one of `p` |
| `allOf` | `CriteriaJunction` | one of `p` |
| `anyOf` | `CriteriaJunction` | one of `p` |
| `negated` | `CriteriaPredicate` | one of `p` |

### CriteriaJunction

| Field | Type | Notes |
|---|---|---|
| `terms` | list of `CriteriaPredicate` |  |

### CriteriaComparison

| Field | Type | Notes |
|---|---|---|
| `field` | string |  |
| `op` | string |  |
| `args` | list of `CriteriaLiteral` |  |
| `attribute` | string | the declared attribute's name, iff field is ATTRIBUTE |

### CriteriaLiteral

| Field | Type | Notes |
|---|---|---|
| `text` | string | one of `v` |
| `integer` | integer | one of `v` |
| `decimal` | string | one of `v`; exact decimal text, for an attribute |
| `flag` | boolean | one of `v`; for an attribute |

### TaskActivation

| Field | Type | Notes |
|---|---|---|
| `taskId` | string |  |
| `instanceId` | string |  |
| `workflow` | string |  |
| `version` | integer |  |
| `nodeId` | string |  |
| `stepName` | string |  |
| `activity` | string |  |
| `kind` | string |  |
| `attempt` | integer |  |
| `leaseExpiresAt` | integer |  |
| `leaseOwner` | string |  |
| `context` | any JSON |  |
| `executionMode` | string | SERVER \| LOCAL_SYNC \| LOCAL_ASYNC (already resolved by the server) |
| `baseContext` | any JSON | Set only for a forEach item step: `context` is then the ITEM's current value (any JSON value, scalars included), and `baseContext` is the frozen pre-forEach context (read-only for items). |
| `itemIndex` | integer | the item's position in the input collection |
| `itemMapKey` | string | the item's source key, when the input collection was a map |
| `collectKey` | string | Set only for the combine of a forEach or of created branches: where its results are staged. A worker binds a created combine by it, since no graph it holds has the node. |
| `armNames` | list of string | Set only for a fork's combine: the arms, in fork order, that it merges. |
| `correlationId` | string | the instance's; empty when it has none |
| `instanceCreatedAt` | integer |  |

### TaskList

| Field | Type | Notes |
|---|---|---|
| `tasks` | list of `TaskActivation` |  |
| `retryAfterMillis` | integer | Backpressure hint: when > 0 the server is shedding load (it is over its memory threshold), so the worker should hold off this many millis before polling again instead of retrying immediately. The value already includes per-response jitter. |

### EmittedEvent

| Field | Type | Notes |
|---|---|---|
| `type` | string | handler-chosen; "wf." is reserved for lifecycle events |
| `payload` | any JSON | a JSON object |

### StepResult

| Field | Type | Notes |
|---|---|---|
| `nodeId` | string |  |
| `merge` | any JSON | one of `outcome`; TASK: the complete next context |
| `predicateValue` | boolean | one of `outcome`; PREDICATE: branch selector |
| `startedAt` | integer | When the step ran, epoch millis; 0 = not reported. Kept on the token for duration stats. |
| `finishedAt` | integer |  |
| `events` | list of `EmittedEvent` | emitted while this step ran, in emission order |
| `branches` | list of `CreatedBranch` | created while this step ran (Step.create), in creation order |

### CreatedBranch

| Field | Type | Notes |
|---|---|---|
| `input` | any JSON |  |
| `key` | string | optional; all-or-none across one report |
| `steps` | list of `BranchStep` | at least one |

### BranchStep

| Field | Type | Notes |
|---|---|---|
| `name` | string | the step name, signal name or sub-flow node name; blank for a sleep or fork |
| `kind` | string | TASK \| PREDICATE \| SLEEP \| SIGNAL \| SUB_WORKFLOW \| FORK |
| `compensable` | boolean |  |
| `queue` | string | blank = the creating step's queue |
| `retry` | any JSON | a retry policy as JSON; absent = the creating step's |
| `sleepMillis` | integer |  |
| `combine` | string | a TASK: creates branches, collected by this combine; a FORK: merges its arms |
| `workflow` | string | SUB_WORKFLOW: the child workflow |
| `escalation` | list of `BranchStep` | SIGNAL: run when the deadline in `sleepMillis` passes |
| `arms` | list of `BranchArm` | FORK: each arm runs on its own copy of the context |
| `body` | list of `BranchStep` | A PREDICATE with a body is a loop: the body runs, then again while the condition `name` holds. |
| `loopBudget` | integer | a loop's most iterations; 0 = the engine's default |
| `cases` | list of `BranchCase` | A PREDICATE with cases is a choice: the first case whose guard holds runs its steps. |

### BranchCase

| Field | Type | Notes |
|---|---|---|
| `guard` | string | blank: the otherwise, which must come last |
| `steps` | list of `BranchStep` |  |

### BranchArm

| Field | Type | Notes |
|---|---|---|
| `steps` | list of `BranchStep` |  |

### ReportedRun

| Field | Type | Notes |
|---|---|---|
| `taskId` | string | the currently-leased token these steps start at |
| `leaseOwner` | string |  |
| `steps` | list of `StepResult` | ordered, at least one; one step is the ordinary case |
| `final` | boolean | The worker will not take a continuation: it reached a boundary, filled its batch, or is draining. A statement about this worker, not about the workflow -- a mode that never chains hands back regardless. |
| `criteria` | list of `QueueCriteria` | The criteria the worker polled with. The server leases a continuation back only when the entry for its queue matches it, so a chain cannot run a step the worker's poll would not have claimed. |

### ReportStepsRequest

| Field | Type | Notes |
|---|---|---|
| `runs` | list of `ReportedRun` | at least one, each naming a different instance |

### RunApplied

| Field | Type | Notes |
|---|---|---|
| `instanceStatus` | string | Any InstanceStatus, not just the terminal four: a concurrent failure can leave the instance COMPENSATING between a worker's local steps. Clients treat anything but RUNNING as "stop here". See docs/state-machines.md. RUNNING \| COMPLETED \| FAILED \| CANCELLED |
| `leaseExpiresAt` | integer | \| COMPENSATING \| COMPENSATED \| COMPENSATION_FAILED renewed lease covering continued local execution |
| `nextTaskId` | string | token to reference for the next local step (blank if handed back) |

### RunOutcome

| Field | Type | Notes |
|---|---|---|
| `taskId` | string |  |
| `applied` | `RunApplied` | set when the run applied (`errorStatus` is 0) |
| `errorStatus` | integer | 0 ok; else the status this run would have thrown |
| `error` | string |  |

### ReportStepsResult

| Field | Type | Notes |
|---|---|---|
| `results` | list of `RunOutcome` | one per submitted run, in submission order |

### StepStatsRequest

| Field | Type | Notes |
|---|---|---|
| `workflow` | string |  |
| `version` | integer | 0 = the latest registered version |
| `since` | integer | epoch millis; 0 = no lower bound |
| `sample` | integer | newest settled steps considered; 0 = a server default |

### NodeStats

| Field | Type | Notes |
|---|---|---|
| `nodeId` | string |  |
| `name` | string |  |
| `count` | integer |  |
| `meanMillis` | number |  |
| `p50Millis` | integer |  |
| `p95Millis` | integer |  |
| `maxMillis` | integer |  |
| `waitP50Millis` | integer | How long the step waited to be claimed by a worker (ready -> claimed). |
| `waitP95Millis` | integer |  |

### StepStats

| Field | Type | Notes |
|---|---|---|
| `workflow` | string |  |
| `version` | integer |  |
| `nodes` | list of `NodeStats` | graph order is not implied; sort by p95 for a bottleneck view |

### TaskFailureRequest

| Field | Type | Notes |
|---|---|---|
| `taskId` | string |  |
| `leaseOwner` | string |  |
| `message` | string |  |
| `retryable` | boolean |  |

### HeartbeatRequest

| Field | Type | Notes |
|---|---|---|
| `taskId` | string |  |
| `leaseOwner` | string |  |
| `extendMillis` | integer |  |

### HeartbeatResult

| Field | Type | Notes |
|---|---|---|
| `leaseExpiresAt` | integer |  |

### StopPollingRequest

| Field | Type | Notes |
|---|---|---|
| `workerId` | string | required |

### StopPollingResult

| Field | Type | Notes |
|---|---|---|
| `stopped` | integer | the polls in progress it ended |

### PollEventsRequest

| Field | Type | Notes |
|---|---|---|
| `consumer` | string | named cursor; the first poll registers it |
| `max` | integer |  |
| `waitMillis` | integer | long-poll, clamped like PollTasks |
| `startFrom` | integer | Start position when the first poll creates the cursor; ignored once it exists. 0 = tail (only events appended from now on), -1 = earliest retained, else a seq. |

### EventView

| Field | Type | Notes |
|---|---|---|
| `seq` | integer | strictly increasing within one shard's log; gaps are possible |
| `instanceId` | string |  |
| `workflow` | string |  |
| `version` | integer |  |
| `correlationId` | string |  |
| `type` | string |  |
| `createdAt` | integer |  |
| `payload` | any JSON |  |
| `nodeId` | string | the step that emitted it; empty for a lifecycle entry |
| `shard` | integer | the shard whose log holds it; (shard, seq) identifies it |
| `cursor` | string | opaque: the consumer's place just after this entry, for AckEvents |

### EventList

| Field | Type | Notes |
|---|---|---|
| `events` | list of `EventView` |  |
| `retryAfterMillis` | integer | Same backpressure hint as TaskList: when > 0, hold off this many millis before polling again. |

### AckEventsRequest

| Field | Type | Notes |
|---|---|---|
| `consumer` | string |  |
| `ackedSeq` | integer | Cumulative: acknowledges every event with seq <= this. Only for a log on one shard; a sharded log refuses it as INVALID_ARGUMENT. |
| `ackedCursor` | string | Cumulative: the cursor of the last event handled, acknowledging it and everything before it. Takes precedence over `ackedSeq` when set. |
