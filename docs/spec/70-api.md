# 70 — Control-plane API

← [Event log](60-event-log.md) · [Index](00-index.md) · Next: [Storage](80-storage.md)

One gRPC service, `com.wiggle.proto.WiggleControlPlane`, is the whole contract between a server and
everything else: submitters, workers, observers, the console, and the clients in other languages.
Nothing is ever pushed to a worker, so workers need no inbound connectivity.

The definition of record is `proto/src/main/proto/wiggle.proto`. This chapter specifies each RPC's
behaviour, the shapes that carry meaning, and the error mapping.

## 1. General rules

**WGL-API-001** (MUST) The transport MUST be gRPC on one port (default 8080), plaintext unless TLS is
configured ([WGL-OPS-060](90-ops.md)).

**WGL-API-002** (MUST) A workflow definition MUST travel as one opaque `google.protobuf.Struct` holding
the definition's JSON, rather than being re-modelled in proto. Contexts and payloads MUST travel as
`google.protobuf.Value`, so any JSON value — object, array or scalar — is carried.

**WGL-API-003** (MUST) Engine statuses MUST map to gRPC statuses exactly: 400 → `INVALID_ARGUMENT`,
404 → `NOT_FOUND`, 409 → `FAILED_PRECONDITION`, anything else → `INTERNAL` (infrastructure failures
excepted: WGL-API-007). An `IllegalArgumentException` from the definition layer MUST map to
`INVALID_ARGUMENT`.

**WGL-API-004** (MUST) Long-polling RPCs (`PollTasks`, `PollEvents`) MUST clamp the caller's requested
wait to the server's maximum.

**WGL-API-005** (MUST) A cancelled call on `PollTasks` MUST be treated as "the worker is gone": the
server MUST NOT claim work it cannot run.

**WGL-API-006** (MUST) There MUST be no per-RPC authorization. TLS (optionally mTLS) authenticates the
channel; any trusted peer may call any RPC.

**WGL-API-007** (MUST) A storage failure that applied nothing — a `StorageException` classified
`TRANSIENT`, see [WGL-STOR-080](80-storage.md) — MUST map to `UNAVAILABLE`, which is what makes it
retryable by [WGL-WRK-100](20-worker.md). Every other storage failure, an unknown commit (`AMBIGUOUS`)
above all, MUST map to `INTERNAL`: the work may be durable, and the caller MUST NOT be told it can
simply re-send.

**WGL-API-008** (MUST NOT) Neither status's description may carry the exception's text: SQL, table and
constraint names, driver codes, host and schema all leak through it. The client MUST get a short
correlation id and the server log MUST get the detail.

*Verified by:* `tests/GrpcErrorMappingTest`, `tests/TlsTest`, `tests/StorageFailureStatusTest`.

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

**WGL-API-022** (MUST) An `OBSERVED` definition MUST be validated against
[WGL-OBS-002](40-execution-modes.md) at registration and refused with `INVALID_ARGUMENT` when it holds a
node kind no observer can report.

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

*Verified by:* `tests/BacklogCoverageTest`, `server/engine/AdvanceManyTest`, `tests/MemoryPollTest`.

## 7. Observed execution

| RPC | Request → Response |
|---|---|
| `ObserveRun` | `ObserveRunRequest{workflow, version, instanceId, correlationId, reporter, steps[], final}` → `ObserveRunResult{instanceId, instanceStatus, anomalies}` |
| `ObserveMany` | `ObserveManyRequest{runs[]}` → `ObserveManyResult{results[]}` |
| `GetStepStats` | `StepStatsRequest{workflow, version, since, sample}` → `StepStats{workflow, version, nodes[]}` |
| `ListAnomalies` | `ListAnomaliesRequest{workflow?, instanceId?, limit}` → `AnomalyList` |

**WGL-API-070** (MUST) Version 0 MUST mean the latest registered version, and `sample` or `limit` of 0 a
server default.

**WGL-API-071** (MUST) `ObserveMany` MUST apply each run on its own and answer in submission order, with
the same `errorStatus` convention as `ReportSteps`.

**WGL-API-072** (MUST) `ObserveRunResult.anomalies` MUST count the anomalies recorded **by that report**.

**WGL-API-073** (MUST) `NodeStats` MUST carry node id, name, count, mean, p50, p95, max, and queue-wait
p50/p95 (0 for steps that were never queued).

## 8. Event feed

| RPC | Request → Response |
|---|---|
| `PollEvents` | `PollEventsRequest{consumer, max, waitMillis, startFrom}` → `EventList{events[], retryAfterMillis}` |
| `AckEvents` | `AckEventsRequest{consumer, ackedSeq}` → `Ack` |

**WGL-API-080** (MUST) Both RPCs MUST be implemented by the server with the semantics of
[chapter 60](60-event-log.md). *(The comment in `wiggle.proto` calling them contract-only is stale — see
[drift](00-index.md#6-known-documentation-drift).)*

**WGL-API-081** (MUST) `EventList.retryAfterMillis` MUST carry the same backpressure hint as `TaskList`.

## 9. Java client surface

**WGL-API-090** (MUST) `WiggleClient` MUST be constructible from a target, optionally with TLS options and
a flag requiring TLS, and MUST strip a `scheme://` prefix from the target.

**WGL-API-091** (MUST) The client MUST expose at least: `register(spec[, force])`, `start` (by spec or by
name, with optional version and correlation id), `instance`, `instanceDetail`, `listInstances`,
`findByCorrelation`, `awaitCompletion(id, timeout)`, `cancel`, `signal`, `workflowNames`, `getWorkflow`,
`createSchedule`, `createCronSchedule`, `schedules`, `deleteSchedule`, `poll`, `reportSteps`, `fail`,
`heartbeat`, `backlogCoverage`, `stepStats`, `anomalies`, `pollEvents`, `ackEvents`, `cluster`.

**WGL-API-092** (MUST) `WiggleApiException` MUST carry the engine status code and MUST distinguish client
errors (4xx) from others.

**WGL-API-093** (MUST) A single-run `reportSteps` MUST read the one answer and raise
`WiggleApiException` with that status, so an ordinary one-step report reads as a plain call.

**WGL-API-094** (MUST) `WiggleConnection` MUST offer `direct(target[, tls])` for one cluster.

**WGL-API-095** (MUST) `register` MUST raise `WorkflowRegistrationException` carrying the server's
explanation when a registration is refused.

*Verified by:* `tests/ScheduleClientTest`, `tests/VersioningTest`, `tests/RpcRetryFailoverTest`.

## 10. The console's HTTP surface

The console is a separate process and a pure gRPC client; these endpoints are its own, not a server
node's ([WGL-OPS-040](90-ops.md)).

**WGL-API-100** (MUST) The JSON API MUST be exactly:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/auth` | who am I, and which role |
| `POST` | `/api/login` · `/logout` · `GET /login` | session cookie login and the browser form |
| `GET` | `/healthz` | the console pod's own probe |
| `GET` | `/api/cluster` | cluster view |
| `GET` | `/api/workflows` · `/api/workflows/{name}` | names, and one compiled graph as JSON |
| `GET` | `/api/instances` | list, filtered by workflow/status/limit or searched by instance or correlation id |
| `GET` | `/api/instances/{id}` | instance with its tokens |
| `POST` | `/api/instances/{id}/cancel` | cancel |
| `POST` | `/api/instances/{id}/signal/{name}` | deliver a signal; the JSON body merges into the context |
| `GET` | `/api/signals` | signal waits pending delivery |
| `GET` | `/api/backlog` | backlog coverage, with uncovered slices and stranded task counts |
| `GET` | `/api/stats` | per-step duration statistics |
| `GET` | `/api/anomalies` | observed-run anomalies |
| `GET`/`POST`/`DELETE` | `/api/schedules[/{id}]` | list, create (interval or cron), delete |
| `GET`/`POST`/`DELETE` | `/api/users[/{name}[/password]]` | managed accounts (admin only) |
| `POST` | `/api/password` | change one's own password |

**WGL-API-101** (MUST) An unknown `/api/*` path MUST be 404; a mutating call on a GET-only endpoint MUST
be 405.

**WGL-API-102** (MUST) A viewer MUST be refused every non-`GET` `/api/*` call except `/api/password`.

**WGL-API-103** (MUST) The backend MUST sit behind one neutral seam (`DashboardData`) carrying no engine
or storage types, so the same JSON is produced whether it is served in process or over gRPC.

**WGL-API-104** *Withdrawn: the cell coordinator was removed ([chapter 85 §15](85-sharding.md#15-dropping-the-coordinator)).*

*Verified by:* `console/ConsoleWebTest`, `console/ConsoleDataTest`, `console/ConsoleUsersTest`.
