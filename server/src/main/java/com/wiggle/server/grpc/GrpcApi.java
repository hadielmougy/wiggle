package com.wiggle.server.grpc;

import com.wiggle.core.Tls;
import com.wiggle.proto.*;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows.ServerNode;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The control-plane API. Workers pull work from here; nothing is ever pushed to them,
 * so workers need no inbound connectivity and can scale independently of the servers.
 */
public final class GrpcApi extends WiggleControlPlaneGrpc.WiggleControlPlaneImplBase implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(GrpcApi.class.getName());

    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final Server server;
    private final ExecutorService pool;
    private final long maxLongPollMillis;
    private final MemoryGuard memory;
    /** Versions already announced at INFO, so N workers registering the same graph log it once. */
    private final Set<String> announced = ConcurrentHashMap.newKeySet();

    public GrpcApi(WorkflowEngine engine, ClusterManager cluster, int port, long maxLongPollMillis)
            throws IOException {
        this(engine, cluster, port, maxLongPollMillis, Tls.Options.DISABLED, ServerConfig.Memory.DISABLED);
    }

    public GrpcApi(WorkflowEngine engine, ClusterManager cluster, int port, long maxLongPollMillis, Tls.Options tls,
                   ServerConfig.Memory memoryConfig) throws IOException {
        this.engine = engine;
        this.cluster = cluster;
        this.maxLongPollMillis = maxLongPollMillis;
        this.memory = new MemoryGuard(memoryConfig);
        this.pool = Executors.newVirtualThreadPerTaskExecutor();
        this.server = Grpc.newServerBuilderForPort(port, credentials(tls))
                .executor(pool)
                .intercept(new MemorySizeInterceptor(memory))   // sums in-flight request/response bytes
                .addService(this)
                .build();
    }

    /** TLS credentials when a keystore is configured (mTLS when a truststore is too); else plaintext. */
    private static ServerCredentials credentials(Tls.Options tls) throws IOException {
        if (!tls.hasKeyStore()) {
            LOG.log(System.Logger.Level.WARNING, "gRPC API is PLAINTEXT; set WIGGLE_TLS_KEYSTORE to enable TLS");
            return InsecureServerCredentials.create();
        }
        TlsServerCredentials.Builder b = TlsServerCredentials.newBuilder().keyManager(Tls.keyManagers(tls));
        if (tls.hasTrustStore()) {
            b.trustManager(Tls.trustManagers(tls)).clientAuth(TlsServerCredentials.ClientAuth.REQUIRE);  // mTLS
            LOG.log(System.Logger.Level.INFO, "gRPC API TLS enabled with required client certificates (mTLS)");
        } else {
            LOG.log(System.Logger.Level.INFO, "gRPC API TLS enabled (server-side)");
        }
        return b.build();
    }

    public void start() {
        try {
            server.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.log(System.Logger.Level.DEBUG, () -> "gRPC listening on port " + port());
    }

    public int port() {
        return server.getPort();
    }

    @Override public void close() {
        server.shutdown();
        try {
            server.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pool.shutdownNow();
    }

    @Override
    public void healthCheck(Empty req, StreamObserver<HealthStatus> resp) {
        LOG.log(System.Logger.Level.DEBUG, "rpc HealthCheck");
        run(resp, () -> HealthStatus.newBuilder()
                .setStatus("UP")
                .setNode(cluster.nodeId())
                .setLeader(cluster.isLeader())
                .build());
    }

    @Override
    public void getCluster(Empty req, StreamObserver<ClusterView> resp) {
        LOG.log(System.Logger.Level.DEBUG, "rpc GetCluster");
        run(resp, this::clusterView);
    }

    @Override
    public void listWorkflows(Empty req, StreamObserver<WorkflowNames> resp) {
        LOG.log(System.Logger.Level.DEBUG, "rpc ListWorkflows");
        run(resp, () -> WorkflowNames.newBuilder().addAllWorkflows(engine.workflowNames()).build());
    }

    @Override
    public void registerWorkflow(WorkflowDefinition req, StreamObserver<RegisterWorkflowResult> resp) {
        LOG.log(System.Logger.Level.DEBUG, "rpc RegisterWorkflow");
        run(resp, () -> {
            com.wiggle.core.WorkflowDefinition def =
                    com.wiggle.core.WorkflowDefinition.fromJson(ProtoJson.fromStruct(req.getDefinition()));
            if (req.getForce() && !ServerConfig.allowGraphReplace()) {
                throw EngineException.conflict("replacing the graph of '" + def.key()
                        + "' was requested but this server does not allow it; set "
                        + "WIGGLE_ALLOW_GRAPH_REPLACE=true (development only) or publish a new version");
            }
            engine.register(def, req.getForce());
            if (announced.add(def.key())) {
                LOG.log(System.Logger.Level.INFO, () -> "registered workflow " + def.key()
                        + " (" + def.nodes().size() + " nodes, mode=" + def.executionMode() + ")");
            }
            return RegisterWorkflowResult.newBuilder()
                    .setName(def.name())
                    .setVersion(String.valueOf(def.version()))
                    .setNodes(def.nodes().size())
                    .build();
        });
    }

    @Override
    public void getWorkflow(GetWorkflowRequest req, StreamObserver<WorkflowDefinition> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetWorkflow name=" + req.getName());
        run(resp, () -> {
            com.wiggle.core.WorkflowDefinition def = (req.hasVersion()
                    ? engine.definition(req.getName(), req.getVersion())
                    : engine.latestDefinition(req.getName()))
                    .orElseThrow(() -> EngineException.notFound("workflow"));
            return WorkflowDefinition.newBuilder().setDefinition(ProtoJson.toStruct(def.toJson())).build();
        });
    }

    @Override
    public void startInstance(StartInstanceRequest req, StreamObserver<StartInstanceResult> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc StartInstance workflow=" + req.getWorkflow()
                + " version=" + (req.hasVersion() ? req.getVersion() : "latest")
                + " correlationId=" + (req.hasCorrelationId() ? req.getCorrelationId() : null));
        run(resp, () -> {
            Integer version = req.hasVersion() ? req.getVersion() : null;
            String correlationId = req.hasCorrelationId() ? req.getCorrelationId() : null;
            Object context = req.hasContext() ? ProtoJson.fromValue(req.getContext()) : null;
            String id = engine.start(req.getWorkflow(), version, context, correlationId);
            return StartInstanceResult.newBuilder().setInstanceId(id).setWorkflow(req.getWorkflow()).build();
        });
    }

    @Override
    public void listInstances(ListInstancesRequest req, StreamObserver<InstanceList> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc ListInstances workflow="
                + (req.hasWorkflow() ? req.getWorkflow() : null) + " status="
                + (req.hasStatus() ? req.getStatus() : null) + " limit=" + req.getLimit());
        run(resp, () -> {
            String workflow = req.hasWorkflow() ? req.getWorkflow() : null;
            String status = req.hasStatus() ? req.getStatus() : null;
            int limit = req.getLimit() > 0 ? req.getLimit() : 50;
            List<com.wiggle.core.InstanceView> views = req.hasCorrelationId()
                    ? engine.findByCorrelation(req.getCorrelationId(), limit)
                    : engine.list(workflow, status, limit);
            InstanceList.Builder out = InstanceList.newBuilder();
            for (com.wiggle.core.InstanceView v : views) out.addInstances(viewProto(v));
            return out.build();
        });
    }

    @Override
    public void getInstance(InstanceIdRequest req, StreamObserver<InstanceDetail> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetInstance id=" + req.getInstanceId());
        run(resp, () -> {
            com.wiggle.core.InstanceView v = engine.instance(req.getInstanceId())
                    .orElseThrow(() -> EngineException.notFound("instance"));
            InstanceDetail.Builder out = InstanceDetail.newBuilder().setInstance(viewProto(v));
            for (com.wiggle.server.store.Rows.Token t : engine.tokens(req.getInstanceId())) out.addTokens(tokenProto(t));
            return out.build();
        });
    }

    @Override
    public void cancelInstance(CancelInstanceRequest req, StreamObserver<CancelInstanceResult> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc CancelInstance id=" + req.getInstanceId()
                + " reason=" + req.getReason());
        run(resp, () -> {
            engine.cancel(req.getInstanceId(),
                    req.getReason().isEmpty() ? "cancelled via API" : req.getReason());
            return CancelInstanceResult.newBuilder().setCancelled(req.getInstanceId()).build();
        });
    }

    @Override
    public void signalInstance(SignalRequest req, StreamObserver<Ack> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc SignalInstance id=" + req.getInstanceId()
                + " signal=" + req.getSignal());
        run(resp, () -> {
            Object payload = req.hasPayload() ? ProtoJson.fromValue(req.getPayload()) : null;
            engine.signal(req.getInstanceId(), req.getSignal(), payload);
            return Ack.newBuilder().setOk(true).build();
        });
    }

    @Override
    public void createSchedule(CreateScheduleRequest req, StreamObserver<ScheduleView> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc CreateSchedule workflow=" + req.getWorkflow());
        run(resp, () -> {
            Object context = req.hasContext() ? ProtoJson.fromValue(req.getContext()) : null;
            String id = req.getCadenceCase() == CreateScheduleRequest.CadenceCase.CRON
                    ? engine.createCronSchedule(req.getWorkflow(), req.getCron(), context)
                    : engine.createSchedule(req.getWorkflow(),
                            java.time.Duration.ofMillis(req.getEveryMillis()), context);
            return engine.schedules().stream().filter(s -> s.id.equals(id)).findFirst()
                    .map(GrpcApi::scheduleView).orElseThrow();
        });
    }

    @Override
    public void listSchedules(Empty req, StreamObserver<ScheduleList> resp) {
        run(resp, () -> {
            ScheduleList.Builder out = ScheduleList.newBuilder();
            engine.schedules().forEach(s -> out.addSchedules(scheduleView(s)));
            return out.build();
        });
    }

    @Override
    public void deleteSchedule(ScheduleIdRequest req, StreamObserver<Ack> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc DeleteSchedule id=" + req.getId());
        run(resp, () -> {
            engine.deleteSchedule(req.getId());
            return Ack.newBuilder().setOk(true).build();
        });
    }

    private static ScheduleView scheduleView(com.wiggle.server.store.Rows.Schedule s) {
        ScheduleView.Builder b = ScheduleView.newBuilder()
                .setId(s.id).setWorkflow(s.workflow)
                .setEveryMillis(s.intervalMillis)
                .setNextFireAt(s.nextFireAt).setCreatedAt(s.createdAt);
        if (s.cron != null) b.setCron(s.cron);
        return b.build();
    }

    /** The (workflow, version) pairs a version-scoped worker serves; empty means every version. */
    private static java.util.Set<com.wiggle.core.WorkflowVersion> versionFilter(PollRequest req) {
        if (req.getVersionsCount() == 0) return java.util.Set.of();
        java.util.Set<com.wiggle.core.WorkflowVersion> out = new java.util.LinkedHashSet<>();
        for (com.wiggle.proto.WorkflowVersion v : req.getVersionsList()) {
            out.add(new com.wiggle.core.WorkflowVersion(v.getWorkflow(), v.getVersion()));
        }
        return out;
    }

    @Override
    public void getBacklogCoverage(BacklogCoverageRequest req, StreamObserver<BacklogCoverage> resp) {
        run(resp, () -> {
            int max = req.getMax() > 0 ? Math.min(req.getMax(), 500) : 100;
            BacklogCoverage.Builder out = BacklogCoverage.newBuilder()
                    .setLivePollers(engine.livePollers().size());
            for (com.wiggle.server.store.Rows.BacklogSlice s : engine.backlog(max)) {
                out.addSlices(BacklogSlice.newBuilder()
                        .setWorkflow(s.workflow())
                        .setVersion(s.version())
                        .setQueue(s.queue() == null ? "" : s.queue())
                        .setReadyCount(s.readyCount())
                        .setOldestAvailableAt(s.oldestAvailableAt())
                        .setCovered(engine.covered(s.workflow(), s.version(), s.queue()))
                        .build());
            }
            return out.build();
        });
    }

    @Override
    public void pollTasks(PollRequest req, StreamObserver<TaskList> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollTasks worker=" + req.getWorkerId()
                + " queues=" + req.getQueuesList() + " max=" + req.getMax() + " waitMillis=" + req.getWaitMillis());
        run(resp, () -> {
            // Admission control: under memory pressure, reject a configured fraction of incoming
            // polls outright (empty + a jittered hold-off), before doing any work. The other polls
            // are served normally, so load eases gently instead of stopping dead.
            if (memory.rejectPoll()) {
                long retryAfter = memory.retryAfterMillis();
                LOG.log(System.Logger.Level.WARNING, () -> "memory pressure ("
                        + String.format("%.0f%%", MemoryGuard.heapUtilization() * 100)
                        + " heap, in-flight req/resp " + memory.inFlightBytes() + " bytes): rejecting poll from "
                        + req.getWorkerId() + ", retry after " + retryAfter + "ms");
                return TaskList.newBuilder().setRetryAfterMillis(retryAfter).build();
            }

            Set<String> queues = new LinkedHashSet<>(req.getQueuesList());
            int max = req.getMax() > 0 ? req.getMax() : 1;
            long lease = req.getLeaseMillis();
            long deadline = System.currentTimeMillis() + Math.min(maxLongPollMillis, req.getWaitMillis());
            // A cancelled call means the worker is gone (closed/dead); don't claim work it can't run.
            io.grpc.Context ctx = io.grpc.Context.current();
            List<com.wiggle.core.TaskActivation> tasks =
                    engine.poll(req.getWorkerId(), queues, versionFilter(req), max, lease, deadline,
                            ctx::isCancelled);
            LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollTasks worker=" + req.getWorkerId()
                    + " returning " + tasks.size() + " task(s)");
            TaskList.Builder out = TaskList.newBuilder();
            for (com.wiggle.core.TaskActivation t : tasks) out.addTasks(taskProto(t));
            return out.build();
        });
    }

    @Override
    public void completeTask(TaskResultRequest req, StreamObserver<Ack> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc CompleteTask taskId=" + req.getTaskId()
                + " leaseOwner=" + req.getLeaseOwner());
        run(resp, () -> {
            Object result = req.hasResult() ? ProtoJson.fromValue(req.getResult()) : null;
            engine.complete(req.getTaskId(), req.getLeaseOwner(), result);
            return Ack.newBuilder().setOk(true).build();
        });
    }

    @Override
    public void failTask(TaskFailureRequest req, StreamObserver<Ack> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc FailTask taskId=" + req.getTaskId()
                + " leaseOwner=" + req.getLeaseOwner() + " retryable=" + req.getRetryable()
                + " message=" + req.getMessage());
        run(resp, () -> {
            engine.fail(req.getTaskId(), req.getLeaseOwner(), req.getMessage(), req.getRetryable());
            return Ack.newBuilder().setOk(true).build();
        });
    }

    @Override
    public void heartbeatTask(HeartbeatRequest req, StreamObserver<HeartbeatResult> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc HeartbeatTask taskId=" + req.getTaskId()
                + " leaseOwner=" + req.getLeaseOwner() + " extendMillis=" + req.getExtendMillis());
        run(resp, () -> {
            long until = engine.extendLease(req.getTaskId(), req.getLeaseOwner(), req.getExtendMillis());
            return HeartbeatResult.newBuilder().setLeaseExpiresAt(until).build();
        });
    }

    @Override
    public void advanceRun(AdvanceRunRequest req, StreamObserver<AdvanceRunResult> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc AdvanceRun taskId=" + req.getTaskId()
                + " leaseOwner=" + req.getLeaseOwner() + " steps=" + req.getStepsCount() + " final=" + req.getFinal());
        run(resp, () -> {
            WorkflowEngine.AdvanceOutcome out =
                    engine.advance(req.getTaskId(), req.getLeaseOwner(), stepInputs(req), req.getFinal());
            return AdvanceRunResult.newBuilder()
                    .setInstanceStatus(out.instanceStatus())
                    .setLeaseExpiresAt(out.leaseExpiresAt())
                    .setNextTaskId(out.nextTaskId() == null ? "" : out.nextTaskId())
                    .build();
        });
    }

    @Override
    public void advanceMany(AdvanceManyRequest req, StreamObserver<AdvanceManyResult> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc AdvanceMany runs=" + req.getRunsCount());
        run(resp, () -> {
            List<WorkflowEngine.Run> runs = new ArrayList<>(req.getRunsCount());
            for (AdvanceRunRequest r : req.getRunsList()) {
                runs.add(new WorkflowEngine.Run(r.getTaskId(), r.getLeaseOwner(), stepInputs(r), r.getFinal()));
            }
            Map<String, WorkflowEngine.RunResult> results = engine.advanceMany(runs);
            AdvanceManyResult.Builder out = AdvanceManyResult.newBuilder();
            results.forEach((taskId, r) -> {
                RunOutcome.Builder one = RunOutcome.newBuilder().setTaskId(taskId);
                if (r.ok()) {
                    one.setOutcome(AdvanceRunResult.newBuilder()
                            .setInstanceStatus(r.outcome().instanceStatus())
                            .setLeaseExpiresAt(r.outcome().leaseExpiresAt())
                            .setNextTaskId(r.outcome().nextTaskId() == null ? "" : r.outcome().nextTaskId()));
                } else {
                    one.setErrorStatus(r.errorStatus()).setError(r.error() == null ? "" : r.error());
                }
                out.addResults(one);
            });
            return out.build();
        });
    }

    private static List<WorkflowEngine.StepInput> stepInputs(AdvanceRunRequest req) {
        return stepInputs(req.getStepsList());
    }

    private static List<WorkflowEngine.StepInput> stepInputs(List<StepResult> reported) {
        List<WorkflowEngine.StepInput> steps = new ArrayList<>(reported.size());
        for (StepResult s : reported) {
            Object merge = s.getOutcomeCase() == StepResult.OutcomeCase.MERGE
                    ? ProtoJson.fromValue(s.getMerge()) : null;
            Boolean predicate = s.getOutcomeCase() == StepResult.OutcomeCase.PREDICATE_VALUE
                    ? s.getPredicateValue() : null;
            String error = s.getOutcomeCase() == StepResult.OutcomeCase.ERROR ? s.getError() : null;
            steps.add(new WorkflowEngine.StepInput(s.getNodeId(), merge, predicate, error,
                    s.getStartedAt() == 0 ? null : s.getStartedAt(),
                    s.getFinishedAt() == 0 ? null : s.getFinishedAt()));
        }
        return steps;
    }

    @Override
    public void observeRun(ObserveRunRequest req, StreamObserver<ObserveRunResult> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc ObserveRun workflow=" + req.getWorkflow()
                + " instanceId=" + req.getInstanceId() + " steps=" + req.getStepsCount() + " final=" + req.getFinal());
        run(resp, () -> observeProto(engine.observe(req.getWorkflow(), req.getVersion() == 0 ? null : req.getVersion(),
                req.getInstanceId(), req.getCorrelationId().isEmpty() ? null : req.getCorrelationId(),
                req.getReporter(), stepInputs(req.getStepsList()), req.getFinal())));
    }

    @Override
    public void observeMany(ObserveManyRequest req, StreamObserver<ObserveManyResult> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc ObserveMany runs=" + req.getRunsCount());
        run(resp, () -> {
            List<WorkflowEngine.ObservedRun> runs = new ArrayList<>(req.getRunsCount());
            for (ObserveRunRequest r : req.getRunsList()) {
                runs.add(new WorkflowEngine.ObservedRun(r.getWorkflow(), r.getVersion() == 0 ? null : r.getVersion(),
                        r.getInstanceId(), r.getCorrelationId().isEmpty() ? null : r.getCorrelationId(),
                        r.getReporter(), stepInputs(r.getStepsList()), r.getFinal()));
            }
            ObserveManyResult.Builder out = ObserveManyResult.newBuilder();
            for (WorkflowEngine.ObserveOutcome o : engine.observeMany(runs)) {
                ObserveOutcome.Builder one = ObserveOutcome.newBuilder();
                if (o.ok()) one.setOutcome(observeProto(o.result()));
                else one.setErrorStatus(o.errorStatus()).setError(o.error() == null ? "" : o.error());
                out.addResults(one);
            }
            return out.build();
        });
    }

    private static ObserveRunResult observeProto(com.wiggle.core.ObserveResult r) {
        return ObserveRunResult.newBuilder()
                .setInstanceId(r.instanceId())
                .setInstanceStatus(r.instanceStatus())
                .setAnomalies(r.anomalies())
                .build();
    }

    @Override
    public void getStepStats(StepStatsRequest req, StreamObserver<StepStats> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetStepStats workflow=" + req.getWorkflow()
                + " version=" + req.getVersion() + " since=" + req.getSince());
        run(resp, () -> {
            int sample = req.getSample() > 0 ? req.getSample() : 10_000;
            StepStats.Builder out = StepStats.newBuilder().setWorkflow(req.getWorkflow()).setVersion(req.getVersion());
            for (com.wiggle.core.NodeStats n : engine.stepStats(req.getWorkflow(), req.getVersion(), req.getSince(), sample)) {
                out.addNodes(NodeStats.newBuilder()
                        .setNodeId(n.nodeId()).setName(n.name() == null ? "" : n.name())
                        .setCount(n.count()).setMeanMillis(n.meanMillis())
                        .setP50Millis(n.p50Millis()).setP95Millis(n.p95Millis()).setMaxMillis(n.maxMillis())
                        .setWaitP50Millis(n.waitP50Millis()).setWaitP95Millis(n.waitP95Millis()));
            }
            return out.build();
        });
    }

    @Override
    public void listAnomalies(ListAnomaliesRequest req, StreamObserver<AnomalyList> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc ListAnomalies workflow="
                + (req.hasWorkflow() ? req.getWorkflow() : null) + " limit=" + req.getLimit());
        run(resp, () -> {
            int limit = req.getLimit() > 0 ? req.getLimit() : 100;
            AnomalyList.Builder out = AnomalyList.newBuilder();
            for (com.wiggle.core.AnomalyView a : engine.anomalies(req.hasWorkflow() ? req.getWorkflow() : null,
                    req.hasInstanceId() ? req.getInstanceId() : null, limit)) {
                Anomaly.Builder one = Anomaly.newBuilder().setInstanceId(a.instanceId()).setWorkflow(a.workflow())
                        .setVersion(a.version()).setKind(a.kind()).setAt(a.at());
                if (a.expectedNode() != null) one.setExpectedNode(a.expectedNode());
                if (a.reportedNode() != null) one.setReportedNode(a.reportedNode());
                if (a.detail() != null) one.setDetail(a.detail());
                out.addAnomalies(one);
            }
            return out.build();
        });
    }

    private ClusterView clusterView() {
        ClusterView.Builder out = ClusterView.newBuilder().setSelf(cluster.nodeId()).setLeader(cluster.isLeader());
        long now = System.currentTimeMillis();
        for (ServerNode n : cluster.members()) {
            out.addMembers(ClusterMember.newBuilder()
                    .setId(n.id)
                    .setName(n.name)
                    .setFirstHeartbeat(n.firstHeartbeat)
                    .setLastHeartbeat(n.lastHeartbeat)
                    .setWorkers(n.workers)
                    .setLeader(n.leader)
                    .setAlive(now - n.lastHeartbeat < cluster.deadAfterMillis())
                    .build());
        }
        return out.build();
    }

    private static InstanceView viewProto(com.wiggle.core.InstanceView v) {
        InstanceView.Builder m = InstanceView.newBuilder()
                .setId(v.id())
                .setWorkflow(v.workflow())
                .setVersion(v.version())
                .setStatus(v.status())
                .setCreatedAt(v.createdAt())
                .setUpdatedAt(v.updatedAt());
        if (v.terminationReason() != null) m.setTerminationReason(v.terminationReason());
        if (v.error() != null) m.setError(v.error());
        if (v.context() != null) m.setContext(ProtoJson.toValue(v.context()));
        return m.build();
    }

    private static Token tokenProto(com.wiggle.server.store.Rows.Token t) {
        Token.Builder m = Token.newBuilder()
                .setId(t.id)
                .setNodeId(t.nodeId)
                .setKind(t.kind.name())
                .setStatus(t.status.name())
                .setActivity(t.activity == null ? "" : t.activity)
                .setAttempt(t.attempt)
                .setAvailableAt(t.availableAt);
        if (t.leaseOwner != null) m.setLeaseOwner(t.leaseOwner);
        if (t.lastError != null) m.setLastError(t.lastError);
        if (t.startedAt != null) m.setStartedAt(t.startedAt);
        if (t.finishedAt != null) m.setFinishedAt(t.finishedAt);
        return m.build();
    }

    private static TaskActivation taskProto(com.wiggle.core.TaskActivation t) {
        TaskActivation.Builder m = TaskActivation.newBuilder()
                .setTaskId(t.taskId())
                .setInstanceId(t.instanceId())
                .setWorkflow(t.workflow())
                .setVersion(t.version())
                .setNodeId(t.nodeId())
                .setStepName(t.stepName() == null ? "" : t.stepName())
                .setActivity(t.activity())
                .setKind(t.kind().name())
                .setAttempt(t.attempt())
                .setLeaseExpiresAt(t.leaseExpiresAt())
                .setLeaseOwner(t.leaseOwner())
                .setExecutionMode(t.executionMode().name());
        if (t.context() != null) m.setContext(ProtoJson.toValue(t.context()));
        if (t.baseContext() != null) {   // a forEach item step: deliver the frozen base alongside
            m.setBaseContext(ProtoJson.toValue(t.baseContext()));
            m.setItemIndex(t.itemIndex());
            if (t.itemMapKey() != null) m.setItemMapKey(t.itemMapKey());
        }
        return m.build();
    }

    private interface Handler<T> { T call() throws Exception; }

    private <T> void run(StreamObserver<T> resp, Handler<T> handler) {
        try {
            T result = handler.call();
            resp.onNext(result);
            resp.onCompleted();
        } catch (EngineException e) {
            LOG.log(System.Logger.Level.DEBUG, () -> "rpc failed with " + e.statusCode() + ": " + e.getMessage());
            resp.onError(status(e.statusCode()).withDescription(e.getMessage()).asRuntimeException());
        } catch (IllegalArgumentException e) {
            LOG.log(System.Logger.Level.DEBUG, () -> "rpc failed with bad request: " + e.getMessage());
            resp.onError(Status.INVALID_ARGUMENT.withDescription(String.valueOf(e.getMessage())).asRuntimeException());
        } catch (Exception e) {
            // Unexpected -- an engine bug or an infrastructure failure (e.g. a StorageException wrapping
            // a SQLException). Log the full detail server-side, but return only a generic INTERNAL to the
            // client: the exception class/message can leak internals (SQL text, table/constraint names,
            // driver codes, host/schema). A short correlation id ties the client's error to this log line.
            String errorId = UUID.randomUUID().toString().substring(0, 8);
            LOG.log(System.Logger.Level.ERROR, "unhandled error [" + errorId + "]", e);
            resp.onError(Status.INTERNAL
                    .withDescription("internal error (ref " + errorId + ")")
                    .asRuntimeException());
        }
    }

    private static Status status(int httpStatusCode) {
        return switch (httpStatusCode) {
            case 400 -> Status.INVALID_ARGUMENT;
            case 404 -> Status.NOT_FOUND;
            case 409 -> Status.FAILED_PRECONDITION;
            default -> Status.INTERNAL;
        };
    }
}
