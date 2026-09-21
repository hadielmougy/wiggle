package com.wiggle.client;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.PollResult;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.NodeKind;
import com.wiggle.core.Tls;
import com.wiggle.proto.*;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;
import io.github.shield.internal.RetriesExhaustedException;

import java.util.*;
import java.util.concurrent.TimeUnit;

public final class WiggleClient implements AutoCloseable {

    private final ManagedChannel channel;
    private final WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub stub;

    /** Connects with TLS if {@code WIGGLE_TLS_*} is configured, otherwise plaintext. */
    public WiggleClient(String target) {
        this(target, Tls.Options.fromEnvironment());
    }

    /**
     * Connects to {@code target}, using TLS when {@code tls} carries a keystore and/or truststore:
     * the truststore verifies the server, and the keystore presents a client certificate for mTLS.
     * With neither, the channel is plaintext.
     */
    public WiggleClient(String target, Tls.Options tls) {
        this(target, tls, tls.any());
    }

    /**
     * Connects to {@code target}, forcing TLS when {@code requireTls} is set even if {@code tls}
     * carries no stores -- so a server whose certificate chains to a CA already in the JVM default
     * trust store can be reached without configuring a truststore. A configured truststore/keystore
     * still overrides the default trust and adds a client certificate for mTLS.
     */
    public WiggleClient(String target, Tls.Options tls, boolean requireTls) {
        this.channel = Grpc.newChannelBuilder(stripScheme(target), channelCredentials(tls, requireTls)).build();
        this.stub = WiggleControlPlaneGrpc.newBlockingStub(channel);
    }

    private static ChannelCredentials channelCredentials(Tls.Options tls, boolean requireTls) {
        if (!requireTls && !tls.any()) return InsecureChannelCredentials.create();
        TlsChannelCredentials.Builder b = TlsChannelCredentials.newBuilder();
        if (tls.hasTrustStore()) b.trustManager(Tls.trustManagers(tls));   // else the JVM default trust store
        if (tls.hasKeyStore()) b.keyManager(Tls.keyManagers(tls));         // client cert for mTLS
        return b.build();
    }

    private static String stripScheme(String target) {
        int i = target.indexOf("://");
        return i < 0 ? target : target.substring(i + 3);
    }

    public void register(FlowSpec flowSpec) {
        register(flowSpec, false);
    }

    /**
     * Publishes a topology at the version it declares. Re-registering the same graph is a no-op;
     * re-registering a <em>changed</em> graph under a version that already exists fails, because
     * instances running on that version would otherwise have it swapped underneath them.
     *
     * <p>{@code force} asks the server to replace it anyway -- for a local edit-run loop where
     * bumping the version every time is noise. The server refuses unless it is started with
     * {@code WIGGLE_ALLOW_GRAPH_REPLACE=true}, so a {@code force} left in application code cannot
     * rewrite a graph in production.
     */
    public void register(FlowSpec flowSpec, boolean force) {
        call(() -> stub.registerWorkflow(WorkflowDefinition.newBuilder()
                .setDefinition(ProtoJson.toStruct(flowSpec.definition().toJson()))
                .setForce(force)
                .build()));
    }

    /**
     * The dispatchable backlog split by (workflow, version, queue), each flagged with whether a worker
     * polling that node would claim it. An uncovered slice is work nothing can pick up -- a queue
     * nobody polls, or a version every worker has scoped itself out of.
     */
    public List<BacklogSlice> backlogCoverage(int max) {
        return Wire.backlogSlices(call(() -> stub.getBacklogCoverage(
                com.wiggle.proto.BacklogCoverageRequest.newBuilder().setMax(max).build())));
    }

    /** One slice of the dispatchable backlog. See {@link #backlogCoverage(int)}. */
    public record BacklogSlice(String workflow, int version, String queue, int readyCount,
                               long oldestAvailableAt, boolean covered, int livePollers) {}

    /**
     * The registered graph for {@code name} -- the server's source of truth for a workflow's step
     * names, kinds, and queues. Throws {@link WiggleApiException} with status 404 if the workflow was
     * never registered. Used by {@link Worker#handle} reconciliation.
     */
    public com.wiggle.core.WorkflowDefinition getWorkflow(String name) {
        return getWorkflow(name, null);
    }

    /** {@code version} null = the latest registered; otherwise that exact version. */
    public com.wiggle.core.WorkflowDefinition getWorkflow(String name, Integer version) {
        GetWorkflowRequest.Builder req = GetWorkflowRequest.newBuilder().setName(name);
        if (version != null) req.setVersion(version);
        WorkflowDefinition def = call(() -> stub.getWorkflow(req.build()));
        return com.wiggle.core.WorkflowDefinition.fromJson(ProtoJson.fromStruct(def.getDefinition()));
    }

    public String start(String workflow, Object context) {
        return start(workflow, context, null, null);
    }

    public String start(FlowSpec flowSpec, Object context) {
        return start(flowSpec.name(), context, flowSpec.version(), null);
    }

    public String start(String workflow, Object context, Integer version, String correlationId) {
        StartInstanceRequest.Builder req = StartInstanceRequest.newBuilder().setWorkflow(workflow);
        if (context != null) req.setContext(ProtoJson.toValue(com.wiggle.core.RecordMapper.toJson(context)));
        if (version != null) req.setVersion(version);
        if (correlationId != null) req.setCorrelationId(correlationId);
        return call(() -> stub.startInstance(req.build())).getInstanceId();
    }

    public com.wiggle.core.InstanceView instance(String instanceId) {
        InstanceDetail detail = call(() -> stub.getInstance(
                InstanceIdRequest.newBuilder().setInstanceId(instanceId).build()));
        return Wire.instanceView(detail.getInstance());
    }

    /** An instance plus its tokens (the {@code GetInstance} detail; {@link #instance} drops the tokens). */
    public InstanceWithTokens instanceDetail(String instanceId) {
        InstanceDetail detail = call(() -> stub.getInstance(
                InstanceIdRequest.newBuilder().setInstanceId(instanceId).build()));
        java.util.List<TokenInfo> tokens = new java.util.ArrayList<>();
        for (Token t : detail.getTokensList()) tokens.add(Wire.tokenInfo(t));
        return new InstanceWithTokens(Wire.instanceView(detail.getInstance()), tokens);
    }

    /** The names of all registered workflows. */
    public java.util.List<String> workflowNames() {
        return call(() -> stub.listWorkflows(Empty.getDefaultInstance())).getWorkflowsList();
    }

    /** Lists instances filtered by {@code workflow} and/or {@code status} (either may be null), newest first. */
    public java.util.List<com.wiggle.core.InstanceView> listInstances(String workflow, String status, int limit) {
        ListInstancesRequest.Builder req = ListInstancesRequest.newBuilder().setLimit(limit);
        if (workflow != null) req.setWorkflow(workflow);
        if (status != null) req.setStatus(status);
        InstanceList list = call(() -> stub.listInstances(req.build()));
        java.util.List<com.wiggle.core.InstanceView> out = new java.util.ArrayList<>();
        for (InstanceView v : list.getInstancesList()) out.add(Wire.instanceView(v));
        return out;
    }

    /** An instance view plus the (proto-shaped) tokens driving it. */
    public record InstanceWithTokens(com.wiggle.core.InstanceView instance, java.util.List<TokenInfo> tokens) {}

    /** A token as carried on the wire (thinner than the server row: no queue/leaseExpires/updatedAt). */
    public record TokenInfo(String id, String nodeId, String kind, String status, String activity,
                            int attempt, long availableAt, String leaseOwner, String lastError) {}

    /** Instances started with {@code correlationId} (a business key), newest first (default limit 50). */
    public java.util.List<com.wiggle.core.InstanceView> findByCorrelation(String correlationId) {
        return findByCorrelation(correlationId, 50);
    }

    public java.util.List<com.wiggle.core.InstanceView> findByCorrelation(String correlationId, int limit) {
        InstanceList list = call(() -> stub.listInstances(ListInstancesRequest.newBuilder()
                .setCorrelationId(correlationId).setLimit(limit).build()));
        java.util.List<com.wiggle.core.InstanceView> out = new java.util.ArrayList<>();
        for (InstanceView v : list.getInstancesList()) out.add(Wire.instanceView(v));
        return out;
    }

    public com.wiggle.core.InstanceView awaitCompletion(String instanceId, java.time.Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        com.wiggle.core.InstanceView v = instance(instanceId);
        while (!v.isTerminal()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("instance " + instanceId + " still " + v.status()
                        + " after " + timeout);
            }
            sleep(50);
            v = instance(instanceId);
        }
        return v;
    }

    public void cancel(String instanceId, String reason) {
        call(() -> stub.cancelInstance(CancelInstanceRequest.newBuilder()
                .setInstanceId(instanceId)
                .setReason(reason == null ? "" : reason)
                .build()));
    }

    public Map<String, Object> cluster() {
        return Wire.clusterMap(call(() -> stub.getCluster(Empty.getDefaultInstance())));
    }

    public PollResult poll(String workerId, Collection<String> queues, int max,
                           long leaseMillis, long waitMillis) {
        return poll(workerId, queues, java.util.Set.of(), max, leaseMillis, waitMillis);
    }

    /**
     * {@code versions} empty = claim every version, which is what an unversioned worker sends.
     * Non-empty scopes the claim to those (workflow, version) pairs.
     */
    public PollResult poll(String workerId, Collection<String> queues,
                           Collection<com.wiggle.core.WorkflowVersion> versions, int max,
                           long leaseMillis, long waitMillis) {
        PollRequest.Builder b = PollRequest.newBuilder()
                .setWorkerId(workerId)
                .addAllQueues(queues)
                .setMax(max)
                .setLeaseMillis(leaseMillis)
                .setWaitMillis(waitMillis);
        for (com.wiggle.core.WorkflowVersion v : versions) {
            b.addVersions(com.wiggle.proto.WorkflowVersion.newBuilder()
                    .setWorkflow(v.workflow()).setVersion(v.version()).build());
        }
        PollRequest req = b.build();
        TaskList res = call(() -> stub.pollTasks(req));
        List<com.wiggle.core.TaskActivation> out = new ArrayList<>(res.getTasksCount());
        for (com.wiggle.proto.TaskActivation t : res.getTasksList()) out.add(Wire.taskActivation(t));
        return new PollResult(out, res.getRetryAfterMillis());
    }

    public void complete(String taskId, String leaseOwner, Object result) {
        TaskResultRequest.Builder req = TaskResultRequest.newBuilder()
                .setTaskId(taskId)
                .setLeaseOwner(leaseOwner);
        if (result != null) req.setResult(ProtoJson.toValue(com.wiggle.core.RecordMapper.toJson(result)));
        call(() -> stub.completeTask(req.build()));
    }

    public void fail(String taskId, String leaseOwner, String message, boolean retryable) {
        call(() -> stub.failTask(TaskFailureRequest.newBuilder()
                .setTaskId(taskId)
                .setLeaseOwner(leaseOwner)
                .setMessage(message)
                .setRetryable(retryable)
                .build()));
    }

    /** Delivers a named signal to a running instance; {@code payload} merges into its context. */
    public void signal(String instanceId, String signal, Object payload) {
        SignalRequest.Builder req = SignalRequest.newBuilder().setInstanceId(instanceId).setSignal(signal);
        if (payload != null) req.setPayload(ProtoJson.toValue(com.wiggle.core.RecordMapper.toJson(payload)));
        call(() -> stub.signalInstance(req.build()));
    }

    /** Creates a recurring start on a fixed interval; returns the schedule id. */
    public String createSchedule(String workflow, java.time.Duration every, Object context) {
        CreateScheduleRequest.Builder req = CreateScheduleRequest.newBuilder()
                .setWorkflow(workflow).setEveryMillis(every.toMillis());
        if (context != null) req.setContext(ProtoJson.toValue(com.wiggle.core.RecordMapper.toJson(context)));
        return call(() -> stub.createSchedule(req.build())).getId();
    }

    /** Creates a recurring start on a five-field cron expression (evaluated in UTC); returns the schedule id. */
    public String createCronSchedule(String workflow, String cron, Object context) {
        CreateScheduleRequest.Builder req = CreateScheduleRequest.newBuilder()
                .setWorkflow(workflow).setCron(cron);
        if (context != null) req.setContext(ProtoJson.toValue(com.wiggle.core.RecordMapper.toJson(context)));
        return call(() -> stub.createSchedule(req.build())).getId();
    }

    /** All schedules on the server. {@code everyMillis} is 0 for cron schedules; {@code cron} is null for interval ones. */
    public List<ScheduleInfo> schedules() {
        return call(() -> stub.listSchedules(Empty.getDefaultInstance())).getSchedulesList().stream()
                .map(Wire::scheduleInfo)
                .toList();
    }

    public void deleteSchedule(String id) {
        call(() -> stub.deleteSchedule(ScheduleIdRequest.newBuilder().setId(id).build()));
    }

    public record ScheduleInfo(String id, String workflow, long everyMillis, String cron,
                               long nextFireAt, long createdAt) {}

    public void heartbeat(String taskId, String leaseOwner, long extendMillis) {
        call(() -> stub.heartbeatTask(HeartbeatRequest.newBuilder()
                .setTaskId(taskId)
                .setLeaseOwner(leaseOwner)
                .setExtendMillis(extendMillis)
                .build()));
    }

    /**
     * Reports a locally-executed run (LOCAL_SYNC/LOCAL_ASYNC) and returns whether to keep going.
     * {@code steps} carries, per node, either a task merge (Object) or a predicate value (Boolean).
     */
    public com.wiggle.core.AdvanceResult advanceRun(String taskId, String leaseOwner,
                                                    List<StepReport> steps, boolean finalHandback) {
        AdvanceRunRequest.Builder req = AdvanceRunRequest.newBuilder()
                .setTaskId(taskId).setLeaseOwner(leaseOwner).setFinal(finalHandback);
        for (StepReport s : steps) req.addSteps(Wire.stepResult(s));
        return Wire.advanceResult(call(() -> stub.advanceRun(req.build())));
    }

    /** One reported step: exactly one of {@code merge} (task) or {@code predicateValue} (predicate). */
    public record StepReport(String nodeId, Object merge, Boolean predicateValue) {}

    private interface Call<T> { T run(); }

    // Every client and worker operation routes through call(), so the shared UNAVAILABLE retry
    // ({@link RpcRetry}) covers them all — a call issued while a cell is momentarily gone (restart /
    // failover) rides out the outage. Permanent errors map straight through; exhausted retries map to
    // WiggleApiException(status 0), as before.
    private <T> T call(Call<T> call) {
        try {
            return RpcRetry.retrying(call::run);
        } catch (RetriesExhaustedException ex) {
            StatusRuntimeException last = RpcRetry.lastStatus(ex);
            String desc = last != null ? last.getStatus().getDescription() : "unavailable";
            throw new WiggleApiException(0, "server unavailable after " + RpcRetry.maxAttempts()
                    + " attempts: " + desc, last != null ? last : ex);
        } catch (StatusRuntimeException e) {
            throw mapStatus(e);
        }
    }

    private static WiggleApiException mapStatus(StatusRuntimeException e) {
        return new WiggleApiException(statusCode(e.getStatus()), e.getStatus().getDescription() != null
                ? e.getStatus().getDescription() : e.getMessage(), e);
    }

    private static int statusCode(Status status) {
        return switch (status.getCode()) {
            case INVALID_ARGUMENT -> 400;
            case NOT_FOUND -> 404;
            case FAILED_PRECONDITION, ALREADY_EXISTS -> 409;
            case UNAVAILABLE, DEADLINE_EXCEEDED -> 0;
            default -> 500;
        };
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static final class WiggleApiException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final int status;

        public WiggleApiException(int status, String message) { this(status, message, null); }

        public WiggleApiException(int status, String message, Throwable cause) {
            super(message, cause);
            this.status = status;
        }

        public int status() { return status; }

        public boolean isClientError() { return status >= 400 && status < 500; }
    }
}
