package dev.wiggle.client;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.worker.PollResult;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.NodeKind;
import dev.wiggle.core.Tls;
import dev.wiggle.proto.*;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;

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

    public void register(Blueprint<?> blueprint) {
        call(() -> stub.registerWorkflow(WorkflowDefinition.newBuilder()
                .setDefinition(ProtoJson.toStruct(blueprint.definition().toJson()))
                .build()));
    }

    /**
     * The registered graph for {@code name} -- the server's source of truth for a workflow's step
     * names, kinds, and queues. Throws {@link WiggleApiException} with status 404 if the workflow was
     * never registered. Used by {@link Worker#handle} reconciliation.
     */
    public dev.wiggle.core.WorkflowDefinition getWorkflow(String name) {
        WorkflowDefinition def = call(() -> stub.getWorkflow(
                GetWorkflowRequest.newBuilder().setName(name).build()));
        return dev.wiggle.core.WorkflowDefinition.fromJson(ProtoJson.fromStruct(def.getDefinition()));
    }

    public String start(String workflow, Object context) {
        return start(workflow, context, null, null);
    }

    public <T> String start(Blueprint<T> blueprint, T context) {
        return start(blueprint.name(), blueprint.codec().encode(context), blueprint.version(), null);
    }

    public String start(String workflow, Object context, Integer version, String correlationId) {
        StartInstanceRequest.Builder req = StartInstanceRequest.newBuilder().setWorkflow(workflow);
        if (context != null) req.setContext(ProtoJson.toValue(context));
        if (version != null) req.setVersion(version);
        if (correlationId != null) req.setCorrelationId(correlationId);
        return call(() -> stub.startInstance(req.build())).getInstanceId();
    }

    public dev.wiggle.core.InstanceView instance(String instanceId) {
        InstanceDetail detail = call(() -> stub.getInstance(
                InstanceIdRequest.newBuilder().setInstanceId(instanceId).build()));
        return toInstanceView(detail.getInstance());
    }

    public dev.wiggle.core.InstanceView awaitCompletion(String instanceId, java.time.Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        dev.wiggle.core.InstanceView v = instance(instanceId);
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
        ClusterView v = call(() -> stub.getCluster(Empty.getDefaultInstance()));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("self", v.getSelf());
        m.put("leader", v.getLeader());
        List<Object> members = new ArrayList<>();
        for (ClusterMember cm : v.getMembersList()) {
            Map<String, Object> mm = new LinkedHashMap<>();
            mm.put("id", cm.getId());
            mm.put("name", cm.getName());
            mm.put("firstHeartbeat", cm.getFirstHeartbeat());
            mm.put("lastHeartbeat", cm.getLastHeartbeat());
            mm.put("workers", (long) cm.getWorkers());
            mm.put("leader", cm.getLeader());
            mm.put("alive", cm.getAlive());
            members.add(mm);
        }
        m.put("members", members);
        return m;
    }

    public PollResult poll(String workerId, Collection<String> queues, int max,
                           long leaseMillis, long waitMillis) {
        PollRequest req = PollRequest.newBuilder()
                .setWorkerId(workerId)
                .addAllQueues(queues)
                .setMax(max)
                .setLeaseMillis(leaseMillis)
                .setWaitMillis(waitMillis)
                .build();
        TaskList res = call(() -> stub.pollTasks(req));
        List<dev.wiggle.core.TaskActivation> out = new ArrayList<>(res.getTasksCount());
        for (dev.wiggle.proto.TaskActivation t : res.getTasksList()) out.add(toTaskActivation(t));
        return new PollResult(out, res.getRetryAfterMillis());
    }

    public void complete(String taskId, String leaseOwner, Object result) {
        TaskResultRequest.Builder req = TaskResultRequest.newBuilder()
                .setTaskId(taskId)
                .setLeaseOwner(leaseOwner);
        if (result != null) req.setResult(ProtoJson.toValue(result));
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
        if (payload != null) req.setPayload(ProtoJson.toValue(payload));
        call(() -> stub.signalInstance(req.build()));
    }

    /** Creates a recurring start on a fixed interval; returns the schedule id. */
    public String createSchedule(String workflow, java.time.Duration every, Object context) {
        CreateScheduleRequest.Builder req = CreateScheduleRequest.newBuilder()
                .setWorkflow(workflow).setEveryMillis(every.toMillis());
        if (context != null) req.setContext(ProtoJson.toValue(context));
        return call(() -> stub.createSchedule(req.build())).getId();
    }

    /** Creates a recurring start on a five-field cron expression (evaluated in UTC); returns the schedule id. */
    public String createCronSchedule(String workflow, String cron, Object context) {
        CreateScheduleRequest.Builder req = CreateScheduleRequest.newBuilder()
                .setWorkflow(workflow).setCron(cron);
        if (context != null) req.setContext(ProtoJson.toValue(context));
        return call(() -> stub.createSchedule(req.build())).getId();
    }

    /** All schedules on the server. {@code everyMillis} is 0 for cron schedules; {@code cron} is null for interval ones. */
    public List<ScheduleInfo> schedules() {
        return call(() -> stub.listSchedules(Empty.getDefaultInstance())).getSchedulesList().stream()
                .map(s -> new ScheduleInfo(s.getId(), s.getWorkflow(), s.getEveryMillis(),
                        s.getCron().isEmpty() ? null : s.getCron(), s.getNextFireAt(), s.getCreatedAt()))
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

    private static dev.wiggle.core.InstanceView toInstanceView(InstanceView v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", v.getId());
        m.put("workflow", v.getWorkflow());
        m.put("version", (long) v.getVersion());
        m.put("status", v.getStatus());
        if (v.hasTerminationReason()) m.put("terminationReason", v.getTerminationReason());
        if (v.hasError()) m.put("error", v.getError());
        if (v.hasContext()) m.put("context", ProtoJson.fromValue(v.getContext()));
        m.put("createdAt", v.getCreatedAt());
        m.put("updatedAt", v.getUpdatedAt());
        return dev.wiggle.core.InstanceView.fromJson(m);
    }

    /**
     * Reports a locally-executed run (LOCAL_SYNC/LOCAL_ASYNC) and returns whether to keep going.
     * {@code steps} carries, per node, either a task merge (Object) or a predicate value (Boolean).
     */
    public dev.wiggle.core.AdvanceResult advanceRun(String taskId, String leaseOwner,
                                                    List<StepReport> steps, boolean finalHandback) {
        AdvanceRunRequest.Builder req = AdvanceRunRequest.newBuilder()
                .setTaskId(taskId).setLeaseOwner(leaseOwner).setFinal(finalHandback);
        for (StepReport s : steps) {
            StepResult.Builder sr = StepResult.newBuilder().setNodeId(s.nodeId());
            if (s.predicateValue() != null) sr.setPredicateValue(s.predicateValue());
            else if (s.merge() != null) sr.setMerge(ProtoJson.toValue(s.merge()));
            req.addSteps(sr);
        }
        AdvanceRunResult res = call(() -> stub.advanceRun(req.build()));
        return new dev.wiggle.core.AdvanceResult(res.getInstanceStatus(), res.getLeaseExpiresAt(),
                res.getNextTaskId().isEmpty() ? null : res.getNextTaskId());
    }

    /** One reported step: exactly one of {@code merge} (task) or {@code predicateValue} (predicate). */
    public record StepReport(String nodeId, Object merge, Boolean predicateValue) {}

    private static dev.wiggle.core.TaskActivation toTaskActivation(dev.wiggle.proto.TaskActivation t) {
        return new dev.wiggle.core.TaskActivation(
                t.getTaskId(), t.getInstanceId(), t.getWorkflow(), t.getVersion(),
                t.getNodeId(), t.getStepName().isEmpty() ? null : t.getStepName(), t.getActivity(),
                NodeKind.valueOf(t.getKind()), t.getAttempt(), t.getLeaseExpiresAt(), t.getLeaseOwner(),
                t.hasContext() ? ProtoJson.fromValue(t.getContext()) : null,
                t.getExecutionMode().isEmpty()
                        ? dev.wiggle.core.ExecutionMode.SERVER
                        : dev.wiggle.core.ExecutionMode.valueOf(t.getExecutionMode()));
    }

    private interface Call<T> { T run(); }

    private <T> T call(Call<T> call) {
        try {
            return call.run();
        } catch (StatusRuntimeException e) {
            throw new WiggleApiException(statusCode(e.getStatus()), e.getStatus().getDescription() != null
                    ? e.getStatus().getDescription() : e.getMessage(), e);
        }
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
