package dev.wiggle.server.grpc;

import dev.wiggle.proto.*;
import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.engine.EngineException;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.Rows.ServerNode;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
    /** Versions already announced at INFO, so N workers registering the same graph log it once. */
    private final Set<String> announced = ConcurrentHashMap.newKeySet();

    public GrpcApi(WorkflowEngine engine, ClusterManager cluster, int port, long maxLongPollMillis)
            throws IOException {
        this.engine = engine;
        this.cluster = cluster;
        this.maxLongPollMillis = maxLongPollMillis;
        this.pool = Executors.newVirtualThreadPerTaskExecutor();
        this.server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .executor(pool)
                .addService(this)
                .build();
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
            dev.wiggle.core.WorkflowDefinition def =
                    dev.wiggle.core.WorkflowDefinition.fromJson(ProtoJson.fromStruct(req.getDefinition()));
            engine.register(def);
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
            dev.wiggle.core.WorkflowDefinition def = engine.latestDefinition(req.getName())
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
            InstanceList.Builder out = InstanceList.newBuilder();
            for (dev.wiggle.core.InstanceView v : engine.list(workflow, status, limit)) out.addInstances(viewProto(v));
            return out.build();
        });
    }

    @Override
    public void getInstance(InstanceIdRequest req, StreamObserver<InstanceDetail> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetInstance id=" + req.getInstanceId());
        run(resp, () -> {
            dev.wiggle.core.InstanceView v = engine.instance(req.getInstanceId())
                    .orElseThrow(() -> EngineException.notFound("instance"));
            InstanceDetail.Builder out = InstanceDetail.newBuilder().setInstance(viewProto(v));
            for (dev.wiggle.server.store.Rows.Token t : engine.tokens(req.getInstanceId())) out.addTokens(tokenProto(t));
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
    public void pollTasks(PollRequest req, StreamObserver<TaskList> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollTasks worker=" + req.getWorkerId()
                + " queues=" + req.getQueuesList() + " max=" + req.getMax() + " waitMillis=" + req.getWaitMillis());
        run(resp, () -> {
            Set<String> queues = new LinkedHashSet<>(req.getQueuesList());
            int max = req.getMax() > 0 ? req.getMax() : 1;
            long lease = req.getLeaseMillis();
            long wait = Math.min(maxLongPollMillis, req.getWaitMillis());

            long deadline = System.currentTimeMillis() + wait;
            List<dev.wiggle.core.TaskActivation> tasks = engine.poll(req.getWorkerId(), queues, max, lease);
            while (tasks.isEmpty() && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(Math.min(100, Math.max(1, deadline - System.currentTimeMillis())));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                tasks = engine.poll(req.getWorkerId(), queues, max, lease);
            }
            List<dev.wiggle.core.TaskActivation> finalTasks = tasks;
            LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollTasks worker=" + req.getWorkerId()
                    + " returning " + finalTasks.size() + " task(s)");
            TaskList.Builder out = TaskList.newBuilder();
            for (dev.wiggle.core.TaskActivation t : tasks) out.addTasks(taskProto(t));
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
            List<WorkflowEngine.StepInput> steps = new ArrayList<>(req.getStepsCount());
            for (StepResult s : req.getStepsList()) {
                Object merge = s.getOutcomeCase() == StepResult.OutcomeCase.MERGE
                        ? ProtoJson.fromValue(s.getMerge()) : null;
                Boolean predicate = s.getOutcomeCase() == StepResult.OutcomeCase.PREDICATE_VALUE
                        ? s.getPredicateValue() : null;
                steps.add(new WorkflowEngine.StepInput(s.getNodeId(), merge, predicate));
            }
            WorkflowEngine.AdvanceOutcome out =
                    engine.advanceRun(req.getTaskId(), req.getLeaseOwner(), steps, req.getFinal());
            return AdvanceRunResult.newBuilder()
                    .setInstanceStatus(out.instanceStatus())
                    .setLeaseExpiresAt(out.leaseExpiresAt())
                    .setNextTaskId(out.nextTaskId() == null ? "" : out.nextTaskId())
                    .build();
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

    private static InstanceView viewProto(dev.wiggle.core.InstanceView v) {
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

    private static Token tokenProto(dev.wiggle.server.store.Rows.Token t) {
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
        return m.build();
    }

    private static TaskActivation taskProto(dev.wiggle.core.TaskActivation t) {
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
            LOG.log(System.Logger.Level.ERROR, "unhandled error", e);
            resp.onError(Status.INTERNAL
                    .withDescription(e.getClass().getSimpleName() + ": " + e.getMessage())
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
