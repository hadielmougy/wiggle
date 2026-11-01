package com.wiggle.server.api;

import com.wiggle.proto.*;
import com.wiggle.server.auth.Permissions;
import com.wiggle.server.auth.Scope;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.ReportOutcome;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows.ServerNode;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The control-plane API, independent of its transport: one handler per method of
 * {@link ControlPlane}. Workers pull work from here; nothing is ever pushed to them, so workers need
 * no inbound connectivity and can scale independently of the servers. A handler reports a refusal or
 * a failure by throwing; {@link HttpApi} maps what it throws to a status.
 */
public final class ControlPlaneApi {

    private static final System.Logger LOG = System.getLogger(ControlPlaneApi.class.getName());

    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final long maxLongPollMillis;
    private final MemoryGuard memory;
    private final Authorizer authz;
    private final com.wiggle.server.search.Search search;
    /** Versions already announced at INFO, so N workers registering the same graph log it once. */
    private final Set<String> announced = ConcurrentHashMap.newKeySet();
    private final Map<String, Handler<Message, Message>> handlers = new HashMap<>();

    public ControlPlaneApi(WorkflowEngine engine, ClusterManager cluster, long maxLongPollMillis, MemoryGuard memory,
                           Authorizer authz, com.wiggle.server.search.Search search) {
        this.authz = authz;
        this.search = search;
        this.engine = engine;
        this.cluster = cluster;
        this.maxLongPollMillis = maxLongPollMillis;
        this.memory = memory;
        bind(ControlPlane.HEALTH_CHECK, this::healthCheck);
        bind(ControlPlane.GET_CLUSTER, this::getCluster);
        bind(ControlPlane.LIST_WORKFLOWS, this::listWorkflows);
        bind(ControlPlane.GET_BACKLOG_COVERAGE, this::getBacklogCoverage);
        bind(ControlPlane.REGISTER_WORKFLOW, this::registerWorkflow);
        bind(ControlPlane.GET_WORKFLOW, this::getWorkflow);
        bind(ControlPlane.START_INSTANCE, this::startInstance);
        bind(ControlPlane.LIST_INSTANCES, this::listInstances);
        bind(ControlPlane.GET_INSTANCE, this::getInstance);
        bind(ControlPlane.CANCEL_INSTANCE, this::cancelInstance);
        bind(ControlPlane.SIGNAL_INSTANCE, this::signalInstance);
        bind(ControlPlane.CREATE_SCHEDULE, this::createSchedule);
        bind(ControlPlane.LIST_SCHEDULES, this::listSchedules);
        bind(ControlPlane.DELETE_SCHEDULE, this::deleteSchedule);
        bind(ControlPlane.POLL_TASKS, this::pollTasks);
        bind(ControlPlane.REPORT_STEPS, this::reportSteps);
        bind(ControlPlane.FAIL_TASK, this::failTask);
        bind(ControlPlane.HEARTBEAT_TASK, this::heartbeatTask);
        bind(ControlPlane.POLL_EVENTS, this::pollEvents);
        bind(ControlPlane.ACK_EVENTS, this::ackEvents);
        bind(ControlPlane.GET_STEP_STATS, this::getStepStats);
        bind(ControlPlane.SEARCH_INSTANCES, this::searchInstances);
        if (handlers.size() != ControlPlane.METHODS.size()) {
            throw new IllegalStateException("a control-plane method has no handler");
        }
    }

    /** Answers one call of {@code method}. */
    Message call(ControlPlane.Method<?, ?> method, Message request) throws Exception {
        return handlers.get(method.name()).call(request);
    }

    interface Handler<Q, R> { R call(Q request) throws Exception; }

    @SuppressWarnings("unchecked")
    private <Q extends Message, R extends Message> void bind(ControlPlane.Method<Q, R> method, Handler<Q, R> handler) {
        handlers.put(method.name(), (Handler<Message, Message>) (Handler<?, ?>) handler);
    }

    HealthStatus healthCheck(Empty req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, "rpc HealthCheck");
        return HealthStatus.newBuilder()
                .setStatus("UP")
                .setNode(cluster.nodeId())
                .setLeader(cluster.isLeader())
                .build();
    }

    ClusterView getCluster(Empty req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, "rpc GetCluster");
        authz.require(Permissions.READ, null);
        return clusterView();
    }

    WorkflowNames listWorkflows(Empty req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, "rpc ListWorkflows");
        authz.requireAny(Permissions.READ);
        Scope readable = authz.scope(Permissions.READ);
        return WorkflowNames.newBuilder()
                .addAllWorkflows(engine.workflowNames().stream().filter(readable::matches).toList()).build();
    }

    RegisterWorkflowResult registerWorkflow(WorkflowDefinition req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, "rpc RegisterWorkflow");
        authz.requireAny(Permissions.WORKFLOW_REGISTER);
        Map<String, Object> raw = com.wiggle.core.Json.asObject(ProtoJson.fromStruct(req.getDefinition()));
        if ("OBSERVED".equals(raw.get("executionMode"))) {
            throw new IllegalArgumentException("OBSERVED execution was removed; register the workflow "
                    + "as SERVER, LOCAL_SYNC or LOCAL_ASYNC");
        }
        com.wiggle.core.WorkflowDefinition def = com.wiggle.core.WorkflowDefinition.fromJson(raw);
        authz.require(Permissions.WORKFLOW_REGISTER, def.name());
        for (com.wiggle.core.Node n : def.nodes().values()) {
            if (n.kind() == com.wiggle.core.NodeKind.SUB_WORKFLOW) authz.require(Permissions.INSTANCE_START, n.activity());
        }
        for (String queue : def.workerQueues()) authz.require(Permissions.WORKFLOW_REGISTER, queue);
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
    }

    WorkflowDefinition getWorkflow(GetWorkflowRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetWorkflow name=" + req.getName());
        authz.require(Permissions.READ, req.getName());
        com.wiggle.core.WorkflowDefinition def = (req.hasVersion()
                ? engine.definition(req.getName(), req.getVersion())
                : engine.latestDefinition(req.getName()))
                .orElseThrow(() -> EngineException.notFound("workflow"));
        return WorkflowDefinition.newBuilder().setDefinition(ProtoJson.toStruct(def.toJson())).build();
    }

    StartInstanceResult startInstance(StartInstanceRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc StartInstance workflow=" + req.getWorkflow()
                + " version=" + (req.hasVersion() ? req.getVersion() : "latest")
                + " correlationId=" + (req.hasCorrelationId() ? req.getCorrelationId() : null));
        authz.require(Permissions.INSTANCE_START, req.getWorkflow());
        Integer version = req.hasVersion() ? req.getVersion() : null;
        String correlationId = req.hasCorrelationId() ? req.getCorrelationId() : null;
        Object context = req.hasContext() ? ProtoJson.fromValue(req.getContext()) : null;
        String id = engine.start(req.getWorkflow(), version, context, correlationId);
        return StartInstanceResult.newBuilder().setInstanceId(id).setWorkflow(req.getWorkflow()).build();
    }

    InstanceList listInstances(ListInstancesRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc ListInstances workflow="
                + (req.hasWorkflow() ? req.getWorkflow() : null) + " status="
                + (req.hasStatus() ? req.getStatus() : null) + " limit=" + req.getLimit());
        String workflow = req.hasWorkflow() ? req.getWorkflow() : null;
        String status = req.hasStatus() ? req.getStatus() : null;
        Scope readable;
        if (req.hasCorrelationId()) {
            authz.requireAny(Permissions.READ);
            readable = authz.scope(Permissions.READ);
        } else {
            authz.require(Permissions.READ, workflow);
            readable = Scope.ALL;
        }
        int limit = req.getLimit() > 0 ? req.getLimit() : 50;
        List<com.wiggle.core.InstanceView> views = req.hasCorrelationId()
                ? engine.findByCorrelation(req.getCorrelationId(), limit)
                : engine.list(workflow, status, limit);
        InstanceList.Builder out = InstanceList.newBuilder();
        for (com.wiggle.core.InstanceView v : views) if (readable.matches(v.workflow())) out.addInstances(viewProto(v));
        return out.build();
    }

    InstanceDetail getInstance(InstanceIdRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetInstance id=" + req.getInstanceId());
        authz.requireAny(Permissions.READ);
        com.wiggle.core.InstanceView v = engine.instance(req.getInstanceId())
                .orElseThrow(() -> EngineException.notFound("instance"));
        authz.require(Permissions.READ, v.workflow());
        InstanceDetail.Builder out = InstanceDetail.newBuilder().setInstance(viewProto(v));
        for (com.wiggle.server.store.Rows.Token t : engine.tokens(req.getInstanceId())) out.addTokens(tokenProto(t));
        return out.build();
    }

    CancelInstanceResult cancelInstance(CancelInstanceRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc CancelInstance id=" + req.getInstanceId()
                + " reason=" + req.getReason());
        authz.requireAny(Permissions.INSTANCE_CANCEL);
        authz.require(Permissions.INSTANCE_CANCEL, workflowOf(req.getInstanceId()));
        engine.cancel(req.getInstanceId(),
                req.getReason().isEmpty() ? "cancelled via API" : req.getReason());
        return CancelInstanceResult.newBuilder().setCancelled(req.getInstanceId()).build();
    }

    Ack signalInstance(SignalRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc SignalInstance id=" + req.getInstanceId()
                + " signal=" + req.getSignal());
        authz.requireAny(Permissions.INSTANCE_SIGNAL);
        authz.require(Permissions.INSTANCE_SIGNAL, workflowOf(req.getInstanceId()));
        Object payload = req.hasPayload() ? ProtoJson.fromValue(req.getPayload()) : null;
        engine.signal(req.getInstanceId(), req.getSignal(), payload);
        return Ack.newBuilder().setOk(true).build();
    }

    ScheduleView createSchedule(CreateScheduleRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc CreateSchedule workflow=" + req.getWorkflow());
        authz.require(Permissions.SCHEDULE_WRITE, req.getWorkflow());
        Object context = req.hasContext() ? ProtoJson.fromValue(req.getContext()) : null;
        String id = req.getCadenceCase() == CreateScheduleRequest.CadenceCase.CRON
                ? engine.createCronSchedule(req.getWorkflow(), req.getCron(), context)
                : engine.createSchedule(req.getWorkflow(),
                        java.time.Duration.ofMillis(req.getEveryMillis()), context);
        return engine.schedules().stream().filter(s -> s.id.equals(id)).findFirst()
                .map(ControlPlaneApi::scheduleView).orElseThrow();
    }

    ScheduleList listSchedules(Empty req) throws Exception {
        authz.requireAny(Permissions.READ);
        Scope readable = authz.scope(Permissions.READ);
        ScheduleList.Builder out = ScheduleList.newBuilder();
        engine.schedules().forEach(s -> { if (readable.matches(s.workflow)) out.addSchedules(scheduleView(s)); });
        return out.build();
    }

    Ack deleteSchedule(ScheduleIdRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc DeleteSchedule id=" + req.getId());
        authz.requireAny(Permissions.SCHEDULE_WRITE);
        String workflow = engine.schedules().stream().filter(x -> x.id.equals(req.getId()))
                .map(x -> x.workflow).findFirst().orElse(null);
        if (workflow != null) authz.require(Permissions.SCHEDULE_WRITE, workflow);
        engine.deleteSchedule(req.getId());
        return Ack.newBuilder().setOk(true).build();
    }

    /** The queues the current caller may serve tasks of, or null for every one. */
    private java.util.function.Predicate<String> servedQueues() {
        Scope queues = authz.scope(Permissions.TASK_POLL);
        return queues.all() ? null : queues::matches;
    }

    /** The workflow of instance {@code id}, which scopes what may be done to it; 404 when there is none. */
    private String workflowOf(String id) {
        return engine.instance(id).map(com.wiggle.core.InstanceView::workflow)
                .orElseThrow(() -> EngineException.notFound("instance"));
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

    BacklogCoverage getBacklogCoverage(BacklogCoverageRequest req) throws Exception {
        authz.requireAny(Permissions.READ);
        Scope readable = authz.scope(Permissions.READ);
        int max = req.getMax() > 0 ? Math.min(req.getMax(), 500) : 100;
        BacklogCoverage.Builder out = BacklogCoverage.newBuilder()
                .setLivePollers(engine.livePollers().size());
        for (com.wiggle.server.store.Rows.BacklogSlice s : engine.backlog(max)) {
            if (!readable.matches(s.workflow())) continue;
            WorkflowEngine.SliceCoverage c = engine.coverage(s);
            BacklogSlice.Builder slice = BacklogSlice.newBuilder()
                    .setWorkflow(s.workflow())
                    .setVersion(s.version())
                    .setQueue(s.queue() == null ? "" : s.queue())
                    .setReadyCount(s.readyCount())
                    .setOldestAvailableAt(s.oldestAvailableAt())
                    .setCovered(c.state() != com.wiggle.server.engine.PollerRegistry.Coverage.State.UNCOVERED)
                    .setCoverage(c.state().name())
                    .addAllCriteria(c.criteria());
            if (c.claimable() != null) slice.setClaimable(c.claimable());
            out.addSlices(slice.build());
        }
        return out.build();
    }

    TaskList pollTasks(PollRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollTasks worker=" + req.getWorkerId()
                + " queues=" + req.getQueuesList() + " max=" + req.getMax() + " waitMillis=" + req.getWaitMillis());
        if (req.getCriteriaCount() > 0 && req.getQueuesCount() > 0) {
            throw EngineException.badRequest("a poll carries queues or criteria, not both");
        }
        List<com.wiggle.core.criteria.QueueCriteria> criteria = CriteriaWire.decode(req.getCriteriaList());
        List<String> polled = criteria.isEmpty() ? req.getQueuesList()
                : criteria.stream().map(com.wiggle.core.criteria.QueueCriteria::queue).toList();
        if (polled.isEmpty()) authz.require(Permissions.TASK_POLL, null);   // every queue
        for (String q : polled) authz.require(Permissions.TASK_POLL, q);
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

        com.wiggle.server.store.ClaimFilter filter = criteria.isEmpty()
                ? com.wiggle.server.store.ClaimFilter.byName(polled, versionFilter(req))
                : new com.wiggle.server.store.ClaimFilter(criteria, versionFilter(req));
        int max = req.getMax() > 0 ? req.getMax() : 1;
        long lease = req.getLeaseMillis();
        long deadline = System.currentTimeMillis() + Math.min(maxLongPollMillis, req.getWaitMillis());
        // An interrupted call means the server is stopping; don't claim work nobody will receive.
        List<com.wiggle.core.TaskActivation> tasks =
                engine.poll(req.getWorkerId(), filter, max, lease, deadline, Thread.currentThread()::isInterrupted);
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollTasks worker=" + req.getWorkerId()
                + " returning " + tasks.size() + " task(s)");
        TaskList.Builder out = TaskList.newBuilder();
        for (com.wiggle.core.TaskActivation t : tasks) out.addTasks(taskProto(t));
        return out.build();
    }

    Ack failTask(TaskFailureRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc FailTask taskId=" + req.getTaskId()
                + " leaseOwner=" + req.getLeaseOwner() + " retryable=" + req.getRetryable()
                + " message=" + req.getMessage());
        authz.requireAny(Permissions.TASK_POLL);
        engine.fail(req.getTaskId(), req.getLeaseOwner(), req.getMessage(), req.getRetryable(), servedQueues());
        return Ack.newBuilder().setOk(true).build();
    }

    HeartbeatResult heartbeatTask(HeartbeatRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc HeartbeatTask taskId=" + req.getTaskId()
                + " leaseOwner=" + req.getLeaseOwner() + " extendMillis=" + req.getExtendMillis());
        authz.requireAny(Permissions.TASK_POLL);
        long until = engine.extendLease(req.getTaskId(), req.getLeaseOwner(), req.getExtendMillis(), servedQueues());
        return HeartbeatResult.newBuilder().setLeaseExpiresAt(until).build();
    }

    ReportStepsResult reportSteps(ReportStepsRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc ReportSteps runs=" + req.getRunsCount());
        authz.requireAny(Permissions.TASK_POLL);
        List<WorkflowEngine.Run> runs = new ArrayList<>(req.getRunsCount());
        for (ReportedRun r : req.getRunsList()) {
            List<WorkflowEngine.StepInput> steps = stepInputs(r);
            for (WorkflowEngine.StepInput step : steps) {
                for (com.wiggle.core.CreatedBranch b : step.branches()) requireSubFlowStarts(b.steps());
            }
            runs.add(new WorkflowEngine.Run(r.getTaskId(), r.getLeaseOwner(), steps, r.getFinal(),
                    CriteriaWire.decode(r.getCriteriaList())));
        }
        Map<String, WorkflowEngine.RunResult> results = engine.report(runs, servedQueues());
        ReportStepsResult.Builder out = ReportStepsResult.newBuilder();
        for (WorkflowEngine.Run submitted : runs) {
            WorkflowEngine.RunResult r = results.get(submitted.startTaskId());
            RunOutcome.Builder one = RunOutcome.newBuilder().setTaskId(submitted.startTaskId());
            if (r != null && r.ok()) {
                one.setApplied(applied(r.outcome()));
            } else if (r != null) {
                one.setErrorStatus(r.errorStatus()).setError(r.error() == null ? "" : r.error());
            } else {
                one.setErrorStatus(500).setError("no answer for run " + submitted.startTaskId());
            }
            out.addResults(one);
        }
        return out.build();
    }

    /**
     * A branch that runs a sub-flow starts that workflow, so the reporter must be allowed to start it,
     * exactly as if it had called {@code StartWorkflow} -- a worker serving one tenant's queues must
     * not start another tenant's workflows through the branches it creates.
     */
    private void requireSubFlowStarts(List<com.wiggle.core.CreatedBranch.BranchStep> steps) {
        for (com.wiggle.core.CreatedBranch.BranchStep st : steps) {
            if (st.kind() == com.wiggle.core.NodeKind.SUB_WORKFLOW && st.workflow() != null) {
                authz.require(Permissions.INSTANCE_START, st.workflow());
            }
            requireSubFlowStarts(st.escalation());
            requireSubFlowStarts(st.body());
            for (List<com.wiggle.core.CreatedBranch.BranchStep> arm : st.arms()) requireSubFlowStarts(arm);
            for (com.wiggle.core.CreatedBranch.BranchStep.Case c : st.cases()) requireSubFlowStarts(c.steps());
        }
    }

    private static RunApplied.Builder applied(ReportOutcome out) {
        return RunApplied.newBuilder()
                .setInstanceStatus(out.instanceStatus())
                .setLeaseExpiresAt(out.leaseExpiresAt())
                .setNextTaskId(out.nextTaskId() == null ? "" : out.nextTaskId());
    }

    private static List<WorkflowEngine.StepInput> stepInputs(ReportedRun req) {
        return stepInputs(req.getStepsList());
    }

    private static List<WorkflowEngine.StepInput> stepInputs(List<StepResult> reported) {
        List<WorkflowEngine.StepInput> steps = new ArrayList<>(reported.size());
        for (StepResult s : reported) {
            Object merge = s.getOutcomeCase() == StepResult.OutcomeCase.MERGE
                    ? ProtoJson.fromValue(s.getMerge()) : null;
            Boolean predicate = s.getOutcomeCase() == StepResult.OutcomeCase.PREDICATE_VALUE
                    ? s.getPredicateValue() : null;
            steps.add(new WorkflowEngine.StepInput(s.getNodeId(), merge, predicate,
                    s.getStartedAt() == 0 ? null : s.getStartedAt(),
                    s.getFinishedAt() == 0 ? null : s.getFinishedAt(),
                    emitted(s.getEventsList()), branches(s.getBranchesList())));
        }
        return steps;
    }

    /** Created branches as reported. A step kind this build does not know arrives as null, and the
     *  engine refuses the branch by failing the step, not the whole report. */
    private static List<com.wiggle.core.CreatedBranch> branches(List<CreatedBranch> reported) {
        List<com.wiggle.core.CreatedBranch> out = new ArrayList<>(reported.size());
        for (CreatedBranch b : reported) {
            out.add(new com.wiggle.core.CreatedBranch(b.hasInput() ? ProtoJson.fromValue(b.getInput()) : null,
                    b.getKey().isEmpty() ? null : b.getKey(), branchSteps(b.getStepsList())));
        }
        return out;
    }

    private static List<com.wiggle.core.CreatedBranch.BranchStep> branchSteps(List<BranchStep> reported) {
        List<com.wiggle.core.CreatedBranch.BranchStep> steps = new ArrayList<>(reported.size());
        for (BranchStep st : reported) {
            List<List<com.wiggle.core.CreatedBranch.BranchStep>> arms = new ArrayList<>(st.getArmsCount());
            for (BranchArm arm : st.getArmsList()) arms.add(branchSteps(arm.getStepsList()));
            List<com.wiggle.core.CreatedBranch.BranchStep.Case> cases = new ArrayList<>(st.getCasesCount());
            for (BranchCase c : st.getCasesList()) {
                cases.add(new com.wiggle.core.CreatedBranch.BranchStep.Case(
                        c.getGuard().isEmpty() ? null : c.getGuard(), branchSteps(c.getStepsList())));
            }
            steps.add(new com.wiggle.core.CreatedBranch.BranchStep(
                    st.getName().isEmpty() ? null : st.getName(),
                    nodeKind(st.getKind()),
                    st.getCompensable(),
                    st.getQueue().isEmpty() ? null : st.getQueue(),
                    st.hasRetry() ? com.wiggle.core.RetryPolicy.fromJson(ProtoJson.fromValue(st.getRetry())) : null,
                    st.getSleepMillis(),
                    st.getCombine().isEmpty() ? null : st.getCombine(),
                    st.getWorkflow().isEmpty() ? null : st.getWorkflow(),
                    branchSteps(st.getEscalationList()),
                    arms,
                    branchSteps(st.getBodyList()),
                    st.getLoopBudget(),
                    cases));
        }
        return steps;
    }

    private static com.wiggle.core.NodeKind nodeKind(String name) {
        try {
            return com.wiggle.core.NodeKind.valueOf(name);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /** The wire's emitted events, validated by {@link com.wiggle.core.EmittedEvent}'s own contract. */
    private static List<com.wiggle.core.EmittedEvent> emitted(List<EmittedEvent> reported) {
        List<com.wiggle.core.EmittedEvent> out = new ArrayList<>(reported.size());
        for (EmittedEvent e : reported) {
            out.add(new com.wiggle.core.EmittedEvent(e.getType(),
                    e.hasPayload() ? ProtoJson.fromValue(e.getPayload()) : null));
        }
        return out;
    }

    StepStats getStepStats(StepStatsRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetStepStats workflow=" + req.getWorkflow()
                + " version=" + req.getVersion() + " since=" + req.getSince());
        authz.require(Permissions.READ, req.getWorkflow());
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
    }

    SearchResult searchInstances(SearchRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc SearchInstances text=" + req.getText());
        authz.requireAny(Permissions.READ);
        if (search == null) {
            throw EngineException.conflict("search is not enabled: give a shard the search role, "
                    + "or set WIGGLE_SEARCH_ENABLED=true on a single database");
        }
        String workflow = req.hasWorkflow() ? req.getWorkflow() : null;
        String status = req.hasStatus() ? req.getStatus() : null;
        Long from = req.getUpdatedFrom() > 0 ? req.getUpdatedFrom() : null;
        Long to = req.getUpdatedTo() > 0 ? req.getUpdatedTo() : null;
        int limit = req.getLimit() > 0 ? req.getLimit() : 20;
        SearchResult.Builder out = SearchResult.newBuilder();
        com.wiggle.server.search.Search.Result r = search.search(req.getText(), workflow, status, from, to,
                limit, req.getPartialOk(), authz.scope(Permissions.READ), attributeFilters(req));
        out.setPartial(r.partial());
        for (com.wiggle.server.search.Search.Hit h : r.hits()) {
            out.addHits(SearchHit.newBuilder().setInstanceId(h.instanceId()).setWorkflow(h.workflow())
                    .setVersion(h.version()).setStatus(h.status())
                    .setCorrelationId(h.correlationId() == null ? "" : h.correlationId())
                    .setCreatedAt(h.createdAt()).setUpdatedAt(h.updatedAt()).setScore(h.score())
                    .setPurged(h.purged()));
        }
        return out.build();
    }

    private static List<com.wiggle.server.store.Rows.AttrFilter> attributeFilters(SearchRequest req) {
        List<com.wiggle.server.store.Rows.AttrFilter> out = new ArrayList<>();
        try {
            for (SearchAttributeFilter f : req.getAttributesList()) {
                Object value = switch (f.getValue().getVCase()) {
                    case TEXT -> f.getValue().getText();
                    case INTEGER -> f.getValue().getInteger();
                    case DECIMAL -> new java.math.BigDecimal(f.getValue().getDecimal());
                    case FLAG -> f.getValue().getFlag();
                    case V_NOT_SET -> throw new IllegalArgumentException("attribute '" + f.getName() + "' has no value");
                };
                out.add(new com.wiggle.server.store.Rows.AttrFilter(f.getName(),
                        com.wiggle.core.criteria.Op.valueOf(f.getOp()), value));
            }
            if (out.size() > com.wiggle.server.store.Rows.SearchQuery.MAX_ATTRIBUTE_FILTERS) {
                throw new IllegalArgumentException("a search filters on at most "
                        + com.wiggle.server.store.Rows.SearchQuery.MAX_ATTRIBUTE_FILTERS + " attributes");
            }
        } catch (IllegalArgumentException e) {
            throw EngineException.badRequest(e.getMessage());
        }
        return out;
    }

    EventList pollEvents(PollEventsRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollEvents consumer=" + req.getConsumer()
                + " max=" + req.getMax() + " waitMillis=" + req.getWaitMillis());
        authz.require(Permissions.EVENT_READ, null);
        if (memory.rejectPoll()) {
            long retryAfter = memory.retryAfterMillis();
            LOG.log(System.Logger.Level.WARNING, () -> "memory pressure: rejecting event poll from "
                    + req.getConsumer() + ", retry after " + retryAfter + "ms");
            return EventList.newBuilder().setRetryAfterMillis(retryAfter).build();
        }
        long deadline = System.currentTimeMillis() + Math.min(maxLongPollMillis, req.getWaitMillis());
        List<com.wiggle.core.EventView> events =
                engine.pollEvents(req.getConsumer(), req.getMax(), req.getStartFrom(), deadline, Thread.currentThread()::isInterrupted);
        EventList.Builder out = EventList.newBuilder();
        for (com.wiggle.core.EventView e : events) out.addEvents(eventProto(e));
        return out.build();
    }

    Ack ackEvents(AckEventsRequest req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc AckEvents consumer=" + req.getConsumer()
                + " ackedSeq=" + req.getAckedSeq() + " ackedCursor=" + req.getAckedCursor());
        authz.require(Permissions.EVENT_READ, null);
        if (req.getAckedCursor().isEmpty()) engine.ackEvents(req.getConsumer(), req.getAckedSeq());
        else engine.ackEvents(req.getConsumer(), req.getAckedCursor());
        return Ack.newBuilder().setOk(true).build();
    }

    private static EventView eventProto(com.wiggle.core.EventView e) {
        EventView.Builder b = EventView.newBuilder()
                .setSeq(e.seq()).setInstanceId(e.instanceId()).setWorkflow(e.workflow())
                .setVersion(e.version()).setType(e.type()).setCreatedAt(e.createdAt()).setShard(e.shard());
        if (e.cursor() != null) b.setCursor(e.cursor());
        if (e.correlationId() != null) b.setCorrelationId(e.correlationId());
        if (e.nodeId() != null) b.setNodeId(e.nodeId());
        if (e.payload() != null && !e.payload().isEmpty()) b.setPayload(ProtoJson.toValue(e.payload()));
        return b.build();
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
        if (t.stepInput != null) m.setInput(ProtoJson.toValue(com.wiggle.core.Json.parse(t.stepInput)));
        if (t.stepOutput != null) m.setOutput(ProtoJson.toValue(com.wiggle.core.Json.parse(t.stepOutput)));
        m.setCreatedAt(t.createdAt);
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
        if (t.collectKey() != null) m.setCollectKey(t.collectKey());
        m.addAllArmNames(t.armNames());
        if (t.correlationId() != null) m.setCorrelationId(t.correlationId());
        m.setInstanceCreatedAt(t.instanceCreatedAt());
        return m.build();
    }
}
