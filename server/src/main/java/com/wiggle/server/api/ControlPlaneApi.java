package com.wiggle.server.api;

import com.wiggle.core.wire.ControlPlane;
import com.wiggle.core.wire.Msg;
import com.wiggle.server.auth.Permissions;
import com.wiggle.server.auth.Scope;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.ReportOutcome;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows.ServerNode;

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
    private final Map<String, Handler> handlers = new HashMap<>();

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
    Msg call(ControlPlane.Method method, Msg request) throws Exception {
        return handlers.get(method.name()).call(request);
    }

    interface Handler { Msg call(Msg request) throws Exception; }

    private void bind(ControlPlane.Method method, Handler handler) {
        handlers.put(method.name(), handler);
    }

    Msg healthCheck(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, "rpc HealthCheck");
        return Msg.create()
                .put("status", "UP")
                .put("node", cluster.nodeId())
                .put("leader", cluster.isLeader());
    }

    Msg getCluster(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, "rpc GetCluster");
        authz.require(Permissions.READ, null);
        return clusterView();
    }

    Msg listWorkflows(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, "rpc ListWorkflows");
        authz.requireAny(Permissions.READ);
        Scope readable = authz.scope(Permissions.READ);
        return Msg.create()
                .put("workflows", engine.workflowNames().stream().filter(readable::matches).toList());
    }

    Msg registerWorkflow(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, "rpc RegisterWorkflow");
        authz.requireAny(Permissions.WORKFLOW_REGISTER);
        Map<String, Object> raw = req.obj("definition");
        if (raw == null) throw EngineException.badRequest("a registration carries a definition");
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
        if (req.bool("force") && !ServerConfig.allowGraphReplace()) {
            throw EngineException.conflict("replacing the graph of '" + def.key()
                    + "' was requested but this server does not allow it; set "
                    + "WIGGLE_ALLOW_GRAPH_REPLACE=true (development only) or publish a new version");
        }
        engine.register(def, req.bool("force"));
        if (announced.add(def.key())) {
            LOG.log(System.Logger.Level.INFO, () -> "registered workflow " + def.key()
                    + " (" + def.nodes().size() + " nodes, mode=" + def.executionMode() + ")");
        }
        return Msg.create()
                .put("name", def.name())
                .put("version", String.valueOf(def.version()))
                .put("nodes", def.nodes().size());
    }

    Msg getWorkflow(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetWorkflow name=" + req.str("name"));
        authz.require(Permissions.READ, req.str("name"));
        com.wiggle.core.WorkflowDefinition def = (req.has("version")
                ? engine.definition(req.str("name"), req.integer("version"))
                : engine.latestDefinition(req.str("name")))
                .orElseThrow(() -> EngineException.notFound("workflow"));
        return Msg.create().put("definition", def.toJson());
    }

    Msg startInstance(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc StartInstance workflow=" + req.str("workflow")
                + " version=" + (req.has("version") ? req.integer("version") : "latest")
                + " correlationId=" + (req.has("correlationId") ? req.str("correlationId") : null));
        authz.require(Permissions.INSTANCE_START, req.str("workflow"));
        Integer version = req.has("version") ? req.integer("version") : null;
        String correlationId = req.has("correlationId") ? req.str("correlationId") : null;
        Object context = req.has("context") ? req.value("context") : null;
        String id = engine.start(req.str("workflow"), version, context, correlationId);
        return Msg.create().put("instanceId", id).put("workflow", req.str("workflow"));
    }

    Msg listInstances(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc ListInstances workflow="
                + (req.has("workflow") ? req.str("workflow") : null) + " status="
                + (req.has("status") ? req.str("status") : null) + " limit=" + req.integer("limit"));
        String workflow = req.has("workflow") ? req.str("workflow") : null;
        String status = req.has("status") ? req.str("status") : null;
        Scope readable;
        if (req.has("correlationId")) {
            authz.requireAny(Permissions.READ);
            readable = authz.scope(Permissions.READ);
        } else {
            authz.require(Permissions.READ, workflow);
            readable = Scope.ALL;
        }
        int limit = req.integer("limit") > 0 ? req.integer("limit") : 50;
        List<com.wiggle.core.InstanceView> views = req.has("correlationId")
                ? engine.findByCorrelation(req.str("correlationId"), limit)
                : engine.list(workflow, status, limit);
        Msg out = Msg.create();
        for (com.wiggle.core.InstanceView v : views) if (readable.matches(v.workflow())) out.add("instances", viewMsg(v));
        return out;
    }

    Msg getInstance(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetInstance id=" + req.str("instanceId"));
        authz.requireAny(Permissions.READ);
        com.wiggle.core.InstanceView v = engine.instance(req.str("instanceId"))
                .orElseThrow(() -> EngineException.notFound("instance"));
        authz.require(Permissions.READ, v.workflow());
        Msg out = Msg.create().put("instance", viewMsg(v));
        for (com.wiggle.server.store.Rows.Token t : engine.tokens(req.str("instanceId"))) out.add("tokens", tokenMsg(t));
        return out;
    }

    Msg cancelInstance(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc CancelInstance id=" + req.str("instanceId")
                + " reason=" + req.str("reason"));
        authz.requireAny(Permissions.INSTANCE_CANCEL);
        authz.require(Permissions.INSTANCE_CANCEL, workflowOf(req.str("instanceId")));
        engine.cancel(req.str("instanceId"),
                req.str("reason").isEmpty() ? "cancelled via API" : req.str("reason"));
        return Msg.create().put("cancelled", req.str("instanceId"));
    }

    Msg signalInstance(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc SignalInstance id=" + req.str("instanceId")
                + " signal=" + req.str("signal"));
        authz.requireAny(Permissions.INSTANCE_SIGNAL);
        authz.require(Permissions.INSTANCE_SIGNAL, workflowOf(req.str("instanceId")));
        Object payload = req.has("payload") ? req.value("payload") : null;
        engine.signal(req.str("instanceId"), req.str("signal"), payload);
        return Msg.create().put("ok", true);
    }

    Msg createSchedule(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc CreateSchedule workflow=" + req.str("workflow"));
        authz.require(Permissions.SCHEDULE_WRITE, req.str("workflow"));
        Object context = req.has("context") ? req.value("context") : null;
        String id = CriteriaWire.oneOf(req, "a schedule's cadence", "everyMillis", "cron").equals("cron")
                ? engine.createCronSchedule(req.str("workflow"), req.str("cron"), context)
                : engine.createSchedule(req.str("workflow"),
                        java.time.Duration.ofMillis(req.lng("everyMillis")), context);
        return engine.schedules().stream().filter(s -> s.id.equals(id)).findFirst()
                .map(ControlPlaneApi::scheduleView).orElseThrow();
    }

    Msg listSchedules(Msg req) throws Exception {
        authz.requireAny(Permissions.READ);
        Scope readable = authz.scope(Permissions.READ);
        Msg out = Msg.create();
        engine.schedules().forEach(s -> { if (readable.matches(s.workflow)) out.add("schedules", scheduleView(s)); });
        return out;
    }

    Msg deleteSchedule(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc DeleteSchedule id=" + req.str("id"));
        authz.requireAny(Permissions.SCHEDULE_WRITE);
        String workflow = engine.schedules().stream().filter(x -> x.id.equals(req.str("id")))
                .map(x -> x.workflow).findFirst().orElse(null);
        if (workflow != null) authz.require(Permissions.SCHEDULE_WRITE, workflow);
        engine.deleteSchedule(req.str("id"));
        return Msg.create().put("ok", true);
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

    private static Msg scheduleView(com.wiggle.server.store.Rows.Schedule s) {
        Msg b = Msg.create()
                .put("id", s.id).put("workflow", s.workflow)
                .put("everyMillis", s.intervalMillis)
                .put("nextFireAt", s.nextFireAt).put("createdAt", s.createdAt);
        if (s.cron != null) b.put("cron", s.cron);
        return b;
    }

    /** The (workflow, version) pairs a version-scoped worker serves; empty means every version. */
    private static java.util.Set<com.wiggle.core.WorkflowVersion> versionFilter(Msg req) {
        if (req.msgs("versions").isEmpty()) return java.util.Set.of();
        java.util.Set<com.wiggle.core.WorkflowVersion> out = new java.util.LinkedHashSet<>();
        for (Msg v : req.msgs("versions")) {
            out.add(new com.wiggle.core.WorkflowVersion(v.str("workflow"), v.integer("version")));
        }
        return out;
    }

    Msg getBacklogCoverage(Msg req) throws Exception {
        authz.requireAny(Permissions.READ);
        Scope readable = authz.scope(Permissions.READ);
        int max = req.integer("max") > 0 ? Math.min(req.integer("max"), 500) : 100;
        Msg out = Msg.create()
                .put("livePollers", engine.livePollers().size());
        for (com.wiggle.server.store.Rows.BacklogSlice s : engine.backlog(max)) {
            if (!readable.matches(s.workflow())) continue;
            WorkflowEngine.SliceCoverage c = engine.coverage(s);
            Msg slice = Msg.create()
                    .put("workflow", s.workflow())
                    .put("version", s.version())
                    .put("queue", s.queue() == null ? "" : s.queue())
                    .put("readyCount", s.readyCount())
                    .put("oldestAvailableAt", s.oldestAvailableAt())
                    .put("covered", c.state() != com.wiggle.server.engine.PollerRegistry.Coverage.State.UNCOVERED)
                    .put("coverage", c.state().name())
                    .put("criteria", c.criteria());
            if (c.claimable() != null) slice.put("claimable", c.claimable());
            out.add("slices", slice);
        }
        return out;
    }

    Msg pollTasks(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollTasks worker=" + req.str("workerId")
                + " queues=" + req.strs("queues") + " max=" + req.integer("max") + " waitMillis=" + req.lng("waitMillis"));
        if (!req.msgs("criteria").isEmpty() && !req.strs("queues").isEmpty()) {
            throw EngineException.badRequest("a poll carries queues or criteria, not both");
        }
        List<com.wiggle.core.criteria.QueueCriteria> criteria = CriteriaWire.decode(req.msgs("criteria"));
        List<String> polled = criteria.isEmpty() ? req.strs("queues")
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
                    + req.str("workerId") + ", retry after " + retryAfter + "ms");
            return Msg.create().put("retryAfterMillis", retryAfter);
        }

        com.wiggle.server.store.ClaimFilter filter = criteria.isEmpty()
                ? com.wiggle.server.store.ClaimFilter.byName(polled, versionFilter(req))
                : new com.wiggle.server.store.ClaimFilter(criteria, versionFilter(req));
        int max = req.integer("max") > 0 ? req.integer("max") : 1;
        long lease = req.lng("leaseMillis");
        long deadline = System.currentTimeMillis() + Math.min(maxLongPollMillis, req.lng("waitMillis"));
        // An interrupted call means the server is stopping; don't claim work nobody will receive.
        List<com.wiggle.core.TaskActivation> tasks =
                engine.poll(req.str("workerId"), filter, max, lease, deadline, Thread.currentThread()::isInterrupted);
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollTasks worker=" + req.str("workerId")
                + " returning " + tasks.size() + " task(s)");
        Msg out = Msg.create();
        for (com.wiggle.core.TaskActivation t : tasks) out.add("tasks", taskMsg(t));
        return out;
    }

    Msg failTask(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc FailTask taskId=" + req.str("taskId")
                + " leaseOwner=" + req.str("leaseOwner") + " retryable=" + req.bool("retryable")
                + " message=" + req.str("message"));
        authz.requireAny(Permissions.TASK_POLL);
        engine.fail(req.str("taskId"), req.str("leaseOwner"), req.str("message"), req.bool("retryable"), servedQueues());
        return Msg.create().put("ok", true);
    }

    Msg heartbeatTask(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc HeartbeatTask taskId=" + req.str("taskId")
                + " leaseOwner=" + req.str("leaseOwner") + " extendMillis=" + req.lng("extendMillis"));
        authz.requireAny(Permissions.TASK_POLL);
        long until = engine.extendLease(req.str("taskId"), req.str("leaseOwner"), req.lng("extendMillis"), servedQueues());
        return Msg.create().put("leaseExpiresAt", until);
    }

    Msg reportSteps(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc ReportSteps runs=" + req.msgs("runs").size());
        authz.requireAny(Permissions.TASK_POLL);
        List<WorkflowEngine.Run> runs = new ArrayList<>(req.msgs("runs").size());
        for (Msg r : req.msgs("runs")) {
            List<WorkflowEngine.StepInput> steps = stepInputs(r);
            for (WorkflowEngine.StepInput step : steps) {
                for (com.wiggle.core.CreatedBranch b : step.branches()) requireSubFlowStarts(b.steps());
            }
            runs.add(new WorkflowEngine.Run(r.str("taskId"), r.str("leaseOwner"), steps, r.bool("final"),
                    CriteriaWire.decode(r.msgs("criteria"))));
        }
        Map<String, WorkflowEngine.RunResult> results = engine.report(runs, servedQueues());
        Msg out = Msg.create();
        for (WorkflowEngine.Run submitted : runs) {
            WorkflowEngine.RunResult r = results.get(submitted.startTaskId());
            Msg one = Msg.create().put("taskId", submitted.startTaskId());
            if (r != null && r.ok()) {
                one.put("applied", applied(r.outcome()));
            } else if (r != null) {
                one.put("errorStatus", r.errorStatus()).put("error", r.error() == null ? "" : r.error());
            } else {
                one.put("errorStatus", 500).put("error", "no answer for run " + submitted.startTaskId());
            }
            out.add("results", one);
        }
        return out;
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

    private static Msg applied(ReportOutcome out) {
        return Msg.create()
                .put("instanceStatus", out.instanceStatus())
                .put("leaseExpiresAt", out.leaseExpiresAt())
                .put("nextTaskId", out.nextTaskId() == null ? "" : out.nextTaskId());
    }

    private static List<WorkflowEngine.StepInput> stepInputs(Msg req) {
        return stepInputs(req.msgs("steps"));
    }

    /** A step's outcome is its {@code merge} (a task's next context) or its {@code predicateValue}, never both. */
    private static List<WorkflowEngine.StepInput> stepInputs(List<Msg> reported) {
        List<WorkflowEngine.StepInput> steps = new ArrayList<>(reported.size());
        for (Msg s : reported) {
            if (s.has("merge") && s.has("predicateValue")) {
                throw new IllegalArgumentException("step " + s.str("nodeId") + " reports both a merge and a predicate value");
            }
            Object merge = s.value("merge");
            Boolean predicate = s.optBool("predicateValue");
            steps.add(new WorkflowEngine.StepInput(s.str("nodeId"), merge, predicate,
                    s.lng("startedAt") == 0 ? null : s.lng("startedAt"),
                    s.lng("finishedAt") == 0 ? null : s.lng("finishedAt"),
                    emitted(s.msgs("events")), branches(s.msgs("branches"))));
        }
        return steps;
    }

    /** Created branches as reported. A step kind this build does not know arrives as null, and the
     *  engine refuses the branch by failing the step, not the whole report. */
    private static List<com.wiggle.core.CreatedBranch> branches(List<Msg> reported) {
        List<com.wiggle.core.CreatedBranch> out = new ArrayList<>(reported.size());
        for (Msg b : reported) {
            out.add(new com.wiggle.core.CreatedBranch(b.has("input") ? b.value("input") : null,
                    b.str("key").isEmpty() ? null : b.str("key"), branchSteps(b.msgs("steps"))));
        }
        return out;
    }

    private static List<com.wiggle.core.CreatedBranch.BranchStep> branchSteps(List<Msg> reported) {
        List<com.wiggle.core.CreatedBranch.BranchStep> steps = new ArrayList<>(reported.size());
        for (Msg st : reported) {
            List<List<com.wiggle.core.CreatedBranch.BranchStep>> arms = new ArrayList<>(st.msgs("arms").size());
            for (Msg arm : st.msgs("arms")) arms.add(branchSteps(arm.msgs("steps")));
            List<com.wiggle.core.CreatedBranch.BranchStep.Case> cases = new ArrayList<>(st.msgs("cases").size());
            for (Msg c : st.msgs("cases")) {
                cases.add(new com.wiggle.core.CreatedBranch.BranchStep.Case(
                        c.str("guard").isEmpty() ? null : c.str("guard"), branchSteps(c.msgs("steps"))));
            }
            steps.add(new com.wiggle.core.CreatedBranch.BranchStep(
                    st.str("name").isEmpty() ? null : st.str("name"),
                    nodeKind(st.str("kind")),
                    st.bool("compensable"),
                    st.str("queue").isEmpty() ? null : st.str("queue"),
                    st.has("retry") ? com.wiggle.core.RetryPolicy.fromJson(st.value("retry")) : null,
                    st.lng("sleepMillis"),
                    st.str("combine").isEmpty() ? null : st.str("combine"),
                    st.str("workflow").isEmpty() ? null : st.str("workflow"),
                    branchSteps(st.msgs("escalation")),
                    arms,
                    branchSteps(st.msgs("body")),
                    st.integer("loopBudget"),
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
    private static List<com.wiggle.core.EmittedEvent> emitted(List<Msg> reported) {
        List<com.wiggle.core.EmittedEvent> out = new ArrayList<>(reported.size());
        for (Msg e : reported) {
            out.add(new com.wiggle.core.EmittedEvent(e.str("type"),
                    e.has("payload") ? e.value("payload") : null));
        }
        return out;
    }

    Msg getStepStats(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc GetStepStats workflow=" + req.str("workflow")
                + " version=" + req.integer("version") + " since=" + req.lng("since"));
        authz.require(Permissions.READ, req.str("workflow"));
        int sample = req.integer("sample") > 0 ? req.integer("sample") : 10_000;
        Msg out = Msg.create().put("workflow", req.str("workflow")).put("version", req.integer("version"));
        for (com.wiggle.core.NodeStats n : engine.stepStats(req.str("workflow"), req.integer("version"), req.lng("since"), sample)) {
            out.add("nodes", Msg.create()
                    .put("nodeId", n.nodeId()).put("name", n.name() == null ? "" : n.name())
                    .put("count", n.count()).put("meanMillis", n.meanMillis())
                    .put("p50Millis", n.p50Millis()).put("p95Millis", n.p95Millis()).put("maxMillis", n.maxMillis())
                    .put("waitP50Millis", n.waitP50Millis()).put("waitP95Millis", n.waitP95Millis()));
        }
        return out;
    }

    Msg searchInstances(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc SearchInstances text=" + req.str("text"));
        authz.requireAny(Permissions.READ);
        if (search == null) {
            throw EngineException.conflict("search is not enabled: give a shard the search role, "
                    + "or set WIGGLE_SEARCH_ENABLED=true on a single database");
        }
        String workflow = req.has("workflow") ? req.str("workflow") : null;
        String status = req.has("status") ? req.str("status") : null;
        Long from = req.lng("updatedFrom") > 0 ? req.lng("updatedFrom") : null;
        Long to = req.lng("updatedTo") > 0 ? req.lng("updatedTo") : null;
        int limit = req.integer("limit") > 0 ? req.integer("limit") : 20;
        Msg out = Msg.create();
        com.wiggle.server.search.Search.Result r = search.search(req.str("text"), workflow, status, from, to,
                limit, req.bool("partialOk"), authz.scope(Permissions.READ), attributeFilters(req));
        out.put("partial", r.partial());
        for (com.wiggle.server.search.Search.Hit h : r.hits()) {
            out.add("hits", Msg.create().put("instanceId", h.instanceId()).put("workflow", h.workflow())
                    .put("version", h.version()).put("status", h.status())
                    .put("correlationId", h.correlationId() == null ? "" : h.correlationId())
                    .put("createdAt", h.createdAt()).put("updatedAt", h.updatedAt()).put("score", h.score())
                    .put("purged", h.purged()));
        }
        return out;
    }

    private static List<com.wiggle.server.store.Rows.AttrFilter> attributeFilters(Msg req) {
        List<com.wiggle.server.store.Rows.AttrFilter> out = new ArrayList<>();
        try {
            for (Msg f : req.msgs("attributes")) {
                Msg v = f.msg("value");
                Object value = switch (CriteriaWire.oneOf(v, "attribute '" + f.str("name") + "'",
                        "text", "integer", "decimal", "flag")) {
                    case "text" -> v.str("text");
                    case "integer" -> v.lng("integer");
                    case "decimal" -> new java.math.BigDecimal(v.str("decimal"));
                    default -> v.bool("flag");
                };
                out.add(new com.wiggle.server.store.Rows.AttrFilter(f.str("name"),
                        com.wiggle.core.criteria.Op.valueOf(f.str("op")), value));
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

    Msg pollEvents(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc PollEvents consumer=" + req.str("consumer")
                + " max=" + req.integer("max") + " waitMillis=" + req.lng("waitMillis"));
        authz.require(Permissions.EVENT_READ, null);
        if (memory.rejectPoll()) {
            long retryAfter = memory.retryAfterMillis();
            LOG.log(System.Logger.Level.WARNING, () -> "memory pressure: rejecting event poll from "
                    + req.str("consumer") + ", retry after " + retryAfter + "ms");
            return Msg.create().put("retryAfterMillis", retryAfter);
        }
        long deadline = System.currentTimeMillis() + Math.min(maxLongPollMillis, req.lng("waitMillis"));
        List<com.wiggle.core.EventView> events =
                engine.pollEvents(req.str("consumer"), req.integer("max"), req.lng("startFrom"), deadline, Thread.currentThread()::isInterrupted);
        Msg out = Msg.create();
        for (com.wiggle.core.EventView e : events) out.add("events", eventMsg(e));
        return out;
    }

    Msg ackEvents(Msg req) throws Exception {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc AckEvents consumer=" + req.str("consumer")
                + " ackedSeq=" + req.lng("ackedSeq") + " ackedCursor=" + req.str("ackedCursor"));
        authz.require(Permissions.EVENT_READ, null);
        if (req.str("ackedCursor").isEmpty()) engine.ackEvents(req.str("consumer"), req.lng("ackedSeq"));
        else engine.ackEvents(req.str("consumer"), req.str("ackedCursor"));
        return Msg.create().put("ok", true);
    }

    private static Msg eventMsg(com.wiggle.core.EventView e) {
        Msg b = Msg.create()
                .put("seq", e.seq()).put("instanceId", e.instanceId()).put("workflow", e.workflow())
                .put("version", e.version()).put("type", e.type()).put("createdAt", e.createdAt()).put("shard", e.shard());
        if (e.cursor() != null) b.put("cursor", e.cursor());
        if (e.correlationId() != null) b.put("correlationId", e.correlationId());
        if (e.nodeId() != null) b.put("nodeId", e.nodeId());
        if (e.payload() != null && !e.payload().isEmpty()) b.put("payload", e.payload());
        return b;
    }

    private Msg clusterView() {
        Msg out = Msg.create().put("self", cluster.nodeId()).put("leader", cluster.isLeader());
        long now = System.currentTimeMillis();
        for (ServerNode n : cluster.members()) {
            out.add("members", Msg.create()
                    .put("id", n.id)
                    .put("name", n.name)
                    .put("firstHeartbeat", n.firstHeartbeat)
                    .put("lastHeartbeat", n.lastHeartbeat)
                    .put("workers", n.workers)
                    .put("leader", n.leader)
                    .put("alive", now - n.lastHeartbeat < cluster.deadAfterMillis()));
        }
        return out;
    }

    private static Msg viewMsg(com.wiggle.core.InstanceView v) {
        Msg m = Msg.create()
                .put("id", v.id())
                .put("workflow", v.workflow())
                .put("version", v.version())
                .put("status", v.status())
                .put("createdAt", v.createdAt())
                .put("updatedAt", v.updatedAt());
        if (v.terminationReason() != null) m.put("terminationReason", v.terminationReason());
        if (v.error() != null) m.put("error", v.error());
        if (v.context() != null) m.put("context", v.context());
        return m;
    }

    private static Msg tokenMsg(com.wiggle.server.store.Rows.Token t) {
        Msg m = Msg.create()
                .put("id", t.id)
                .put("nodeId", t.nodeId)
                .put("kind", t.kind.name())
                .put("status", t.status.name())
                .put("activity", t.activity == null ? "" : t.activity)
                .put("attempt", t.attempt)
                .put("availableAt", t.availableAt);
        if (t.leaseOwner != null) m.put("leaseOwner", t.leaseOwner);
        if (t.lastError != null) m.put("lastError", t.lastError);
        if (t.startedAt != null) m.put("startedAt", t.startedAt);
        if (t.finishedAt != null) m.put("finishedAt", t.finishedAt);
        if (t.stepInput != null) m.put("input", com.wiggle.core.Json.parse(t.stepInput));
        if (t.stepOutput != null) m.put("output", com.wiggle.core.Json.parse(t.stepOutput));
        m.put("createdAt", t.createdAt);
        return m;
    }

    private static Msg taskMsg(com.wiggle.core.TaskActivation t) {
        Msg m = Msg.create()
                .put("taskId", t.taskId())
                .put("instanceId", t.instanceId())
                .put("workflow", t.workflow())
                .put("version", t.version())
                .put("nodeId", t.nodeId())
                .put("stepName", t.stepName() == null ? "" : t.stepName())
                .put("activity", t.activity())
                .put("kind", t.kind().name())
                .put("attempt", t.attempt())
                .put("leaseExpiresAt", t.leaseExpiresAt())
                .put("leaseOwner", t.leaseOwner())
                .put("executionMode", t.executionMode().name());
        if (t.context() != null) m.put("context", t.context());
        if (t.baseContext() != null) {   // a forEach item step: deliver the frozen base alongside
            m.put("baseContext", t.baseContext());
            m.put("itemIndex", t.itemIndex());
            if (t.itemMapKey() != null) m.put("itemMapKey", t.itemMapKey());
        }
        if (t.collectKey() != null) m.put("collectKey", t.collectKey());
        m.put("armNames", t.armNames());
        if (t.correlationId() != null) m.put("correlationId", t.correlationId());
        m.put("instanceCreatedAt", t.instanceCreatedAt());
        return m;
    }
}
