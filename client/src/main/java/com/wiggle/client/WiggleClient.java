package com.wiggle.client;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.PollResult;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.NodeKind;
import com.wiggle.core.Tls;
import com.wiggle.core.wire.ControlPlane;
import com.wiggle.core.wire.Msg;
import io.github.shield.internal.RetriesExhaustedException;

import java.util.*;

public final class WiggleClient implements AutoCloseable {

    private final HttpTransport transport;

    /**
     * Connects with TLS if {@code WIGGLE_TLS_*} is configured, otherwise plaintext, presenting the
     * API key in {@code WIGGLE_API_KEY} when one is set.
     */
    public WiggleClient(String target) {
        this(target, Tls.Options.fromEnvironment());
    }

    /**
     * Connects to {@code target}, using TLS when {@code tls} carries a keystore and/or truststore:
     * the truststore verifies the server, and the keystore presents a client certificate for mTLS.
     * With neither, the connection is plaintext. Presents the API key in {@code WIGGLE_API_KEY} when set.
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
        this(target, tls, requireTls, apiKeyFromEnvironment());
    }

    /**
     * As {@link #WiggleClient(String, Tls.Options, boolean)}, presenting {@code apiKey} on every
     * call as {@code Authorization: Bearer <key>}; null presents none. A key travels in the clear on a
     * plaintext connection, so use one with TLS.
     */
    public WiggleClient(String target, Tls.Options tls, boolean requireTls, String apiKey) {
        this.transport = new HttpTransport(target, tls, requireTls, apiKey);
    }

    /** {@code -Dwiggle.api.key}, else {@code WIGGLE_API_KEY}; null when neither is set. */
    static String apiKeyFromEnvironment() {
        String v = System.getProperty("wiggle.api.key");
        if (v == null) v = System.getenv("WIGGLE_API_KEY");
        return v == null || v.isBlank() ? null : v;
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
        call(() -> transport.call(ControlPlane.REGISTER_WORKFLOW, Msg.create()
                .put("definition", flowSpec.definition().toJson())
                .put("force", force)));
    }

    /**
     * The dispatchable backlog split by (workflow, version, queue), each flagged with whether a worker
     * polling that node would claim it. An uncovered slice is work nothing can pick up -- a queue
     * nobody polls, or a version every worker has scoped itself out of.
     */
    public List<BacklogSlice> backlogCoverage(int max) {
        return Wire.backlogSlices(call(() -> transport.call(ControlPlane.GET_BACKLOG_COVERAGE,
                Msg.create().put("max", max))));
    }

    /**
     * Full-text search over instances: every word of {@code text} must occur in an instance's
     * correlation id, context, termination reason or error. {@code workflow}, {@code status} and the
     * {@code updatedFrom}/{@code updatedTo} bounds (epoch millis) filter, null for none. With
     * {@code partialOk}, a search shard that does not answer leaves the result partial instead of
     * failing it.
     */
    public SearchResult search(String text, String workflow, String status, Long updatedFrom, Long updatedTo,
                               int limit, boolean partialOk) {
        return search(text, workflow, status, updatedFrom, updatedTo, limit, partialOk, List.of());
    }

    /**
     * {@link #search(String, String, String, Long, Long, int, boolean)}, keeping the instances whose
     * search attributes satisfy every filter. A flow declares them with
     * {@code GraphBuilder.searchAttribute}, so the filters need a {@code workflow}; the server answers
     * 400 without one. An instance that does not hold an attribute, or holds it as another type, does
     * not match. Empty {@code text} matches on the filters alone.
     */
    public SearchResult search(String text, String workflow, String status, Long updatedFrom, Long updatedTo,
                               int limit, boolean partialOk, List<AttributeFilter> attributes) {
        Msg req = Msg.create()
                .put("text", text == null ? "" : text).put("limit", limit).put("partialOk", partialOk);
        for (AttributeFilter f : attributes) req.add("attributes", f.toWire());
        if (workflow != null) req.put("workflow", workflow);
        if (status != null) req.put("status", status);
        if (updatedFrom != null) req.put("updatedFrom", updatedFrom);
        if (updatedTo != null) req.put("updatedTo", updatedTo);
        Msg r = call(() -> transport.call(ControlPlane.SEARCH_INSTANCES, req));
        List<SearchHit> hits = new ArrayList<>();
        for (Msg h : r.msgs("hits")) {
            hits.add(new SearchHit(h.str("instanceId"), h.str("workflow"), h.integer("version"), h.str("status"),
                    h.str("correlationId").isEmpty() ? null : h.str("correlationId"), h.lng("createdAt"), h.lng("updatedAt"),
                    h.dbl("score"), h.bool("purged")));
        }
        return new SearchResult(hits, r.bool("partial"));
    }

    /**
     * A condition on a search attribute: {@code value} is a String, Long (or Integer), BigDecimal (or
     * Double) or Boolean. Text and boolean attributes are compared for equality only.
     */
    public record AttributeFilter(String name, com.wiggle.core.criteria.Op op, Object value) {
        public static AttributeFilter eq(String name, Object value) { return new AttributeFilter(name, com.wiggle.core.criteria.Op.EQUAL, value); }
        public static AttributeFilter lt(String name, Number value) { return new AttributeFilter(name, com.wiggle.core.criteria.Op.LESS_THAN, value); }
        public static AttributeFilter le(String name, Number value) { return new AttributeFilter(name, com.wiggle.core.criteria.Op.LESS_THAN_OR_EQUAL, value); }
        public static AttributeFilter gt(String name, Number value) { return new AttributeFilter(name, com.wiggle.core.criteria.Op.GREATER_THAN, value); }
        public static AttributeFilter ge(String name, Number value) { return new AttributeFilter(name, com.wiggle.core.criteria.Op.GREATER_THAN_OR_EQUAL, value); }

        Msg toWire() {
            Msg v = Msg.create();
            switch (value) {
                case String s -> v.put("text", s);
                case Boolean b -> v.put("flag", b);
                case Long l -> v.put("integer", l);
                case Integer i -> v.put("integer", i);
                case Number n -> v.put("decimal", new java.math.BigDecimal(n.toString()).toPlainString());
                case null, default -> throw new IllegalArgumentException("attribute '" + name
                        + "' compares with a String, Number or Boolean, got " + value);
            }
            return Msg.create().put("name", name).put("op", op.name()).put("value", v);
        }
    }

    /** One search hit; {@code purged} when the instance is gone and only its document remains. */
    public record SearchHit(String instanceId, String workflow, int version, String status, String correlationId,
                            long createdAt, long updatedAt, double score, boolean purged) {}

    /** Search hits, best first; {@code partial} when a search shard did not answer. */
    public record SearchResult(List<SearchHit> hits, boolean partial) {}

    /** One slice of the dispatchable backlog. See {@link #backlogCoverage(int)}. */
    /** {@code coverage} is COVERED, CONDITIONAL or UNCOVERED (empty from a server that predates it);
     *  {@code claimable} how many ready tasks a live worker would claim, null when not counted;
     *  {@code criteria} the worker criteria a conditional slice depends on. */
    public record BacklogSlice(String workflow, int version, String queue, int readyCount,
                               long oldestAvailableAt, boolean covered, int livePollers,
                               String coverage, Integer claimable, List<String> criteria) {

        public BacklogSlice(String workflow, int version, String queue, int readyCount,
                            long oldestAvailableAt, boolean covered, int livePollers) {
            this(workflow, version, queue, readyCount, oldestAvailableAt, covered, livePollers, "", null, List.of());
        }
    }

    /**
     * Per-node duration statistics for one workflow version over its newest {@code sample} timed
     * steps that finished after {@code since} (epoch millis; 0 = no bound). A null or zero version
     * means the latest. Slowest 95th percentile first, so the head of the list is the bottleneck.
     */
    public java.util.List<com.wiggle.core.NodeStats> stepStats(String workflow, Integer version, long since, int sample) {
        Msg req = Msg.create()
                .put("workflow", workflow).put("since", since).put("sample", sample);
        if (version != null) req.put("version", version);
        return Wire.nodeStats(call(() -> transport.call(ControlPlane.GET_STEP_STATS, req)));
    }

    /**
     * The next entries of the event log for {@code consumer}, oldest first, long-polling up to
     * {@code waitMillis} for one to appear. A consumer is a named cursor: its first poll registers
     * it at the log's tail ({@code startFrom} 0), at the earliest entry still retained ({@code -1}),
     * or, on a log that is not sharded, after the seq it names; later polls ignore {@code startFrom}.
     *
     * <p>Delivery is at-least-once and the cursor only moves on an ack, so a consumer that dies
     * mid-batch is served the same entries again. Acknowledge what you have handled with
     * {@link #ackEvents(String, String)} and the last entry's {@link com.wiggle.core.EventView#cursor()}.
     */
    public java.util.List<com.wiggle.core.EventView> pollEvents(String consumer, int max, long waitMillis, long startFrom) {
        return Wire.events(call(() -> transport.call(ControlPlane.POLL_EVENTS, Msg.create()
                .put("consumer", consumer).put("max", max).put("waitMillis", waitMillis).put("startFrom", startFrom))));
    }

    /**
     * Acknowledges, for {@code consumer}, the event whose {@link com.wiggle.core.EventView#cursor()} this
     * is and every event served before it; cumulative, never backwards.
     */
    public void ackEvents(String consumer, String cursor) {
        call(() -> transport.call(ControlPlane.ACK_EVENTS, Msg.create()
                .put("consumer", consumer).put("ackedCursor", cursor)));
    }

    /** Acknowledges every event up to {@code ackedSeq} for {@code consumer}; cumulative, never backwards.
     *  Only for a log that is not sharded: a sharded one refuses it, and takes {@link #ackEvents(String, String)}. */
    public void ackEvents(String consumer, long ackedSeq) {
        call(() -> transport.call(ControlPlane.ACK_EVENTS, Msg.create()
                .put("consumer", consumer).put("ackedSeq", ackedSeq)));
    }

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
        Msg req = Msg.create().put("name", name);
        if (version != null) req.put("version", version);
        Msg def = call(() -> transport.call(ControlPlane.GET_WORKFLOW, req));
        return com.wiggle.core.WorkflowDefinition.fromJson(def.obj("definition"));
    }

    public String start(String workflow, Object context) {
        return start(workflow, context, null, null);
    }

    public String start(FlowSpec flowSpec, Object context) {
        return start(flowSpec.name(), context, flowSpec.version(), null);
    }

    public String start(String workflow, Object context, Integer version, String correlationId) {
        Msg req = Msg.create().put("workflow", workflow);
        if (context != null) req.put("context", com.wiggle.core.RecordMapper.toJson(context));
        if (version != null) req.put("version", version);
        if (correlationId != null) req.put("correlationId", correlationId);
        return call(() -> transport.call(ControlPlane.START_INSTANCE, req)).str("instanceId");
    }

    public com.wiggle.core.InstanceView instance(String instanceId) {
        Msg detail = call(() -> transport.call(ControlPlane.GET_INSTANCE,
                Msg.create().put("instanceId", instanceId)));
        return Wire.instanceView(detail.msg("instance"));
    }

    /** An instance plus its tokens (the {@code GetInstance} detail; {@link #instance} drops the tokens). */
    public InstanceWithTokens instanceDetail(String instanceId) {
        Msg detail = call(() -> transport.call(ControlPlane.GET_INSTANCE,
                Msg.create().put("instanceId", instanceId)));
        java.util.List<TokenInfo> tokens = new java.util.ArrayList<>();
        for (Msg t : detail.msgs("tokens")) tokens.add(Wire.tokenInfo(t));
        return new InstanceWithTokens(Wire.instanceView(detail.msg("instance")), tokens);
    }

    /** The names of all registered workflows. */
    public java.util.List<String> workflowNames() {
        return call(() -> transport.call(ControlPlane.LIST_WORKFLOWS, Msg.create())).strs("workflows");
    }

    /** Lists instances filtered by {@code workflow} and/or {@code status} (either may be null), newest first. */
    public java.util.List<com.wiggle.core.InstanceView> listInstances(String workflow, String status, int limit) {
        Msg req = Msg.create().put("limit", limit);
        if (workflow != null) req.put("workflow", workflow);
        if (status != null) req.put("status", status);
        Msg list = call(() -> transport.call(ControlPlane.LIST_INSTANCES, req));
        java.util.List<com.wiggle.core.InstanceView> out = new java.util.ArrayList<>();
        for (Msg v : list.msgs("instances")) out.add(Wire.instanceView(v));
        return out;
    }

    /** An instance view plus the tokens driving it. */
    public record InstanceWithTokens(com.wiggle.core.InstanceView instance, java.util.List<TokenInfo> tokens) {}

    /**
     * A token as carried on the wire (thinner than the server row: no queue/leaseExpires/updatedAt).
     * {@code startedAt}/{@code finishedAt} are null for an untimed step; {@code input}/{@code output}
     * are the step's recorded JSON, null where none was recorded.
     */
    public record TokenInfo(String id, String nodeId, String kind, String status, String activity,
                            int attempt, long availableAt, String leaseOwner, String lastError,
                            Long startedAt, Long finishedAt, long createdAt, Object input, Object output) {}

    /** Instances started with {@code correlationId} (a business key), newest first (default limit 50). */
    public java.util.List<com.wiggle.core.InstanceView> findByCorrelation(String correlationId) {
        return findByCorrelation(correlationId, 50);
    }

    public java.util.List<com.wiggle.core.InstanceView> findByCorrelation(String correlationId, int limit) {
        Msg list = call(() -> transport.call(ControlPlane.LIST_INSTANCES, Msg.create()
                .put("correlationId", correlationId).put("limit", limit)));
        java.util.List<com.wiggle.core.InstanceView> out = new java.util.ArrayList<>();
        for (Msg v : list.msgs("instances")) out.add(Wire.instanceView(v));
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
        call(() -> transport.call(ControlPlane.CANCEL_INSTANCE, Msg.create()
                .put("instanceId", instanceId)
                .put("reason", reason == null ? "" : reason)));
    }

    public Map<String, Object> cluster() {
        return Wire.clusterMap(call(() -> transport.call(ControlPlane.GET_CLUSTER, Msg.create())));
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
        Msg b = Msg.create()
                .put("workerId", workerId)
                .put("queues", queues)
                .put("max", max)
                .put("leaseMillis", leaseMillis)
                .put("waitMillis", waitMillis);
        for (com.wiggle.core.WorkflowVersion v : versions) {
            b.add("versions", Msg.create()
                    .put("workflow", v.workflow()).put("version", v.version()));
        }
        return poll(b);
    }

    /**
     * Polls with criteria instead of queue names: claims only tasks on the criteria's queues that the
     * entry for their queue matches. {@code versions} scopes the claim as in the queue-name form.
     */
    public PollResult poll(String workerId, List<com.wiggle.core.criteria.QueueCriteria> criteria,
                           Collection<com.wiggle.core.WorkflowVersion> versions, int max,
                           long leaseMillis, long waitMillis) {
        Msg b = Msg.create()
                .put("workerId", workerId)
                .put("max", max)
                .put("leaseMillis", leaseMillis)
                .put("waitMillis", waitMillis);
        for (com.wiggle.core.criteria.QueueCriteria c : com.wiggle.core.criteria.QueueCriteria.forPoll(criteria)) {
            b.add("criteria", Wire.criteria(c));
        }
        for (com.wiggle.core.WorkflowVersion v : versions) {
            b.add("versions", Msg.create()
                    .put("workflow", v.workflow()).put("version", v.version()));
        }
        return poll(b);
    }

    private PollResult poll(Msg req) {
        Msg res = call(() -> transport.call(ControlPlane.POLL_TASKS, req));
        List<com.wiggle.core.TaskActivation> out = new ArrayList<>(res.msgs("tasks").size());
        for (Msg t : res.msgs("tasks")) out.add(Wire.taskActivation(t));
        return new PollResult(out, res.lng("retryAfterMillis"));
    }

    public void fail(String taskId, String leaseOwner, String message, boolean retryable) {
        call(() -> transport.call(ControlPlane.FAIL_TASK, Msg.create()
                .put("taskId", taskId)
                .put("leaseOwner", leaseOwner)
                .put("message", message)
                .put("retryable", retryable)));
    }

    /** Delivers a named signal to a running instance; {@code payload} merges into its context. */
    public void signal(String instanceId, String signal, Object payload) {
        Msg req = Msg.create().put("instanceId", instanceId).put("signal", signal);
        if (payload != null) req.put("payload", com.wiggle.core.RecordMapper.toJson(payload));
        call(() -> transport.call(ControlPlane.SIGNAL_INSTANCE, req));
    }

    /** Creates a recurring start on a fixed interval; returns the schedule id. */
    public String createSchedule(String workflow, java.time.Duration every, Object context) {
        Msg req = Msg.create()
                .put("workflow", workflow).put("everyMillis", every.toMillis());
        if (context != null) req.put("context", com.wiggle.core.RecordMapper.toJson(context));
        return call(() -> transport.call(ControlPlane.CREATE_SCHEDULE, req)).str("id");
    }

    /** Creates a recurring start on a five-field cron expression (evaluated in UTC); returns the schedule id. */
    public String createCronSchedule(String workflow, String cron, Object context) {
        Msg req = Msg.create()
                .put("workflow", workflow).put("cron", cron);
        if (context != null) req.put("context", com.wiggle.core.RecordMapper.toJson(context));
        return call(() -> transport.call(ControlPlane.CREATE_SCHEDULE, req)).str("id");
    }

    /** All schedules on the server. {@code everyMillis} is 0 for cron schedules; {@code cron} is null for interval ones. */
    public List<ScheduleInfo> schedules() {
        return call(() -> transport.call(ControlPlane.LIST_SCHEDULES, Msg.create())).msgs("schedules").stream()
                .map(Wire::scheduleInfo)
                .toList();
    }

    public void deleteSchedule(String id) {
        call(() -> transport.call(ControlPlane.DELETE_SCHEDULE, Msg.create().put("id", id)));
    }

    public record ScheduleInfo(String id, String workflow, long everyMillis, String cron,
                               long nextFireAt, long createdAt) {}

    /**
     * Tells the server that {@code workerId} has stopped polling: its polls in progress return at once and
     * claim nothing more. Answers how many there were. Tried once: a server that cannot be reached delivers
     * nothing to the worker's polls either.
     */
    public int stopPolling(String workerId) {
        try {
            return (int) transport.call(ControlPlane.STOP_POLLING, Msg.create().put("workerId", workerId)).lng("stopped");
        } catch (HttpTransport.RpcException e) {
            throw new WiggleApiException(statusCode(e), e.getMessage(), e, e.unavailable);
        }
    }

    public void heartbeat(String taskId, String leaseOwner, long extendMillis) {
        call(() -> transport.call(ControlPlane.HEARTBEAT_TASK, Msg.create()
                .put("taskId", taskId)
                .put("leaseOwner", leaseOwner)
                .put("extendMillis", extendMillis)));
    }

    /**
     * Reports finished work: one step, or the ordered run of steps this worker chained locally.
     * The server decides from the workflow's mode whether the continuation comes back leased to
     * this worker ({@code nextTaskId}) or is released. {@code finalHandback} says this worker will
     * not take another step whatever the mode allows.
     *
     * <p>One run, so its refusal is this call's failure: it raises {@link WiggleApiException} with
     * the status the run was refused under, the same as any other single-instance operation.
     */
    public com.wiggle.core.ReportResult reportSteps(String taskId, String leaseOwner,
                                                    List<StepReport> steps, boolean finalHandback) {
        return reportSteps(taskId, leaseOwner, steps, finalHandback, List.of());
    }

    /**
     * The same, from a worker that polled with {@code criteria}: the server leases the continuation
     * back only when the entry for its queue matches it.
     */
    public com.wiggle.core.ReportResult reportSteps(String taskId, String leaseOwner, List<StepReport> steps,
                                                    boolean finalHandback,
                                                    List<com.wiggle.core.criteria.QueueCriteria> criteria) {
        RunSubmission run = new RunSubmission(taskId, leaseOwner, steps, finalHandback, criteria);
        RunOutcome only = reportSteps(List.of(run)).get(taskId);
        if (only == null) throw new WiggleApiException(500, "no answer for run " + taskId, null);
        if (!only.ok()) throw new WiggleApiException(only.errorStatus(), only.error(), null);
        return only.outcome();
    }

    /**
     * One reported step: exactly one of {@code merge} (task) or {@code predicateValue} (predicate),
     * plus what it emitted and the branches it created.
     */
    public record StepReport(String nodeId, Object merge, Boolean predicateValue, Long startedAt, Long finishedAt,
                             java.util.List<com.wiggle.core.EmittedEvent> events,
                             java.util.List<com.wiggle.core.CreatedBranch> branches) {

        public StepReport {
            events = events == null ? java.util.List.of() : java.util.List.copyOf(events);
            branches = branches == null ? java.util.List.of() : java.util.List.copyOf(branches);
        }

        public StepReport(String nodeId, Object merge, Boolean predicateValue, Long startedAt, Long finishedAt,
                          java.util.List<com.wiggle.core.EmittedEvent> events) {
            this(nodeId, merge, predicateValue, startedAt, finishedAt, events, java.util.List.of());
        }

        public StepReport(String nodeId, Object merge, Boolean predicateValue) {
            this(nodeId, merge, predicateValue, null, null, java.util.List.of());
        }

        public StepReport(String nodeId, Object merge, Boolean predicateValue, Long startedAt, Long finishedAt) {
            this(nodeId, merge, predicateValue, startedAt, finishedAt, java.util.List.of());
        }
    }

    /** One instance's worth of a report: exactly the arguments of the single-run {@link #reportSteps}. */
    public record RunSubmission(String taskId, String leaseOwner, List<StepReport> steps, boolean finalHandback,
                                List<com.wiggle.core.criteria.QueueCriteria> criteria) {

        public RunSubmission {
            criteria = criteria == null ? List.of() : List.copyOf(criteria);
        }

        public RunSubmission(String taskId, String leaseOwner, List<StepReport> steps, boolean finalHandback) {
            this(taskId, leaseOwner, steps, finalHandback, List.of());
        }
    }

    /** A run's fate: what it applied to, or the status and message it was refused under. A refused
     *  run wrote nothing and may be reported again in a call of its own. */
    public record RunOutcome(com.wiggle.core.ReportResult outcome, int errorStatus, String error) {
        public boolean ok() { return outcome != null; }
    }

    /**
     * The same report for several independent runs, applied in one server-side commit where
     * batching is sound. Answers are keyed by task id, one per submitted run, and a refused run
     * carries the status it was refused under rather than failing the call.
     */
    public Map<String, RunOutcome> reportSteps(List<RunSubmission> runs) {
        Msg req = Msg.create();
        for (RunSubmission run : runs) {
            Msg one = Msg.create()
                    .put("taskId", run.taskId()).put("leaseOwner", run.leaseOwner()).put("final", run.finalHandback());
            for (StepReport s : run.steps()) one.add("steps", Wire.stepResult(s));
            for (com.wiggle.core.criteria.QueueCriteria c : run.criteria()) one.add("criteria", Wire.criteria(c));
            req.add("runs", one);
        }
        Msg res = call(() -> transport.call(ControlPlane.REPORT_STEPS, req));
        Map<String, RunOutcome> out = new java.util.LinkedHashMap<>();
        for (Msg r : res.msgs("results")) {
            out.put(r.str("taskId"), r.integer("errorStatus") == 0 && r.has("applied")
                    ? new RunOutcome(Wire.reportResult(r.msg("applied")), 0, null)
                    : new RunOutcome(null, r.integer("errorStatus"), r.str("error")));
        }
        return out;
    }

    private interface Call<T> { T run(); }

    // Every client and worker operation routes through call(), so the shared UNAVAILABLE retry
    // ({@link RpcRetry}) covers them all — a call issued while the server is momentarily gone (restart /
    // failover) rides out the outage. Permanent errors map straight through; exhausted retries map to
    // WiggleApiException(status 0), as before.
    private <T> T call(Call<T> call) {
        try {
            return RpcRetry.retrying(call::run);
        } catch (RetriesExhaustedException ex) {
            HttpTransport.RpcException last = RpcRetry.lastFailure(ex);
            String desc = last != null ? last.getMessage() : "unavailable";
            throw new WiggleApiException(0, "server unavailable after " + RpcRetry.maxAttempts()
                    + " attempts: " + desc, last != null ? last : ex, true);
        } catch (HttpTransport.RpcException e) {
            throw new WiggleApiException(statusCode(e), e.getMessage(), e, e.unavailable);
        }
    }

    /** The status a failed call reports: its HTTP status, or 0 when the server did not answer. */
    private static int statusCode(HttpTransport.RpcException e) {
        return switch (e.status) {
            case 400, 401, 403, 404, 409 -> e.status;
            case 0, 502, 503, 504 -> 0;
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
        transport.close();
    }

    public static final class WiggleApiException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final int status;
        private final boolean unavailable;

        public WiggleApiException(int status, String message) { this(status, message, null); }

        public WiggleApiException(int status, String message, Throwable cause) { this(status, message, cause, false); }

        public WiggleApiException(int status, String message, Throwable cause, boolean unavailable) {
            super(message, cause);
            this.status = status;
            this.unavailable = unavailable;
        }

        public int status() { return status; }

        /** True when the server could not be reached or could not serve the call, so it did not run. */
        public boolean unavailable() { return unavailable; }

        public boolean isClientError() { return status >= 400 && status < 500; }
    }
}
