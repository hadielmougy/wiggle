package com.wiggle.console;

import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.NodeStats;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.criteria.AttrType;
import com.wiggle.core.criteria.Op;
import com.wiggle.server.auth.Scope;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.search.Search;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.TokenPayload;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** {@link DashboardData} over the engine and cluster of the server process the portal runs in. */
public final class EngineDashboardData implements DashboardData {

    private final WorkflowEngine engine;
    private final ClusterManager cluster;
    private final Search search;

    public EngineDashboardData(WorkflowEngine engine, ClusterManager cluster) {
        this(engine, cluster, null);
    }

    /** @param search full-text search, or null when it is not enabled */
    public EngineDashboardData(WorkflowEngine engine, ClusterManager cluster, Search search) {
        this.engine = engine;
        this.cluster = cluster;
        this.search = search;
    }

    @Override public boolean searchEnabled() {
        return search != null;
    }

    @Override public Optional<SearchView> search(String text, String workflow, String status, int limit,
                                                 boolean partialOk, Scope readable, List<AttributeFilter> attributes) {
        if (search == null) return Optional.empty();
        Search.Result r = search.search(text, workflow, status, null, null, limit, partialOk, readable,
                attrFilters(workflow, attributes));
        return Optional.of(new SearchView(r.hits().stream()
                .map(h -> new SearchHitView(h.instanceId(), h.workflow(), h.version(), h.status(), h.correlationId(),
                        h.updatedAt(), h.score(), h.purged()))
                .toList(), r.partial()));
    }

    private List<Rows.AttrFilter> attrFilters(String workflow, List<AttributeFilter> filters) {
        if (filters.isEmpty()) return List.of();
        if (workflow == null) throw EngineException.badRequest("attribute filters need a workflow");
        Map<String, AttrType> declared = new HashMap<>();
        engine.latestDefinition(workflow).ifPresent(d -> d.searchAttributes().forEach(a -> declared.put(a.name(), a.type())));
        List<Rows.AttrFilter> out = new ArrayList<>();
        for (AttributeFilter f : filters) {
            AttrType type = declared.get(f.name());
            if (type == null) throw EngineException.badRequest(workflow + " declares no search attribute '" + f.name() + "'");
            Op op = switch (f.op()) {
                case "eq" -> Op.EQUAL;
                case "lt" -> Op.LESS_THAN;
                case "le" -> Op.LESS_THAN_OR_EQUAL;
                case "gt" -> Op.GREATER_THAN;
                case "ge" -> Op.GREATER_THAN_OR_EQUAL;
                default -> throw EngineException.badRequest("unknown attribute operator '" + f.op() + "'");
            };
            try {
                Object value = switch (type) {
                    case STRING -> f.value();
                    case LONG -> Long.parseLong(f.value().trim());
                    case DECIMAL -> new BigDecimal(f.value().trim());
                    case BOOLEAN -> parseFlag(f.value());
                };
                out.add(new Rows.AttrFilter(f.name(), op, value));
            } catch (IllegalArgumentException e) {
                throw EngineException.badRequest("attribute '" + f.name() + "': " + e.getMessage());
            }
        }
        return out;
    }

    private static boolean parseFlag(String s) {
        return switch (s.trim()) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("expected true or false, got '" + s + "'");
        };
    }

    @Override public List<String> workflowNames() {
        return engine.workflowNames();
    }

    @Override public Optional<Object> workflowGraph(String name) {
        return engine.latestDefinition(name).map(WorkflowDefinition::toJson);
    }

    @Override public List<InstanceView> listInstances(String workflow, String status, int limit) {
        return engine.list(workflow, status, limit);
    }

    @Override public List<InstanceView> findByCorrelation(String correlationId, int limit) {
        return engine.findByCorrelation(correlationId, limit);
    }

    @Override public Optional<InstanceDetail> instance(String id) {
        return engine.instance(id).map(v -> new InstanceDetail(v,
                engine.tokens(id).stream().map(EngineDashboardData::token).toList()));
    }

    @Override public void cancel(String id, String reason) {
        engine.cancel(id, reason);
    }

    @Override public void signal(String id, String name, Object payload) {
        engine.signal(id, name, payload);
    }

    @Override public List<SignalView> pendingSignals(int limit) {
        return engine.pendingSignals(limit).stream()
                .map(t -> new SignalView(t.instanceId, t.workflow, t.activity, t.availableAt, t.createdAt))
                .toList();
    }

    @Override public List<BacklogView> backlogCoverage(int limit) {
        int pollers = engine.livePollers().size();
        List<BacklogView> out = new ArrayList<>();
        for (Rows.BacklogSlice s : engine.backlog(limit)) {
            WorkflowEngine.SliceCoverage c = engine.coverage(s);
            out.add(new BacklogView(s.workflow(), s.version(), s.queue(), s.readyCount(), s.oldestAvailableAt(),
                    c.state().name(), c.claimable(), c.criteria(), pollers));
        }
        // Uncovered first, then conditional, then the rest; deepest first within each.
        out.sort(Comparator.comparingInt((BacklogView v) -> switch (v.coverage()) {
                    case "UNCOVERED" -> 0;
                    case "CONDITIONAL" -> 1;
                    default -> 2;
                }).thenComparing(Comparator.comparingInt(BacklogView::readyCount).reversed()));
        return out;
    }

    @Override public List<NodeStats> stepStats(String workflow, Integer version, long since, int sample) {
        List<NodeStats> out = new ArrayList<>(engine.stepStats(workflow, version, since, sample));
        out.sort(Comparator.comparingLong(NodeStats::p95Millis).reversed());
        return out;
    }

    @Override public List<ScheduleView> schedules() {
        return engine.schedules().stream()
                .map(s -> new ScheduleView(s.id, s.workflow, s.intervalMillis, s.cron, s.nextFireAt, s.createdAt))
                .toList();
    }

    @Override public String createSchedule(String workflow, Duration every, Object context) {
        return engine.createSchedule(workflow, every, context);
    }

    @Override public String createCronSchedule(String workflow, String cron, Object context) {
        return engine.createCronSchedule(workflow, cron, context);
    }

    @Override public void deleteSchedule(String id) {
        engine.deleteSchedule(id);
    }

    @Override public ClusterView cluster() {
        long now = System.currentTimeMillis();
        long deadAfter = cluster.deadAfterMillis();
        List<MemberView> members = cluster.members().stream()
                .map(n -> new MemberView(n.id, n.name, n.workers, n.leader, now - n.lastHeartbeat < deadAfter,
                        n.lastHeartbeat))
                .toList();
        return new ClusterView(cluster.nodeId(), cluster.isLeader(), members);
    }

    private static TokenView token(Rows.Token t) {
        String group = t.currentJoinGroup();
        String scope = group == null ? null : group.contains("#") ? group.substring(0, group.lastIndexOf('#')) : group;
        TokenPayload.Frame frame = scope == null || t.payload == null ? null : t.payload.top();
        return new TokenView(t.id, t.nodeId, t.kind == null ? null : t.kind.name(),
                t.status == null ? null : t.status.name(), t.activity, t.queue, t.attempt, t.availableAt,
                t.leaseOwner, t.leaseExpiresAt, t.lastError, t.updatedAt, t.startedAt, t.finishedAt, t.createdAt,
                t.stepInput == null ? null : Json.parse(t.stepInput),
                t.stepOutput == null ? null : Json.parse(t.stepOutput),
                scope,
                frame == null ? null : frame.kind() == TokenPayload.FrameKind.ARM ? "arm" : "item",
                frame == null ? null : frame.idx(),
                frame == null ? null : frame.mapKey());
    }
}
