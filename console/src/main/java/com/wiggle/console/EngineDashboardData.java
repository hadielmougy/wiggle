package com.wiggle.console;

import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.NodeStats;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.auth.Scope;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.search.Search;
import com.wiggle.server.store.Rows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

    @Override public boolean semanticEnabled() {
        return search != null && search.semanticEnabled();
    }

    @Override public Optional<SearchView> search(String text, String workflow, String status, int limit,
                                                 boolean partialOk, boolean semantic, Scope readable) {
        if (search == null) return Optional.empty();
        List<Search.Hit> hits;
        boolean partial;
        if (semantic) {
            Search.SemanticResult r = search.semantic(text, workflow, status, null, null, limit, partialOk, readable);
            hits = r.hits();
            partial = r.partial();
        } else {
            Search.Result r = search.search(text, workflow, status, null, null, limit, partialOk, readable);
            hits = r.hits();
            partial = r.partial();
        }
        return Optional.of(new SearchView(hits.stream()
                .map(h -> new SearchHitView(h.instanceId(), h.workflow(), h.version(), h.status(), h.correlationId(),
                        h.updatedAt(), h.score(), h.purged()))
                .toList(), partial));
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
            out.add(new BacklogView(s.workflow(), s.version(), s.queue(), s.readyCount(), s.oldestAvailableAt(),
                    engine.covered(s.workflow(), s.version(), s.queue()), pollers));
        }
        out.sort((a, b) -> {
            if (a.covered() != b.covered()) return a.covered() ? 1 : -1;
            return Integer.compare(b.readyCount(), a.readyCount());
        });
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
        return new TokenView(t.id, t.nodeId, t.kind == null ? null : t.kind.name(),
                t.status == null ? null : t.status.name(), t.activity, t.queue, t.attempt, t.availableAt,
                t.leaseOwner, t.leaseExpiresAt, t.lastError, t.updatedAt, t.startedAt, t.finishedAt, t.createdAt,
                t.stepInput == null ? null : Json.parse(t.stepInput),
                t.stepOutput == null ? null : Json.parse(t.stepOutput));
    }
}
