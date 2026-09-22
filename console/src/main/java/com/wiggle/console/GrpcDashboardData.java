package com.wiggle.console;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.core.AnomalyView;
import com.wiggle.core.InstanceView;
import com.wiggle.core.NodeStats;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link DashboardData} backed by a gRPC {@link ConsoleBackend} instead of a local engine, so the
 * standalone console serves the same dashboard SPA. Instance listing fans out across the backend's
 * cells and merges newest-first; instance detail / cancel / signal route to the owning cell.
 *
 * <p>Degradations vs. the in-process dashboard (documented, not silent): pending signals aren't
 * enumerable over gRPC (returns empty), and the wire {@code Token} omits queue / leaseExpiresAt /
 * updatedAt.
 */
public final class GrpcDashboardData implements DashboardData {

    private final ConsoleBackend backend;

    public GrpcDashboardData(ConsoleBackend backend) {
        this.backend = backend;
    }

    @Override public List<String> workflowNames() {
        return backend.reads().workflowNames();
    }

    @Override public Optional<Object> workflowGraph(String name) {
        try {
            return Optional.of(backend.reads().getWorkflow(name).toJson());
        } catch (WiggleApiException e) {
            if (e.status() == 404) return Optional.empty();
            throw e;
        }
    }

    @Override public List<InstanceView> listInstances(String workflow, String status, int limit) {
        List<InstanceView> merged = new ArrayList<>();
        for (WiggleClient c : backend.cells()) {
            merged.addAll(c.listInstances(workflow, status, limit));
        }
        return newestFirstCapped(merged, limit);
    }

    @Override public List<InstanceView> findByCorrelation(String correlationId, int limit) {
        // The correlation key isn't the instance id, so we can't route to one cell -- fan the lookup
        // across every active cell and merge, exactly like listInstances.
        List<InstanceView> merged = new ArrayList<>();
        for (WiggleClient c : backend.cells()) {
            merged.addAll(c.findByCorrelation(correlationId, limit));
        }
        return newestFirstCapped(merged, limit);
    }

    private static List<InstanceView> newestFirstCapped(List<InstanceView> merged, int limit) {
        merged.sort(Comparator.comparingLong(InstanceView::createdAt).reversed());
        return merged.size() > limit ? new ArrayList<>(merged.subList(0, limit)) : merged;
    }

    @Override public Optional<InstanceDetail> instance(String id) {
        try {
            WiggleClient.InstanceWithTokens d = backend.forInstance(id).instanceDetail(id);
            List<TokenView> tokens = d.tokens().stream().map(GrpcDashboardData::token).toList();
            return Optional.of(new InstanceDetail(d.instance(), tokens));
        } catch (WiggleApiException e) {
            if (e.status() == 404) return Optional.empty();
            throw e;
        }
    }

    @Override public void cancel(String id, String reason) {
        backend.forInstance(id).cancel(id, reason);
    }

    @Override public void signal(String id, String name, Object payload) {
        backend.forInstance(id).signal(id, name, payload);
    }

    @Override public List<BacklogView> backlogCoverage(int limit) {
        // Each cell knows only its own pollers, so ask every one and merge. A slice is covered if ANY
        // cell that holds it has a worker for it -- reporting it uncovered because a sibling cell
        // happens to have no backlog there would be a false alarm.
        List<BacklogView> merged = new ArrayList<>();
        for (WiggleClient c : backend.cells()) {
            for (WiggleClient.BacklogSlice s : c.backlogCoverage(limit)) {
                merged.add(new BacklogView(s.workflow(), s.version(), s.queue(), s.readyCount(),
                        s.oldestAvailableAt(), s.covered(), s.livePollers()));
            }
        }
        merged.sort((a, b) -> {
            if (a.covered() != b.covered()) return a.covered() ? 1 : -1;   // uncovered first
            return Integer.compare(b.readyCount(), a.readyCount());
        });
        return merged.size() > limit ? merged.subList(0, limit) : merged;
    }

    /**
     * Every cell keeps its own timed steps, so ask each and merge per node. Percentiles cannot be
     * recombined from summaries: a merged row keeps the WORST cell's p50 and p95 (the bottleneck
     * view is a pessimistic one by design), sums the counts, and weights the mean by them.
     */
    @Override public List<NodeStats> stepStats(String workflow, Integer version, long since, int sample) {
        Map<String, NodeStats> merged = new java.util.LinkedHashMap<>();
        for (WiggleClient c : backend.cells()) {
            for (NodeStats n : c.stepStats(workflow, version, since, sample)) {
                merged.merge(n.nodeId(), n, (a, b) -> new NodeStats(a.nodeId(), a.name() != null ? a.name() : b.name(),
                        a.count() + b.count(),
                        (a.meanMillis() * a.count() + b.meanMillis() * b.count()) / Math.max(1, a.count() + b.count()),
                        Math.max(a.p50Millis(), b.p50Millis()), Math.max(a.p95Millis(), b.p95Millis()),
                        Math.max(a.maxMillis(), b.maxMillis())));
            }
        }
        List<NodeStats> out = new ArrayList<>(merged.values());
        out.sort(Comparator.comparingLong(NodeStats::p95Millis).reversed());
        return out;
    }

    @Override public List<AnomalyView> anomalies(String workflow, String instanceId, int limit) {
        List<AnomalyView> merged = new ArrayList<>();
        if (instanceId != null) {
            merged.addAll(backend.forInstance(instanceId).anomalies(workflow, instanceId, limit));
        } else {
            for (WiggleClient c : backend.cells()) merged.addAll(c.anomalies(workflow, null, limit));
        }
        merged.sort(Comparator.comparingLong(AnomalyView::at).reversed());
        return merged.size() > limit ? new ArrayList<>(merged.subList(0, limit)) : merged;
    }

    @Override public List<SignalView> pendingSignals(int limit) {
        return List.of();   // no gRPC enumeration of pending signals today
    }

    @Override public List<ScheduleView> schedules() {
        return backend.reads().schedules().stream()
                .map(s -> new ScheduleView(s.id(), s.workflow(), s.everyMillis(), s.cron(), s.nextFireAt(), s.createdAt()))
                .toList();
    }

    @Override public String createSchedule(String workflow, Duration every, Object context) {
        return backend.reads().createSchedule(workflow, every, context);
    }

    @Override public String createCronSchedule(String workflow, String cron, Object context) {
        return backend.reads().createCronSchedule(workflow, cron, context);
    }

    @Override public void deleteSchedule(String id) {
        backend.reads().deleteSchedule(id);
    }

    @Override public ClusterView cluster() {
        Map<String, Object> c = backend.reads().cluster();
        List<MemberView> members = new ArrayList<>();
        Object raw = c.get("members");
        if (raw instanceof List<?> list) {
            for (Object o : list) {
                if (o instanceof Map<?, ?> m) {
                    members.add(new MemberView(str(m.get("id")), str(m.get("name")), (int) num(m.get("workers")),
                            bool(m.get("leader")), bool(m.get("alive")), num(m.get("lastHeartbeat"))));
                }
            }
        }
        return new ClusterView(str(c.get("self")), bool(c.get("leader")), members);
    }

    private static TokenView token(WiggleClient.TokenInfo t) {
        // queue / leaseExpiresAt / updatedAt are not on the wire Token -> null / 0.
        return new TokenView(t.id(), t.nodeId(), t.kind(), t.status(), t.activity(),
                null, t.attempt(), t.availableAt(), t.leaseOwner(), 0, t.lastError(), 0);
    }

    private static String str(Object o) { return o == null ? null : String.valueOf(o); }
    private static long num(Object o) { return o instanceof Number n ? n.longValue() : 0; }
    private static boolean bool(Object o) { return o instanceof Boolean b && b; }
}
