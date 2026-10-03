package com.wiggle.console;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.core.InstanceView;
import com.wiggle.core.NodeStats;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link DashboardData} backed by a gRPC {@link WiggleClient} instead of a local engine, so the
 * standalone console serves the same dashboard SPA.
 *
 * <p>Degradations vs. the in-process dashboard (documented, not silent): pending signals aren't
 * enumerable over gRPC (returns empty), and the wire {@code Token} omits queue / leaseExpiresAt /
 * updatedAt.
 */
public final class GrpcDashboardData implements DashboardData {

    private final WiggleClient client;

    public GrpcDashboardData(WiggleClient client) {
        this.client = client;
    }

    @Override public List<String> workflowNames() {
        return client.workflowNames();
    }

    @Override public Optional<Object> workflowGraph(String name) {
        try {
            return Optional.of(client.getWorkflow(name).toJson());
        } catch (WiggleApiException e) {
            if (e.status() == 404) return Optional.empty();
            throw e;
        }
    }

    @Override public List<InstanceView> listInstances(String workflow, String status, int limit) {
        return client.listInstances(workflow, status, limit);
    }

    @Override public List<InstanceView> findByCorrelation(String correlationId, int limit) {
        return client.findByCorrelation(correlationId, limit);
    }

    @Override public Optional<InstanceDetail> instance(String id) {
        try {
            WiggleClient.InstanceWithTokens d = client.instanceDetail(id);
            List<TokenView> tokens = d.tokens().stream().map(GrpcDashboardData::token).toList();
            return Optional.of(new InstanceDetail(d.instance(), tokens));
        } catch (WiggleApiException e) {
            if (e.status() == 404) return Optional.empty();
            throw e;
        }
    }

    @Override public void cancel(String id, String reason) {
        client.cancel(id, reason);
    }

    @Override public void signal(String id, String name, Object payload) {
        client.signal(id, name, payload);
    }

    @Override public List<BacklogView> backlogCoverage(int limit) {
        List<BacklogView> out = new ArrayList<>();
        for (WiggleClient.BacklogSlice s : client.backlogCoverage(limit)) {
            out.add(new BacklogView(s.workflow(), s.version(), s.queue(), s.readyCount(),
                    s.oldestAvailableAt(), s.covered(), s.livePollers()));
        }
        out.sort((a, b) -> {
            if (a.covered() != b.covered()) return a.covered() ? 1 : -1;   // uncovered first
            return Integer.compare(b.readyCount(), a.readyCount());
        });
        return out;
    }

    @Override public List<NodeStats> stepStats(String workflow, Integer version, long since, int sample) {
        List<NodeStats> out = new ArrayList<>(client.stepStats(workflow, version, since, sample));
        out.sort(Comparator.comparingLong(NodeStats::p95Millis).reversed());
        return out;
    }

    @Override public List<SignalView> pendingSignals(int limit) {
        return List.of();   // no gRPC enumeration of pending signals today
    }

    @Override public List<ScheduleView> schedules() {
        return client.schedules().stream()
                .map(s -> new ScheduleView(s.id(), s.workflow(), s.everyMillis(), s.cron(), s.nextFireAt(), s.createdAt()))
                .toList();
    }

    @Override public String createSchedule(String workflow, Duration every, Object context) {
        return client.createSchedule(workflow, every, context);
    }

    @Override public String createCronSchedule(String workflow, String cron, Object context) {
        return client.createCronSchedule(workflow, cron, context);
    }

    @Override public void deleteSchedule(String id) {
        client.deleteSchedule(id);
    }

    @Override public ClusterView cluster() {
        Map<String, Object> c = client.cluster();
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
                null, t.attempt(), t.availableAt(), t.leaseOwner(), 0, t.lastError(), 0,
                t.startedAt(), t.finishedAt(), t.createdAt(), t.input(), t.output());
    }

    private static String str(Object o) { return o == null ? null : String.valueOf(o); }
    private static long num(Object o) { return o instanceof Number n ? n.longValue() : 0; }
    private static boolean bool(Object o) { return o instanceof Boolean b && b; }
}
