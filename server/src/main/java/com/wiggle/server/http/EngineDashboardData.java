package com.wiggle.server.http;

import com.wiggle.core.InstanceView;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.server.store.Rows.Token;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** {@link DashboardData} over the in-process engine + cluster -- what an embedded cell dashboard uses. */
public final class EngineDashboardData implements DashboardData {

    private final WorkflowEngine engine;
    private final ClusterManager cluster;

    public EngineDashboardData(WorkflowEngine engine, ClusterManager cluster) {
        this.engine = engine;
        this.cluster = cluster;
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

    @Override public Optional<InstanceDetail> instance(String id) {
        return engine.instance(id).map(v -> new InstanceDetail(v, engine.tokens(id).stream().map(EngineDashboardData::token).toList()));
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
        List<MemberView> members = cluster.members().stream().map(n -> member(n, now, deadAfter)).toList();
        return new ClusterView(cluster.nodeId(), cluster.isLeader(), members);
    }

    private static TokenView token(Token t) {
        return new TokenView(t.id, t.nodeId, t.kind == null ? null : t.kind.name(),
                t.status == null ? null : t.status.name(), t.activity, t.queue, t.attempt,
                t.availableAt, t.leaseOwner, t.leaseExpiresAt, t.lastError, t.updatedAt);
    }

    private static MemberView member(ServerNode n, long now, long deadAfter) {
        return new MemberView(n.id, n.name, n.workers, n.leader, (now - n.lastHeartbeat) < deadAfter, n.lastHeartbeat);
    }
}
