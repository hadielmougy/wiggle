package com.wiggle.console;

import com.wiggle.console.DashboardData.ClusterView;
import com.wiggle.console.DashboardData.MemberView;
import com.wiggle.console.DashboardData.ScheduleView;
import com.wiggle.console.DashboardData.SignalView;
import com.wiggle.console.DashboardData.TokenView;
import com.wiggle.core.InstanceView;

import java.util.LinkedHashMap;
import java.util.Map;

/** The JSON shapes the dashboard SPA consumes -- the wire contract, kept in one place. */
final class DashboardJson {

    private DashboardJson() {}

    static Map<String, Object> instance(InstanceView v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", v.id());
        m.put("workflow", v.workflow());
        m.put("version", v.version());
        m.put("status", v.status());
        m.put("terminationReason", v.terminationReason());
        m.put("error", v.error());
        m.put("context", v.context());
        m.put("createdAt", v.createdAt());
        m.put("updatedAt", v.updatedAt());
        return m;
    }

    static Map<String, Object> token(TokenView t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", t.id());
        m.put("nodeId", t.nodeId());
        m.put("kind", t.kind());
        m.put("status", t.status());
        m.put("activity", t.activity());
        m.put("queue", t.queue());
        m.put("attempt", t.attempt());
        m.put("availableAt", t.availableAt());
        m.put("leaseOwner", t.leaseOwner());
        m.put("leaseExpiresAt", t.leaseExpiresAt());
        m.put("lastError", t.lastError());
        m.put("updatedAt", t.updatedAt());
        return m;
    }

    static Map<String, Object> signal(SignalView t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("instanceId", t.instanceId());
        m.put("workflow", t.workflow());
        m.put("signal", t.signal());
        m.put("deadline", t.deadline());
        m.put("createdAt", t.createdAt());
        return m;
    }

    static Map<String, Object> schedule(ScheduleView s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", s.id());
        m.put("workflow", s.workflow());
        m.put("everyMillis", s.everyMillis());
        if (s.cron() != null) m.put("cron", s.cron());
        m.put("nextFireAt", s.nextFireAt());
        m.put("createdAt", s.createdAt());
        return m;
    }

    static Map<String, Object> member(MemberView n) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", n.id());
        m.put("name", n.name());
        m.put("workers", n.workers());
        m.put("leader", n.leader());
        m.put("alive", n.alive());
        m.put("lastHeartbeat", n.lastHeartbeat());
        return m;
    }

    static Map<String, Object> cluster(ClusterView v) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("nodeId", v.nodeId());
        out.put("leader", v.leader());
        java.util.List<Object> members = new java.util.ArrayList<>();
        for (MemberView n : v.members()) members.add(member(n));
        out.put("members", members);
        return out;
    }
}
