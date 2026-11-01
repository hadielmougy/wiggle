package com.wiggle.client;

import com.wiggle.core.NodeKind;
import com.wiggle.core.wire.Msg;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The wire↔domain seam: every conversion between a wire message and the view types the API
 *  returns, so a client method reads as request → call → view. */
final class Wire {

    private Wire() {}

    static com.wiggle.core.InstanceView instanceView(Msg v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", v.str("id"));
        m.put("workflow", v.str("workflow"));
        m.put("version", (long) v.integer("version"));
        m.put("status", v.str("status"));
        if (v.has("terminationReason")) m.put("terminationReason", v.str("terminationReason"));
        if (v.has("error")) m.put("error", v.str("error"));
        if (v.has("context")) m.put("context", v.value("context"));
        m.put("createdAt", v.lng("createdAt"));
        m.put("updatedAt", v.lng("updatedAt"));
        return com.wiggle.core.InstanceView.fromJson(m);
    }

    static com.wiggle.core.TaskActivation taskActivation(Msg t) {
        return new com.wiggle.core.TaskActivation(
                t.str("taskId"), t.str("instanceId"), t.str("workflow"), t.integer("version"),
                t.str("nodeId"), t.str("stepName").isEmpty() ? null : t.str("stepName"), t.str("activity"),
                NodeKind.valueOf(t.str("kind")), t.integer("attempt"), t.lng("leaseExpiresAt"), t.str("leaseOwner"),
                t.value("context"),
                t.value("baseContext"),
                t.lng("itemIndex"),
                t.str("itemMapKey").isEmpty() ? null : t.str("itemMapKey"),
                t.str("executionMode").isEmpty()
                        ? com.wiggle.core.ExecutionMode.SERVER
                        : com.wiggle.core.ExecutionMode.valueOf(t.str("executionMode")),
                t.str("collectKey").isEmpty() ? null : t.str("collectKey"),
                t.strs("armNames"),
                t.str("correlationId").isEmpty() ? null : t.str("correlationId"),
                t.lng("instanceCreatedAt"));
    }

    static Msg criteria(com.wiggle.core.criteria.QueueCriteria c) {
        Msg b = Msg.create().put("queue", c.queue());
        if (c.where() != null) b.put("where", predicate(c.where()));
        return b;
    }

    private static Msg predicate(com.wiggle.core.criteria.Predicate p) {
        Msg b = Msg.create();
        switch (p) {
            case com.wiggle.core.criteria.Predicate.Comparison c -> {
                Msg cmp = Msg.create()
                        .put("field", c.field().name()).put("op", c.op().name());
                if (c.attribute() != null) cmp.put("attribute", c.attribute());
                for (Object arg : c.args()) {
                    Msg l = Msg.create();
                    switch (arg) {
                        case Long n -> l.put("integer", n);
                        case java.math.BigDecimal d -> l.put("decimal", d.toPlainString());
                        case Boolean f -> l.put("flag", f);
                        default -> l.put("text", (String) arg);
                    }
                    cmp.add("args", l);
                }
                b.put("cmp", cmp);
            }
            case com.wiggle.core.criteria.Predicate.And a -> b.put("allOf", junction(a.terms()));
            case com.wiggle.core.criteria.Predicate.Or o -> b.put("anyOf", junction(o.terms()));
            case com.wiggle.core.criteria.Predicate.Not n -> b.put("negated", predicate(n.term()));
        }
        return b;
    }

    private static Msg junction(List<com.wiggle.core.criteria.Predicate> terms) {
        Msg j = Msg.create();
        for (com.wiggle.core.criteria.Predicate t : terms) j.add("terms", predicate(t));
        return j;
    }

    static WiggleClient.TokenInfo tokenInfo(Msg t) {
        return new WiggleClient.TokenInfo(t.str("id"), t.str("nodeId"), t.str("kind"), t.str("status"), t.str("activity"),
                t.integer("attempt"), t.lng("availableAt"), t.has("leaseOwner") ? t.str("leaseOwner") : null,
                t.has("lastError") ? t.str("lastError") : null,
                t.has("startedAt") ? t.lng("startedAt") : null, t.has("finishedAt") ? t.lng("finishedAt") : null,
                t.lng("createdAt"),
                t.value("input"),
                t.value("output"));
    }

    static List<WiggleClient.BacklogSlice> backlogSlices(Msg res) {
        List<WiggleClient.BacklogSlice> out = new ArrayList<>(res.msgs("slices").size());
        for (Msg s : res.msgs("slices")) {
            out.add(new WiggleClient.BacklogSlice(s.str("workflow"), s.integer("version"), s.str("queue"),
                    s.integer("readyCount"), s.lng("oldestAvailableAt"), s.bool("covered"), res.integer("livePollers"),
                    s.str("coverage"), s.has("claimable") ? s.integer("claimable") : null, List.copyOf(s.strs("criteria"))));
        }
        return out;
    }

    static List<com.wiggle.core.NodeStats> nodeStats(Msg res) {
        List<com.wiggle.core.NodeStats> out = new ArrayList<>(res.msgs("nodes").size());
        for (Msg n : res.msgs("nodes")) {
            out.add(new com.wiggle.core.NodeStats(n.str("nodeId"), n.str("name").isEmpty() ? null : n.str("name"),
                    n.lng("count"), n.dbl("meanMillis"), n.lng("p50Millis"), n.lng("p95Millis"), n.lng("maxMillis"),
                    n.lng("waitP50Millis"), n.lng("waitP95Millis")));
        }
        return out;
    }

    static Msg emitted(com.wiggle.core.EmittedEvent e) {
        Msg b = Msg.create().put("type", e.type());
        if (e.payload() != null) b.put("payload", e.payload());
        return b;
    }

    static Msg branch(com.wiggle.core.CreatedBranch b) {
        Msg out = Msg.create()
                .put("input", com.wiggle.core.RecordMapper.toJson(b.input()));
        if (b.key() != null) out.put("key", b.key());
        for (com.wiggle.core.CreatedBranch.BranchStep st : b.steps()) out.add("steps", branchStep(st));
        return out;
    }

    private static Msg branchStep(com.wiggle.core.CreatedBranch.BranchStep st) {
        Msg sb = Msg.create()
                .put("kind", st.kind().name())
                .put("compensable", st.compensable())
                .put("sleepMillis", st.sleepMillis());
        if (st.name() != null) sb.put("name", st.name());
        if (st.queue() != null) sb.put("queue", st.queue());
        if (st.retry() != null) sb.put("retry", st.retry().toJson());
        if (st.combine() != null) sb.put("combine", st.combine());
        if (st.workflow() != null) sb.put("workflow", st.workflow());
        for (com.wiggle.core.CreatedBranch.BranchStep e : st.escalation()) sb.add("escalation", branchStep(e));
        for (List<com.wiggle.core.CreatedBranch.BranchStep> arm : st.arms()) {
            Msg ab = Msg.create();
            for (com.wiggle.core.CreatedBranch.BranchStep a : arm) ab.add("steps", branchStep(a));
            sb.add("arms", ab);
        }
        for (com.wiggle.core.CreatedBranch.BranchStep b : st.body()) sb.add("body", branchStep(b));
        sb.put("loopBudget", st.loopBudget());
        for (com.wiggle.core.CreatedBranch.BranchStep.Case c : st.cases()) {
            Msg cb = Msg.create();
            if (c.guard() != null) cb.put("guard", c.guard());
            for (com.wiggle.core.CreatedBranch.BranchStep cs : c.steps()) cb.add("steps", branchStep(cs));
            sb.add("cases", cb);
        }
        return sb;
    }

    static List<com.wiggle.core.EventView> events(Msg res) {
        List<com.wiggle.core.EventView> out = new ArrayList<>(res.msgs("events").size());
        for (Msg e : res.msgs("events")) {
            Object payload = e.value("payload");
            out.add(new com.wiggle.core.EventView(e.lng("seq"), e.str("instanceId"), e.str("workflow"), e.integer("version"),
                    e.str("correlationId").isEmpty() ? null : e.str("correlationId"), e.str("type"),
                    e.str("nodeId").isEmpty() ? null : e.str("nodeId"), e.lng("createdAt"),
                    payload instanceof Map<?, ?> m ? com.wiggle.core.Json.asObject(m) : Map.of(),
                    e.integer("shard"), e.str("cursor").isEmpty() ? null : e.str("cursor")));
        }
        return out;
    }

    static Map<String, Object> clusterMap(Msg v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("self", v.str("self"));
        m.put("leader", v.bool("leader"));
        List<Object> members = new ArrayList<>();
        for (Msg cm : v.msgs("members")) {
            Map<String, Object> mm = new LinkedHashMap<>();
            mm.put("id", cm.str("id"));
            mm.put("name", cm.str("name"));
            mm.put("firstHeartbeat", cm.lng("firstHeartbeat"));
            mm.put("lastHeartbeat", cm.lng("lastHeartbeat"));
            mm.put("workers", (long) cm.integer("workers"));
            mm.put("leader", cm.bool("leader"));
            mm.put("alive", cm.bool("alive"));
            members.add(mm);
        }
        m.put("members", members);
        return m;
    }

    static com.wiggle.core.ReportResult reportResult(Msg res) {
        return new com.wiggle.core.ReportResult(res.str("instanceStatus"), res.lng("leaseExpiresAt"),
                res.str("nextTaskId").isEmpty() ? null : res.str("nextTaskId"));
    }

    static WiggleClient.ScheduleInfo scheduleInfo(Msg s) {
        return new WiggleClient.ScheduleInfo(s.str("id"), s.str("workflow"), s.lng("everyMillis"),
                s.str("cron").isEmpty() ? null : s.str("cron"), s.lng("nextFireAt"), s.lng("createdAt"));
    }

    /** One reported step on the wire: exactly one of merge (task) or predicateValue (predicate). */
    static Msg stepResult(WiggleClient.StepReport s) {
        Msg sr = Msg.create().put("nodeId", s.nodeId());
        for (com.wiggle.core.EmittedEvent e : s.events()) sr.add("events", emitted(e));
        for (com.wiggle.core.CreatedBranch b : s.branches()) sr.add("branches", branch(b));
        if (s.startedAt() != null && s.finishedAt() != null) sr.put("startedAt", s.startedAt()).put("finishedAt", s.finishedAt());
        if (s.predicateValue() != null) sr.put("predicateValue", s.predicateValue());
        else if (s.merge() != null) sr.put("merge", com.wiggle.core.RecordMapper.toJson(s.merge()));
        return sr;
    }
}
