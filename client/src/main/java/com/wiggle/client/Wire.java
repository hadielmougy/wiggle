package com.wiggle.client;

import com.wiggle.core.NodeKind;
import com.wiggle.proto.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The proto↔domain seam: every conversion between a wire message and the view types the API
 *  returns, so a client method reads as request → call → view. */
final class Wire {

    private Wire() {}

    static com.wiggle.core.InstanceView instanceView(InstanceView v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", v.getId());
        m.put("workflow", v.getWorkflow());
        m.put("version", (long) v.getVersion());
        m.put("status", v.getStatus());
        if (v.hasTerminationReason()) m.put("terminationReason", v.getTerminationReason());
        if (v.hasError()) m.put("error", v.getError());
        if (v.hasContext()) m.put("context", ProtoJson.fromValue(v.getContext()));
        m.put("createdAt", v.getCreatedAt());
        m.put("updatedAt", v.getUpdatedAt());
        return com.wiggle.core.InstanceView.fromJson(m);
    }

    static com.wiggle.core.TaskActivation taskActivation(TaskActivation t) {
        return new com.wiggle.core.TaskActivation(
                t.getTaskId(), t.getInstanceId(), t.getWorkflow(), t.getVersion(),
                t.getNodeId(), t.getStepName().isEmpty() ? null : t.getStepName(), t.getActivity(),
                NodeKind.valueOf(t.getKind()), t.getAttempt(), t.getLeaseExpiresAt(), t.getLeaseOwner(),
                t.hasContext() ? ProtoJson.fromValue(t.getContext()) : null,
                t.hasBaseContext() ? ProtoJson.fromValue(t.getBaseContext()) : null,
                t.getItemIndex(),
                t.getItemMapKey().isEmpty() ? null : t.getItemMapKey(),
                t.getExecutionMode().isEmpty()
                        ? com.wiggle.core.ExecutionMode.SERVER
                        : com.wiggle.core.ExecutionMode.valueOf(t.getExecutionMode()));
    }

    static WiggleClient.TokenInfo tokenInfo(Token t) {
        return new WiggleClient.TokenInfo(t.getId(), t.getNodeId(), t.getKind(), t.getStatus(), t.getActivity(),
                t.getAttempt(), t.getAvailableAt(), t.hasLeaseOwner() ? t.getLeaseOwner() : null,
                t.hasLastError() ? t.getLastError() : null);
    }

    static List<WiggleClient.BacklogSlice> backlogSlices(BacklogCoverage res) {
        List<WiggleClient.BacklogSlice> out = new ArrayList<>(res.getSlicesCount());
        for (BacklogSlice s : res.getSlicesList()) {
            out.add(new WiggleClient.BacklogSlice(s.getWorkflow(), s.getVersion(), s.getQueue(),
                    s.getReadyCount(), s.getOldestAvailableAt(), s.getCovered(), res.getLivePollers()));
        }
        return out;
    }

    static List<com.wiggle.core.NodeStats> nodeStats(com.wiggle.proto.StepStats res) {
        List<com.wiggle.core.NodeStats> out = new ArrayList<>(res.getNodesCount());
        for (com.wiggle.proto.NodeStats n : res.getNodesList()) {
            out.add(new com.wiggle.core.NodeStats(n.getNodeId(), n.getName().isEmpty() ? null : n.getName(),
                    n.getCount(), n.getMeanMillis(), n.getP50Millis(), n.getP95Millis(), n.getMaxMillis()));
        }
        return out;
    }

    static List<com.wiggle.core.AnomalyView> anomalies(com.wiggle.proto.AnomalyList res) {
        List<com.wiggle.core.AnomalyView> out = new ArrayList<>(res.getAnomaliesCount());
        for (com.wiggle.proto.Anomaly a : res.getAnomaliesList()) {
            out.add(new com.wiggle.core.AnomalyView(a.getInstanceId(), a.getWorkflow(), a.getVersion(), a.getKind(),
                    a.hasExpectedNode() ? a.getExpectedNode() : null, a.hasReportedNode() ? a.getReportedNode() : null,
                    a.hasDetail() ? a.getDetail() : null, a.getAt()));
        }
        return out;
    }

    static Map<String, Object> clusterMap(ClusterView v) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("self", v.getSelf());
        m.put("leader", v.getLeader());
        List<Object> members = new ArrayList<>();
        for (ClusterMember cm : v.getMembersList()) {
            Map<String, Object> mm = new LinkedHashMap<>();
            mm.put("id", cm.getId());
            mm.put("name", cm.getName());
            mm.put("firstHeartbeat", cm.getFirstHeartbeat());
            mm.put("lastHeartbeat", cm.getLastHeartbeat());
            mm.put("workers", (long) cm.getWorkers());
            mm.put("leader", cm.getLeader());
            mm.put("alive", cm.getAlive());
            members.add(mm);
        }
        m.put("members", members);
        return m;
    }

    static com.wiggle.core.AdvanceResult advanceResult(AdvanceRunResult res) {
        return new com.wiggle.core.AdvanceResult(res.getInstanceStatus(), res.getLeaseExpiresAt(),
                res.getNextTaskId().isEmpty() ? null : res.getNextTaskId());
    }

    static WiggleClient.ScheduleInfo scheduleInfo(ScheduleView s) {
        return new WiggleClient.ScheduleInfo(s.getId(), s.getWorkflow(), s.getEveryMillis(),
                s.getCron().isEmpty() ? null : s.getCron(), s.getNextFireAt(), s.getCreatedAt());
    }

    /** One reported step on the wire: exactly one of merge (task) or predicateValue (predicate). */
    static StepResult stepResult(WiggleClient.StepReport s) {
        StepResult.Builder sr = StepResult.newBuilder().setNodeId(s.nodeId());
        if (s.predicateValue() != null) sr.setPredicateValue(s.predicateValue());
        else if (s.merge() != null) sr.setMerge(ProtoJson.toValue(s.merge()));
        return sr.build();
    }
}
