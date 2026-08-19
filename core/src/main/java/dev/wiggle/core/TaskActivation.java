package dev.wiggle.core;

import java.util.LinkedHashMap;
import java.util.Map;

/** A unit of work leased by a worker from the server. */
public record TaskActivation(String taskId, String instanceId, String workflow, int version,
                             String nodeId, String stepName, String activity, NodeKind kind,
                             int attempt, long leaseExpiresAt, String leaseOwner, Object context) {

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("taskId", taskId);
        m.put("instanceId", instanceId);
        m.put("workflow", workflow);
        m.put("version", (long) version);
        m.put("nodeId", nodeId);
        m.put("stepName", stepName);
        m.put("activity", activity);
        m.put("kind", kind.name());
        m.put("attempt", (long) attempt);
        m.put("leaseExpiresAt", leaseExpiresAt);
        m.put("leaseOwner", leaseOwner);
        m.put("context", context);
        return m;
    }

    public static TaskActivation fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        return new TaskActivation(
                Json.reqStr(m, "taskId"), Json.reqStr(m, "instanceId"), Json.reqStr(m, "workflow"),
                (int) Json.num(m, "version", 0), Json.reqStr(m, "nodeId"), Json.str(m, "stepName", null),
                Json.reqStr(m, "activity"), NodeKind.valueOf(Json.reqStr(m, "kind")),
                (int) Json.num(m, "attempt", 0), Json.num(m, "leaseExpiresAt", 0),
                Json.str(m, "leaseOwner", null), m.get("context"));
    }
}
