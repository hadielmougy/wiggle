package com.wiggle.core;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A unit of work leased by a worker from the server. For a forEach item step, {@code context} is the
 * ITEM's current value (any JSON value) and {@code baseContext} carries the frozen pre-forEach
 * context (read-only for items; null for every other step). {@code itemIndex}/{@code itemMapKey}
 * locate the element in its input collection.
 */
public record TaskActivation(String taskId, String instanceId, String workflow, int version,
                             String nodeId, String stepName, String activity, NodeKind kind,
                             int attempt, long leaseExpiresAt, String leaseOwner, Object context,
                             Object baseContext, long itemIndex, String itemMapKey,
                             ExecutionMode executionMode, String collectKey, List<String> armNames) {

    public TaskActivation {
        executionMode = executionMode == null ? ExecutionMode.SERVER : executionMode;
        armNames = armNames == null ? List.of() : List.copyOf(armNames);
    }

    public TaskActivation(String taskId, String instanceId, String workflow, int version,
                          String nodeId, String stepName, String activity, NodeKind kind,
                          int attempt, long leaseExpiresAt, String leaseOwner, Object context,
                          Object baseContext, long itemIndex, String itemMapKey,
                          ExecutionMode executionMode, String collectKey) {
        this(taskId, instanceId, workflow, version, nodeId, stepName, activity, kind, attempt,
                leaseExpiresAt, leaseOwner, context, baseContext, itemIndex, itemMapKey, executionMode,
                collectKey, List.of());
    }

    /** An activation of a step that is not a forEach's or created branches' combine. */
    public TaskActivation(String taskId, String instanceId, String workflow, int version,
                          String nodeId, String stepName, String activity, NodeKind kind,
                          int attempt, long leaseExpiresAt, String leaseOwner, Object context,
                          Object baseContext, long itemIndex, String itemMapKey,
                          ExecutionMode executionMode) {
        this(taskId, instanceId, workflow, version, nodeId, stepName, activity, kind, attempt,
                leaseExpiresAt, leaseOwner, context, baseContext, itemIndex, itemMapKey, executionMode, null,
                List.of());
    }

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
        if (baseContext != null) {
            m.put("baseContext", baseContext);
            m.put("itemIndex", itemIndex);
            if (itemMapKey != null) m.put("itemMapKey", itemMapKey);
        }
        m.put("executionMode", executionMode.name());
        if (collectKey != null) m.put("collectKey", collectKey);
        if (!armNames.isEmpty()) m.put("armNames", armNames);
        return m;
    }

    public static TaskActivation fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        return new TaskActivation(
                Json.reqStr(m, "taskId"), Json.reqStr(m, "instanceId"), Json.reqStr(m, "workflow"),
                (int) Json.num(m, "version", 0), Json.reqStr(m, "nodeId"), Json.str(m, "stepName", null),
                Json.reqStr(m, "activity"), NodeKind.valueOf(Json.reqStr(m, "kind")),
                (int) Json.num(m, "attempt", 0), Json.num(m, "leaseExpiresAt", 0),
                Json.str(m, "leaseOwner", null), m.get("context"),
                m.get("baseContext"), Json.num(m, "itemIndex", 0), Json.str(m, "itemMapKey", null),
                ExecutionMode.valueOf(Json.str(m, "executionMode", ExecutionMode.SERVER.name())),
                Json.str(m, "collectKey", null),
                Json.asArray(m.get("armNames")).stream().map(String::valueOf).toList());
    }
}
