package com.wiggle.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One departure of an observed run from its topology. {@code kind} is one of
 * {@code OUT_OF_ORDER} (a step reported where another was expected), {@code UNKNOWN_NODE} (a step
 * the graph has no node for), {@code AFTER_END} (steps reported once the instance had ended) or
 * {@code INCOMPLETE} (the run closed before reaching END).
 */
public record AnomalyView(String instanceId, String workflow, int version, String kind,
                          String expectedNode, String reportedNode, String detail, long at) {

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("instanceId", instanceId);
        m.put("workflow", workflow);
        m.put("version", (long) version);
        m.put("kind", kind);
        if (expectedNode != null) m.put("expectedNode", expectedNode);
        if (reportedNode != null) m.put("reportedNode", reportedNode);
        if (detail != null) m.put("detail", detail);
        m.put("at", at);
        return m;
    }

    public static AnomalyView fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        return new AnomalyView(Json.reqStr(m, "instanceId"), Json.reqStr(m, "workflow"),
                (int) Json.num(m, "version", 0), Json.reqStr(m, "kind"),
                Json.str(m, "expectedNode", null), Json.str(m, "reportedNode", null),
                Json.str(m, "detail", null), Json.num(m, "at", 0));
    }
}
