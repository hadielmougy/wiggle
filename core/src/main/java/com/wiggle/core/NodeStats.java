package com.wiggle.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Duration statistics for one node over a window of settled steps: how many ran, and the mean,
 * median, 95th percentile and maximum of their wall-clock durations in millis.
 */
public record NodeStats(String nodeId, String name, long count, double meanMillis,
                        long p50Millis, long p95Millis, long maxMillis) {

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", nodeId);
        m.put("name", name);
        m.put("count", count);
        m.put("meanMillis", meanMillis);
        m.put("p50Millis", p50Millis);
        m.put("p95Millis", p95Millis);
        m.put("maxMillis", maxMillis);
        return m;
    }

    public static NodeStats fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        return new NodeStats(Json.reqStr(m, "nodeId"), Json.str(m, "name", null),
                Json.num(m, "count", 0), Json.dbl(m, "meanMillis", 0),
                Json.num(m, "p50Millis", 0), Json.num(m, "p95Millis", 0), Json.num(m, "maxMillis", 0));
    }
}
