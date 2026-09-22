package com.wiggle.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One entry of the event log: an instance lifecycle transition, recorded in the transaction
 * that made it. {@code type} is {@code wf.started}, {@code wf.completed}, {@code wf.failed},
 * {@code wf.cancelled}, {@code wf.compensating}, {@code wf.compensated} or
 * {@code wf.compensation_failed}; {@code payload} carries the transition's reason or error.
 */
public record EventView(long seq, String instanceId, String workflow, int version, String correlationId,
                        String type, long createdAt, Map<String, Object> payload) {

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("seq", seq);
        m.put("instanceId", instanceId);
        m.put("workflow", workflow);
        m.put("version", version);
        m.put("correlationId", correlationId);
        m.put("type", type);
        m.put("createdAt", createdAt);
        m.put("payload", payload);
        return m;
    }
}
