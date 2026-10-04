package com.wiggle.core;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One entry of the event log: an instance lifecycle transition, recorded in the transaction
 * that made it. {@code type} is {@code wf.started}, {@code wf.completed}, {@code wf.failed},
 * {@code wf.cancelled}, {@code wf.compensating}, {@code wf.compensated} or
 * {@code wf.compensation_failed}, and {@code payload} carries the transition's reason or error;
 * or it is a handler's own {@link EmittedEvent}, and {@code nodeId} names the step that emitted it.
 *
 * <p>{@code seq} increases within one {@code shard}'s log; {@code (shard, seq)} identifies an entry.
 * {@code cursor} is the consumer's place just after this entry, to hand back on an ack; null when the
 * entry was not served by a consumer's poll.
 */
public record EventView(long seq, String instanceId, String workflow, int version, String correlationId,
                        String type, String nodeId, long createdAt, Map<String, Object> payload,
                        int shard, String cursor) {

    /** An entry of shard 0, outside any consumer's poll. */
    public EventView(long seq, String instanceId, String workflow, int version, String correlationId,
                     String type, String nodeId, long createdAt, Map<String, Object> payload) {
        this(seq, instanceId, workflow, version, correlationId, type, nodeId, createdAt, payload, 0, null);
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("seq", seq);
        m.put("instanceId", instanceId);
        m.put("workflow", workflow);
        m.put("version", version);
        m.put("correlationId", correlationId);
        m.put("type", type);
        m.put("nodeId", nodeId);
        m.put("createdAt", createdAt);
        m.put("payload", payload);
        m.put("shard", shard);
        if (cursor != null) m.put("cursor", cursor);
        return m;
    }
}
