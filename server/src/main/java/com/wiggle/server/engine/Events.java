package com.wiggle.server.engine;

import com.wiggle.core.EmittedEvent;
import com.wiggle.core.Json;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Tx;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The event log's writer: one entry per instance lifecycle transition, appended in the
 * transaction that makes the transition, so a reader of the log and a reader of the instance
 * never disagree. Types carry the reserved {@code wf.} prefix; the payload is the transition's
 * reason or error, versioned as envelope 1.
 */
final class Events {

    static final String STARTED = "wf.started";
    static final String COMPLETED = "wf.completed";
    static final String FAILED = "wf.failed";
    static final String CANCELLED = "wf.cancelled";
    static final String COMPENSATING = "wf.compensating";
    static final String COMPENSATED = "wf.compensated";
    static final String COMPENSATION_FAILED = "wf.compensation_failed";
    static final int PAYLOAD_VERSION = 1;

    private Events() {}

    static void started(Tx tx, Instance inst, long now) {
        append(tx, inst, STARTED, null, Map.of(), now);
    }

    /**
     * Appends what a handler emitted while {@code nodeId} ran. Called inside the transaction that
     * settles the step, so an attempt that threw leaves nothing and its retry emits fresh.
     */
    static void emitted(Tx tx, Instance inst, String nodeId, List<EmittedEvent> events, long now) {
        for (EmittedEvent e : events) {
            if (!(e.payload() instanceof Map<?, ?> payload)) {
                throw EngineException.badRequest("event '" + e.type() + "' carries a "
                        + (e.payload() == null ? "null" : e.payload().getClass().getSimpleName())
                        + " payload; an event's payload is a JSON object, so a consumer can read a field from it");
            }
            append(tx, inst, e.type(), nodeId, payload, now);
        }
    }

    /** The entry for the status {@code inst} has just been moved to, from the fields that explain it. */
    static void moved(Tx tx, Instance inst, long now) {
        InstanceStatus s = inst.status;
        String type = switch (s) {
            case COMPLETED -> COMPLETED;
            case FAILED -> FAILED;
            case CANCELLED -> CANCELLED;
            case COMPENSATING -> COMPENSATING;
            case COMPENSATED -> COMPENSATED;
            case COMPENSATION_FAILED -> COMPENSATION_FAILED;
            default -> null;
        };
        if (type == null) return;
        Map<String, Object> payload = new LinkedHashMap<>();
        if (inst.terminationReason != null) payload.put("reason", inst.terminationReason);
        if (inst.error != null) payload.put("error", inst.error);
        append(tx, inst, type, null, payload, now);
    }

    private static void append(Tx tx, Instance inst, String type, String nodeId,
                               Map<?, ?> payload, long now) {
        tx.appendEvent(new Rows.Event(0, inst.id, inst.workflow, inst.version, inst.correlationId,
                type, nodeId, PAYLOAD_VERSION, Json.write(payload), now));
    }
}
