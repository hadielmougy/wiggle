package com.wiggle.server.engine;

import com.wiggle.core.Json;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Tx;

import java.util.LinkedHashMap;
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
        append(tx, inst, STARTED, Map.of(), now);
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
        append(tx, inst, type, payload, now);
    }

    private static void append(Tx tx, Instance inst, String type, Map<String, Object> payload, long now) {
        tx.appendEvent(new Rows.Event(0, inst.id, inst.workflow, inst.version, inst.correlationId,
                type, PAYLOAD_VERSION, Json.write(payload), now));
    }
}
