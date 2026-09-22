package com.wiggle.observe;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What identifies a run across a message boundary: the workflow, its version, and the run's
 * key. Travels as three string headers, so any transport that carries headers -- a broker, HTTP,
 * gRPC metadata -- can hand a run from one service to the next; the receiving side
 * {@link Observed#join(RunContext) joins} it.
 */
public record RunContext(String workflow, int version, String correlationId) {

    public static final String WORKFLOW_HEADER = "wiggle-workflow";
    public static final String VERSION_HEADER = "wiggle-version";
    public static final String RUN_HEADER = "wiggle-run";

    public RunContext {
        if (workflow == null || workflow.isBlank()) throw new IllegalArgumentException("a run context names its workflow");
        if (correlationId == null || correlationId.isBlank()) throw new IllegalArgumentException("a run context carries its key");
    }

    /** The three headers, insertion-ordered. */
    public Map<String, String> toHeaders() {
        Map<String, String> h = new LinkedHashMap<>();
        h.put(WORKFLOW_HEADER, workflow);
        h.put(VERSION_HEADER, Integer.toString(version));
        h.put(RUN_HEADER, correlationId);
        return h;
    }

    /** The context the headers carry, or null when the run header is absent. */
    public static RunContext fromHeaders(Map<String, String> headers) {
        if (headers == null) return null;
        String key = headers.get(RUN_HEADER);
        String workflow = headers.get(WORKFLOW_HEADER);
        if (key == null || key.isBlank() || workflow == null || workflow.isBlank()) return null;
        String v = headers.get(VERSION_HEADER);
        int version;
        try {
            version = v == null || v.isBlank() ? 0 : Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            version = 0;
        }
        return new RunContext(workflow, version, key);
    }
}
