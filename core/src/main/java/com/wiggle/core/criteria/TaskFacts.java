package com.wiggle.core.criteria;

import java.util.Map;

/**
 * The properties of one task a predicate is evaluated against. {@code correlationId} may be null;
 * {@code attributes} holds the declared attributes present, by name.
 */
public record TaskFacts(String workflow, int version, String queue, String step, String nodeId,
                        String kind, int attempt, String correlationId, long instanceCreatedAt,
                        Map<String, Object> attributes) implements Criteria.Facts {

    public TaskFacts {
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    public TaskFacts(String workflow, int version, String queue, String step, String nodeId,
                     String kind, int attempt, String correlationId, long instanceCreatedAt) {
        this(workflow, version, queue, step, nodeId, kind, attempt, correlationId, instanceCreatedAt, Map.of());
    }

    /** The step name carried by an activity id ({@code workflow#step}). */
    public static String stepOf(String workflow, String activity) {
        if (activity == null) return null;
        String prefix = workflow + "#";
        return activity.startsWith(prefix) ? activity.substring(prefix.length()) : activity;
    }

    /** The field's value as a literal, or null when absent. */
    @Override public Object value(Field field, String attribute) {
        return switch (field) {
            case WORKFLOW -> workflow;
            case VERSION -> (long) version;
            case STEP -> step;
            case NODE_ID -> nodeId;
            case KIND -> kind;
            case ATTEMPT -> (long) attempt;
            case CORRELATION_ID -> correlationId;
            case INSTANCE_CREATED_AT -> instanceCreatedAt;
            case ATTRIBUTE -> attributes.get(attribute);
        };
    }
}
