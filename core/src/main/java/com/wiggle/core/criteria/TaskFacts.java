package com.wiggle.core.criteria;

/** The properties of one task a predicate is evaluated against. {@code correlationId} may be null. */
public record TaskFacts(String workflow, int version, String queue, String step, String nodeId,
                        String kind, int attempt, String correlationId, long instanceCreatedAt) {

    /** The step name carried by an activity id ({@code workflow#step}). */
    public static String stepOf(String workflow, String activity) {
        if (activity == null) return null;
        String prefix = workflow + "#";
        return activity.startsWith(prefix) ? activity.substring(prefix.length()) : activity;
    }

    /** The field's value as a literal: a String, a Long, or null when absent. */
    public Object value(Field field) {
        return switch (field) {
            case WORKFLOW -> workflow;
            case VERSION -> (long) version;
            case STEP -> step;
            case NODE_ID -> nodeId;
            case KIND -> kind;
            case ATTEMPT -> (long) attempt;
            case CORRELATION_ID -> correlationId;
            case INSTANCE_CREATED_AT -> instanceCreatedAt;
        };
    }
}
