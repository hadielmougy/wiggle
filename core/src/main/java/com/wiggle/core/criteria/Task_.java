package com.wiggle.core.criteria;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;

/** The metamodel of a claimable task: one typed {@link Attribute} per {@link Field}. */
public final class Task_ {

    public static final Attribute<String> WORKFLOW = new Attribute<>(Field.WORKFLOW, String.class);
    public static final Attribute<Integer> VERSION = new Attribute<>(Field.VERSION, Integer.class);
    public static final Attribute<String> QUEUE = new Attribute<>(Field.QUEUE, String.class);
    public static final Attribute<String> STEP = new Attribute<>(Field.STEP, String.class);
    public static final Attribute<String> NODE_ID = new Attribute<>(Field.NODE_ID, String.class);
    public static final Attribute<String> KIND = new Attribute<>(Field.KIND, String.class);
    public static final Attribute<Integer> ATTEMPT = new Attribute<>(Field.ATTEMPT, Integer.class);
    public static final Attribute<String> CORRELATION_ID = new Attribute<>(Field.CORRELATION_ID, String.class);
    public static final Attribute<Instant> INSTANCE_CREATED_AT =
            new Attribute<>(Field.INSTANCE_CREATED_AT, Instant.class);

    private static final Set<Class<?>> ATTRIBUTE_TYPES =
            Set.of(String.class, Long.class, Integer.class, BigDecimal.class, Boolean.class);

    /**
     * A declared attribute, compared as {@code type}: String for STRING, Long or Integer for LONG,
     * BigDecimal for DECIMAL (Long and BigDecimal compare numerically with either), Boolean for
     * BOOLEAN. A comparison with a value of another type is false.
     */
    public static <T> Attribute<T> attr(String name, Class<T> type) {
        if (!ATTRIBUTE_TYPES.contains(type)) {
            throw new IllegalArgumentException("attribute '" + name + "' cannot be compared as " + type.getSimpleName());
        }
        return new Attribute<>(Field.ATTRIBUTE, name, type);
    }

    private Task_() { }
}
