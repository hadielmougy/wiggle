package com.wiggle.core.criteria;

import java.time.Instant;

/** The metamodel of a claimable task: one typed {@link Attribute} per {@link Field}. */
public final class Task_ {

    public static final Attribute<String> WORKFLOW = new Attribute<>(Field.WORKFLOW, String.class);
    public static final Attribute<Integer> VERSION = new Attribute<>(Field.VERSION, Integer.class);
    public static final Attribute<String> STEP = new Attribute<>(Field.STEP, String.class);
    public static final Attribute<String> NODE_ID = new Attribute<>(Field.NODE_ID, String.class);
    public static final Attribute<String> KIND = new Attribute<>(Field.KIND, String.class);
    public static final Attribute<Integer> ATTEMPT = new Attribute<>(Field.ATTEMPT, Integer.class);
    public static final Attribute<String> CORRELATION_ID = new Attribute<>(Field.CORRELATION_ID, String.class);
    public static final Attribute<Instant> INSTANCE_CREATED_AT =
            new Attribute<>(Field.INSTANCE_CREATED_AT, Instant.class);

    private Task_() { }
}
