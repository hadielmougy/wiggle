package com.wiggle.core.criteria;

import java.time.Instant;

/**
 * A typed reference to a task property, the counterpart of a JPA metamodel attribute. The
 * constants live on {@link Task_}.
 *
 * @param <T> the Java type a comparison on this attribute takes
 */
public final class Attribute<T> {

    private final Field field;
    private final Class<T> javaType;

    Attribute(Field field, Class<T> javaType) {
        this.field = field;
        this.javaType = javaType;
    }

    public Field field() {
        return field;
    }

    public Class<T> javaType() {
        return javaType;
    }

    /** {@code value} as the literal a comparison carries: a String, or a Long for every integer field. */
    Object literal(T value) {
        if (value == null) throw new IllegalArgumentException(field + ": a comparison value must not be null");
        return switch (value) {
            case Integer i -> i.longValue();
            case Instant t -> t.toEpochMilli();
            default -> value;
        };
    }

    @Override public String toString() {
        return field.name();
    }
}
