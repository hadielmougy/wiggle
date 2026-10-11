package com.wiggle.core.criteria;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A typed reference to a task property, the counterpart of a JPA metamodel attribute. The
 * constants live on {@link Task_}, and {@link Task_#attr} names a declared attribute.
 *
 * @param <T> the Java type a comparison on this attribute takes
 */
public final class Attribute<T> {

    private final Field field;
    private final String name;
    private final Class<T> javaType;

    Attribute(Field field, Class<T> javaType) {
        this(field, null, javaType);
    }

    Attribute(Field field, String name, Class<T> javaType) {
        this.field = field;
        this.name = name;
        this.javaType = javaType;
    }

    public Field field() {
        return field;
    }

    /** The declared attribute's name; null for every other field. */
    public String name() {
        return name;
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
            case BigDecimal d -> d.stripTrailingZeros();
            default -> value;
        };
    }

    @Override public String toString() {
        return name == null ? field.name() : "attr(" + name + ")";
    }
}
