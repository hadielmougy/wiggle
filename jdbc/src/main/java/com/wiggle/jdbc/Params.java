package com.wiggle.jdbc;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.Types;

/**
 * Named statement parameters. A value that may be null is added with its SQL type, so the driver is
 * never asked for parameter metadata to bind it.
 */
final class Params extends MapSqlParameterSource {

    /** A value that is never null. */
    Params add(String name, Object value) {
        if (value == null) throw new IllegalArgumentException("parameter '" + name + "' is null; add it with its type");
        addValue(name, value);
        return this;
    }

    Params text(String name, String value) {
        addValue(name, value, Types.VARCHAR);
        return this;
    }

    Params number(String name, Long value) {
        addValue(name, value, Types.BIGINT);
        return this;
    }

    Params integer(String name, Integer value) {
        addValue(name, value, Types.INTEGER);
        return this;
    }
}
