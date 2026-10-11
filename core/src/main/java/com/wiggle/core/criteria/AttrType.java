package com.wiggle.core.criteria;

import java.math.BigDecimal;
import java.math.BigInteger;

/** The type of a declared attribute, and how a context value is read as one. */
public enum AttrType {
    STRING, LONG, DECIMAL, BOOLEAN;

    /** The largest scale a DECIMAL keeps; a value with more fraction digits is absent. */
    public static final int MAX_SCALE = 10;
    /** The most integer digits a DECIMAL keeps. */
    public static final int MAX_INTEGER_DIGITS = 28;

    private static final BigDecimal LONG_MIN = BigDecimal.valueOf(Long.MIN_VALUE);
    private static final BigDecimal LONG_MAX = BigDecimal.valueOf(Long.MAX_VALUE);

    /**
     * {@code json} as this type: a String, Long, BigDecimal or Boolean, or null when it does not fit.
     * Numbers are read from their decimal text, so every JSON reader's number type agrees.
     */
    public Object read(Object json) {
        return switch (this) {
            case STRING -> json instanceof String s && s.length() <= Criteria.MAX_TEXT ? s : null;
            case BOOLEAN -> json instanceof Boolean b ? b : null;
            case LONG -> {
                BigDecimal d = decimal(json);
                yield d != null && d.scale() <= 0 && d.compareTo(LONG_MIN) >= 0 && d.compareTo(LONG_MAX) <= 0
                        ? d.longValueExact() : null;
            }
            case DECIMAL -> {
                BigDecimal d = decimal(json);
                yield d != null && fits(d) ? d : null;
            }
        };
    }

    /** Whether {@code d} is within the DECIMAL range every store keeps exactly. */
    public static boolean fits(BigDecimal d) {
        BigDecimal s = d.stripTrailingZeros();
        return s.scale() <= MAX_SCALE && s.precision() - s.scale() <= MAX_INTEGER_DIGITS;
    }

    private static BigDecimal decimal(Object json) {
        if (!(json instanceof Number n)) return null;
        if (n instanceof Double x && (x.isNaN() || x.isInfinite())) return null;
        if (n instanceof Float x && (x.isNaN() || x.isInfinite())) return null;
        BigDecimal d = n instanceof BigDecimal b ? b : n instanceof BigInteger i ? new BigDecimal(i) : new BigDecimal(n.toString());
        return d.stripTrailingZeros();
    }
}
