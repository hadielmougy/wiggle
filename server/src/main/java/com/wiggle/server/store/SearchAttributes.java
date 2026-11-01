package com.wiggle.server.store;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/** How a store without a query language for them tests a document's search attributes. */
public final class SearchAttributes {

    /** Whether {@code attributes} satisfy every filter. */
    public static boolean matches(Map<String, Object> attributes, List<Rows.AttrFilter> filters) {
        for (Rows.AttrFilter f : filters) {
            if (!matches(attributes.get(f.name()), f)) return false;
        }
        return true;
    }

    private static boolean matches(Object held, Rows.AttrFilter f) {
        if (held == null) return false;
        if (f.value() instanceof String || f.value() instanceof Boolean) return f.value().equals(held);
        if (!(held instanceof Number n)) return false;
        int c = number(n).compareTo(number((Number) f.value()));
        return switch (f.op()) {
            case EQUAL -> c == 0;
            case LESS_THAN -> c < 0;
            case LESS_THAN_OR_EQUAL -> c <= 0;
            case GREATER_THAN -> c > 0;
            case GREATER_THAN_OR_EQUAL -> c >= 0;
            default -> throw new IllegalStateException("unexpected operator " + f.op());
        };
    }

    private static BigDecimal number(Number n) {
        return n instanceof BigDecimal d ? d : BigDecimal.valueOf(n.longValue());
    }

    private SearchAttributes() { }
}
