package com.wiggle.server.api;

import com.wiggle.core.criteria.Criteria;
import com.wiggle.core.criteria.Field;
import com.wiggle.core.criteria.Op;
import com.wiggle.core.criteria.Predicate;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.wire.Msg;
import com.wiggle.server.engine.EngineException;

import java.util.ArrayList;
import java.util.List;

/** Wire criteria decoded and validated; anything malformed is a 400 (INVALID_ARGUMENT). */
final class CriteriaWire {

    private CriteriaWire() { }

    /** One poll's criteria; empty when it carries none. */
    static List<QueueCriteria> decode(List<Msg> all) {
        if (all.isEmpty()) return List.of();
        try {
            List<QueueCriteria> out = new ArrayList<>(all.size());
            for (Msg c : all) {
                out.add(new QueueCriteria(c.str("queue"), c.has("where") ? predicate(c.msg("where"), 1) : null));
            }
            return QueueCriteria.forPoll(out);
        } catch (IllegalArgumentException e) {
            throw EngineException.badRequest("invalid criteria: " + e.getMessage());
        }
    }

    /** A predicate holds exactly one of {@code cmp}, {@code allOf}, {@code anyOf} and {@code negated}. */
    private static Predicate predicate(Msg p, int depth) {
        if (depth > Criteria.MAX_DEPTH) throw new IllegalArgumentException("criteria nest deeper than " + Criteria.MAX_DEPTH);
        return switch (oneOf(p, "a predicate", "cmp", "allOf", "anyOf", "negated")) {
            case "cmp" -> comparison(p.msg("cmp"));
            case "allOf" -> new Predicate.And(terms(p.msg("allOf").msgs("terms"), depth));
            case "anyOf" -> new Predicate.Or(terms(p.msg("anyOf").msgs("terms"), depth));
            default -> new Predicate.Not(predicate(p.msg("negated"), depth + 1));
        };
    }

    private static List<Predicate> terms(List<Msg> terms, int depth) {
        if (terms.size() > Criteria.MAX_NODES) throw new IllegalArgumentException("criteria have more than " + Criteria.MAX_NODES + " nodes");
        List<Predicate> out = new ArrayList<>(terms.size());
        for (Msg t : terms) out.add(predicate(t, depth + 1));
        return out;
    }

    private static Predicate comparison(Msg c) {
        Field field = named(Field.class, c.str("field"), "field");
        Op op = named(Op.class, c.str("op"), "op");
        List<Msg> literals = c.msgs("args");
        List<Object> args = new ArrayList<>(literals.size());
        for (Msg l : literals) args.add(literal(field, l));
        String attribute = c.str("attribute").isEmpty() ? null : c.str("attribute");
        return new Predicate.Comparison(field, attribute, op, args);
    }

    /** A literal holds exactly one of {@code text}, {@code integer}, {@code decimal} and {@code flag}. */
    static Object literal(Object field, Msg l) {
        return switch (oneOf(l, field + ": an argument", "text", "integer", "decimal", "flag")) {
            case "text" -> l.str("text");
            case "integer" -> l.lng("integer");
            case "decimal" -> decimal(field, l.str("decimal"));
            default -> l.bool("flag");
        };
    }

    /** The one of {@code names} that {@code m} sets; refused when it sets none or several. */
    static String oneOf(Msg m, String what, String... names) {
        String found = null;
        for (String n : names) {
            if (!m.has(n)) continue;
            if (found != null) throw new IllegalArgumentException(what + " sets both '" + found + "' and '" + n + "'");
            found = n;
        }
        if (found == null) throw new IllegalArgumentException(what + " is empty");
        return found;
    }

    private static java.math.BigDecimal decimal(Object field, String text) {
        if (text.length() > 64) throw new IllegalArgumentException(field + ": a decimal of more than 64 characters");
        try {
            return new java.math.BigDecimal(text).stripTrailingZeros();
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(field + ": '" + text + "' is not a decimal");
        }
    }

    private static <E extends Enum<E>> E named(Class<E> type, String name, String what) {
        try {
            return Enum.valueOf(type, name);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown " + what + " '" + name + "'");
        }
    }
}
