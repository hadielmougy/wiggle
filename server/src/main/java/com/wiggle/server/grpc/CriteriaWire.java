package com.wiggle.server.grpc;

import com.wiggle.core.criteria.Criteria;
import com.wiggle.core.criteria.Field;
import com.wiggle.core.criteria.Op;
import com.wiggle.core.criteria.Predicate;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.proto.CriteriaComparison;
import com.wiggle.proto.CriteriaLiteral;
import com.wiggle.proto.CriteriaPredicate;
import com.wiggle.server.engine.EngineException;

import java.util.ArrayList;
import java.util.List;

/** Wire criteria decoded and validated; anything malformed is a 400 (INVALID_ARGUMENT). */
final class CriteriaWire {

    private CriteriaWire() { }

    static QueueCriteria decode(com.wiggle.proto.QueueCriteria c) {
        try {
            return new QueueCriteria(c.getQueuesList(), c.hasWhere() ? predicate(c.getWhere(), 1) : null);
        } catch (IllegalArgumentException e) {
            throw EngineException.badRequest("invalid criteria: " + e.getMessage());
        }
    }

    private static Predicate predicate(CriteriaPredicate p, int depth) {
        if (depth > Criteria.MAX_DEPTH) throw new IllegalArgumentException("criteria nest deeper than " + Criteria.MAX_DEPTH);
        return switch (p.getPCase()) {
            case CMP -> comparison(p.getCmp());
            case ALL_OF -> new Predicate.And(terms(p.getAllOf().getTermsList(), depth));
            case ANY_OF -> new Predicate.Or(terms(p.getAnyOf().getTermsList(), depth));
            case NEGATED -> new Predicate.Not(predicate(p.getNegated(), depth + 1));
            case P_NOT_SET -> throw new IllegalArgumentException("an empty predicate");
        };
    }

    private static List<Predicate> terms(List<CriteriaPredicate> terms, int depth) {
        if (terms.size() > Criteria.MAX_NODES) throw new IllegalArgumentException("criteria have more than " + Criteria.MAX_NODES + " nodes");
        List<Predicate> out = new ArrayList<>(terms.size());
        for (CriteriaPredicate t : terms) out.add(predicate(t, depth + 1));
        return out;
    }

    private static Predicate comparison(CriteriaComparison c) {
        Field field = named(Field.class, c.getField(), "field");
        Op op = named(Op.class, c.getOp(), "op");
        List<Object> args = new ArrayList<>(c.getArgsCount());
        for (CriteriaLiteral l : c.getArgsList()) {
            args.add(switch (l.getVCase()) {
                case TEXT -> l.getText();
                case INTEGER -> l.getInteger();
                case DECIMAL -> decimal(field, l.getDecimal());
                case FLAG -> l.getFlag();
                case V_NOT_SET -> throw new IllegalArgumentException(field + ": an empty argument");
            });
        }
        String attribute = c.getAttribute().isEmpty() ? null : c.getAttribute();
        return new Predicate.Comparison(field, attribute, op, args);
    }

    private static java.math.BigDecimal decimal(Field field, String text) {
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
