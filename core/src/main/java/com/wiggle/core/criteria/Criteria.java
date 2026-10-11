package com.wiggle.core.criteria;

import com.wiggle.core.criteria.Predicate.And;
import com.wiggle.core.criteria.Predicate.Comparison;
import com.wiggle.core.criteria.Predicate.Not;
import com.wiggle.core.criteria.Predicate.Or;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validation and evaluation of criteria. Evaluation is two-valued: a comparison on an absent value
 * is false and {@code not} of it is true; only {@code isNull}/{@code isNotNull} test for absence. A
 * literal of the wrong type compares false. Every storage backend must agree with {@link #matches}.
 */
public final class Criteria {

    public static final int MAX_DEPTH = 8;
    public static final int MAX_NODES = 64;
    public static final int MAX_IN = 256;
    public static final int MAX_TEXT = 256;

    /** What a predicate is evaluated against: a field's value, or {@link #UNKNOWN}. */
    @FunctionalInterface
    public interface Facts {
        /** {@code attribute} names the declared attribute when {@code field} is ATTRIBUTE, else null. */
        Object value(Field field, String attribute);
    }

    private static final Pattern ATTRIBUTE_NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");

    /** A fact {@link #evaluate} cannot know, as returned by its facts function. */
    public static final Object UNKNOWN = new Object() {
        @Override public String toString() { return "UNKNOWN"; }
    };

    /** The result of evaluating over facts that may be {@link #UNKNOWN}. */
    public enum Truth {
        TRUE, FALSE, UNKNOWN;

        static Truth of(boolean b) { return b ? TRUE : FALSE; }
    }

    private Criteria() { }

    /** @throws IllegalArgumentException naming the first limit or typing rule the criteria break */
    public static void validate(List<String> queues, Predicate where) {
        if (queues == null || queues.isEmpty()) {
            throw new IllegalArgumentException("criteria need at least one queue");
        }
        for (String q : queues) {
            if (q == null || q.isBlank()) throw new IllegalArgumentException("a criteria queue must not be blank");
        }
        if (where == null) return;
        int[] nodes = {0};
        validate(where, 1, nodes);
    }

    private static void validate(Predicate p, int depth, int[] nodes) {
        if (depth > MAX_DEPTH) throw new IllegalArgumentException("criteria nest deeper than " + MAX_DEPTH);
        if (++nodes[0] > MAX_NODES) throw new IllegalArgumentException("criteria have more than " + MAX_NODES + " nodes");
        switch (p) {
            case Comparison c -> validate(c);
            case And a -> validateTerms("and", a.terms(), depth, nodes);
            case Or o -> validateTerms("or", o.terms(), depth, nodes);
            case Not n -> validate(n.term(), depth + 1, nodes);
        }
    }

    private static void validateTerms(String what, List<Predicate> terms, int depth, int[] nodes) {
        if (terms.isEmpty()) throw new IllegalArgumentException(what + "() needs at least one term");
        for (Predicate t : terms) validate(t, depth + 1, nodes);
    }

    private static void validate(Comparison c) {
        Field f = c.field();
        Op op = c.op();
        int n = c.args().size();
        if (n < op.minArgs() || n > op.maxArgs()) {
            throw new IllegalArgumentException(f + " " + op + " takes " + (op.minArgs() == op.maxArgs()
                    ? String.valueOf(op.minArgs()) : op.minArgs() + ".." + op.maxArgs()) + " argument(s), got " + n);
        }
        if (f == Field.STEP && op != Op.EQUAL && op != Op.NOT_EQUAL && op != Op.IN) {
            throw new IllegalArgumentException("STEP supports equal, notEqual and in only, not " + op);
        }
        if (op == Op.LIKE && f.type() == Field.Type.INTEGER) {
            throw new IllegalArgumentException("like needs a text field, not " + f);
        }
        if (f == Field.ATTRIBUTE) {
            validateAttribute(c);
            return;
        }
        for (Object arg : c.args()) {
            boolean fits = f.type() == Field.Type.TEXT ? arg instanceof String : arg instanceof Long;
            if (!fits) {
                throw new IllegalArgumentException(f + " takes " + (f.type() == Field.Type.TEXT ? "text" : "an integer")
                        + ", got " + (arg == null ? "null" : arg.getClass().getSimpleName()));
            }
            requireShortText(f.name(), arg);
        }
        if (op == Op.LIKE) likeRegex((String) c.args().getFirst());
    }

    private static void validateAttribute(Comparison c) {
        String what = "attribute '" + c.attribute() + "'";
        if (!ATTRIBUTE_NAME.matcher(c.attribute()).matches()) {
            throw new IllegalArgumentException(what + " is not a valid attribute name");
        }
        Class<?> kind = null;
        for (Object arg : c.args()) {
            Class<?> k = switch (arg) {
                case String s -> String.class;
                case Long n -> Number.class;
                case BigDecimal d -> {
                    if (!AttrType.fits(d)) {
                        throw new IllegalArgumentException(what + ": " + d + " has more than " + AttrType.MAX_SCALE
                                + " fraction or " + AttrType.MAX_INTEGER_DIGITS + " integer digits");
                    }
                    yield Number.class;
                }
                case Boolean b -> Boolean.class;
                case null, default -> throw new IllegalArgumentException(what + " takes a String, Long, BigDecimal "
                        + "or Boolean, got " + (arg == null ? "null" : arg.getClass().getSimpleName()));
            };
            if (kind != null && kind != k) throw new IllegalArgumentException(what + ": arguments of mixed types");
            kind = k;
            requireShortText(what, arg);
        }
        if (c.op() == Op.LIKE && kind != String.class) throw new IllegalArgumentException(what + ": like needs text");
        if (kind == Boolean.class && (c.op().ordering() || c.op() == Op.LIKE)) {
            throw new IllegalArgumentException(what + ": a boolean supports equal, notEqual and in only");
        }
        if (c.op() == Op.LIKE) likeRegex((String) c.args().getFirst());
    }

    private static void requireShortText(String what, Object arg) {
        if (arg instanceof String s && s.length() > MAX_TEXT) {
            throw new IllegalArgumentException(what + ": a text argument is longer than " + MAX_TEXT + " characters");
        }
    }

    /**
     * What is wrong with {@code where}'s attribute comparisons given the attributes the flows on its
     * queues declare: a name none declares, or a literal of a type no declaration of that name has.
     * Either makes the comparison false for every task (and {@code isNull} true). Empty when sound.
     */
    public static List<String> attributeProblems(Predicate where, Collection<DispatchAttribute> declared) {
        if (where == null) return List.of();
        Set<String> out = new LinkedHashSet<>();
        for (Comparison c : where.comparisons()) {
            if (c.field() != Field.ATTRIBUTE) continue;
            List<AttrType> types = declared.stream().filter(a -> a.name().equals(c.attribute()))
                    .map(DispatchAttribute::type).distinct().toList();
            if (types.isEmpty()) {
                out.add("attribute '" + c.attribute() + "' is declared by no flow on these queues, so no task carries it");
                continue;
            }
            if (c.args().isEmpty()) continue;
            Object literal = c.args().getFirst();
            boolean fits = types.stream().anyMatch(t -> switch (t) {
                case STRING -> literal instanceof String;
                case BOOLEAN -> literal instanceof Boolean;
                case LONG, DECIMAL -> literal instanceof Number;
            });
            if (!fits) {
                out.add("attribute '" + c.attribute() + "' is declared " + types + " but compared with a "
                        + literal.getClass().getSimpleName() + ", which never matches it");
            }
        }
        return List.copyOf(out);
    }

    /** {@code p} as an operator reads it: {@code region = 'eu' and not (correlation_id like 'vip-%')}. */
    public static String describe(Predicate p) {
        return switch (p) {
            case Comparison c -> describe(c);
            case And a -> join(a.terms(), " and ");
            case Or o -> join(o.terms(), " or ");
            case Not n -> "not (" + describe(n.term()) + ")";
        };
    }

    private static String join(List<Predicate> terms, String op) {
        StringBuilder out = new StringBuilder();
        for (Predicate t : terms) {
            if (!out.isEmpty()) out.append(op);
            boolean nested = t instanceof And || t instanceof Or;
            out.append(nested ? "(" : "").append(describe(t)).append(nested ? ")" : "");
        }
        return out.toString();
    }

    private static String describe(Comparison c) {
        String name = c.field() == Field.ATTRIBUTE ? c.attribute() : c.field().name().toLowerCase(java.util.Locale.ROOT);
        List<String> args = c.args().stream().map(Criteria::literal).toList();
        return name + switch (c.op()) {
            case EQUAL -> " = " + args.getFirst();
            case NOT_EQUAL -> " <> " + args.getFirst();
            case IN -> " in (" + String.join(", ", args) + ")";
            case LESS_THAN -> " < " + args.getFirst();
            case LESS_THAN_OR_EQUAL -> " <= " + args.getFirst();
            case GREATER_THAN -> " > " + args.getFirst();
            case GREATER_THAN_OR_EQUAL -> " >= " + args.getFirst();
            case BETWEEN -> " between " + args.get(0) + " and " + args.get(1);
            case LIKE -> " like " + args.getFirst();
            case IS_NULL -> " is null";
            case IS_NOT_NULL -> " is not null";
        };
    }

    private static String literal(Object v) {
        return v instanceof String s ? "'" + s.replace("'", "''") + "'"
                : v instanceof BigDecimal d ? d.toPlainString() : String.valueOf(v);
    }

    public static boolean matches(Predicate p, Facts task) {
        return evaluate(p, task) == Truth.TRUE;
    }

    /** Evaluates with three values: a comparison on an {@link #UNKNOWN} fact is UNKNOWN. */
    public static Truth evaluate(Predicate p, Facts facts) {
        return switch (p) {
            case Comparison c -> compare(c, facts.value(c.field(), c.attribute()));
            case And a -> {
                Truth out = Truth.TRUE;
                for (Predicate t : a.terms()) {
                    Truth r = evaluate(t, facts);
                    if (r == Truth.FALSE) yield Truth.FALSE;
                    if (r == Truth.UNKNOWN) out = Truth.UNKNOWN;
                }
                yield out;
            }
            case Or o -> {
                Truth out = Truth.FALSE;
                for (Predicate t : o.terms()) {
                    Truth r = evaluate(t, facts);
                    if (r == Truth.TRUE) yield Truth.TRUE;
                    if (r == Truth.UNKNOWN) out = Truth.UNKNOWN;
                }
                yield out;
            }
            case Not n -> switch (evaluate(n.term(), facts)) {
                case TRUE -> Truth.FALSE;
                case FALSE -> Truth.TRUE;
                case UNKNOWN -> Truth.UNKNOWN;
            };
        };
    }

    private static Truth compare(Comparison c, Object value) {
        if (value == UNKNOWN) return Truth.UNKNOWN;
        if (c.op() == Op.IS_NULL) return Truth.of(value == null);
        if (c.op() == Op.IS_NOT_NULL) return Truth.of(value != null);
        if (value == null) return Truth.FALSE;
        List<Object> args = c.args();
        return Truth.of(switch (c.op()) {
            case EQUAL -> order(value, args.getFirst()) instanceof Integer o && o == 0;
            case NOT_EQUAL -> order(value, args.getFirst()) instanceof Integer o && o != 0;
            case IN -> args.stream().anyMatch(a -> order(value, a) instanceof Integer o && o == 0);
            case LESS_THAN -> order(value, args.getFirst()) instanceof Integer o && o < 0;
            case LESS_THAN_OR_EQUAL -> order(value, args.getFirst()) instanceof Integer o && o <= 0;
            case GREATER_THAN -> order(value, args.getFirst()) instanceof Integer o && o > 0;
            case GREATER_THAN_OR_EQUAL -> order(value, args.getFirst()) instanceof Integer o && o >= 0;
            case BETWEEN -> order(value, args.get(0)) instanceof Integer lo && lo >= 0
                    && order(value, args.get(1)) instanceof Integer hi && hi <= 0;
            case LIKE -> value instanceof String s && likeRegex((String) args.getFirst()).matcher(s).matches();
            case IS_NULL, IS_NOT_NULL -> throw new IllegalStateException("handled above");
        });
    }

    /** The order of two literals, or null when their types differ. Numbers compare by value, text by
     *  code point. */
    private static Integer order(Object a, Object b) {
        if (a instanceof Long x && b instanceof Long y) return Long.compare(x, y);
        if (a instanceof Number x && b instanceof Number y) return decimal(x).compareTo(decimal(y));
        if (a instanceof String x && b instanceof String y) return codePointOrder(x, y);
        if (a instanceof Boolean x && b instanceof Boolean y) return Boolean.compare(x, y);
        return null;
    }

    private static BigDecimal decimal(Number n) {
        return n instanceof BigDecimal d ? d : BigDecimal.valueOf(n.longValue());
    }

    private static int codePointOrder(String a, String b) {
        int i = 0;
        int j = 0;
        while (i < a.length() && j < b.length()) {
            int ca = a.codePointAt(i);
            int cb = b.codePointAt(j);
            if (ca != cb) return Integer.compare(ca, cb);
            i += Character.charCount(ca);
            j += Character.charCount(cb);
        }
        return Boolean.compare(i < a.length(), j < b.length());
    }

    /** A LIKE pattern as an anchored regex. @throws IllegalArgumentException on a dangling escape */
    public static Pattern likeRegex(String like) {
        StringBuilder re = new StringBuilder();
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < like.length(); i++) {
            char ch = like.charAt(i);
            if (ch == '\\') {
                if (++i == like.length()) throw new IllegalArgumentException("like pattern ends in an escape: " + like);
                literal.append(like.charAt(i));
            } else if (ch == '%' || ch == '_') {
                if (!literal.isEmpty()) re.append(Pattern.quote(literal.toString()));
                literal.setLength(0);
                re.append(ch == '%' ? ".*" : ".");
            } else {
                literal.append(ch);
            }
        }
        if (!literal.isEmpty()) re.append(Pattern.quote(literal.toString()));
        return Pattern.compile(re.toString(), Pattern.DOTALL);
    }
}
