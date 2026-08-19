package dev.wiggle.core;

import java.math.BigDecimal;
import java.util.*;

/**
 * Minimal, dependency-free JSON reader/writer.
 *
 * Java representation:
 *   object -> LinkedHashMap<String,Object>
 *   array  -> ArrayList<Object>
 *   string -> String
 *   number -> Long (integral) or Double
 *   bool   -> Boolean
 *   null   -> null
 */
public final class Json {
    private Json() {}

    // ---------------------------------------------------------------- writing

    public static String write(Object v) {
        StringBuilder sb = new StringBuilder(256);
        write(v, sb);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    private static void write(Object v, StringBuilder sb) {
        switch (v) {
            case null -> sb.append("null");
            case String s -> string(s, sb);
            case Boolean b -> sb.append(b.booleanValue());
            case Double d -> {
                if (d.isNaN() || d.isInfinite()) throw new IllegalArgumentException("non-finite number: " + d);
                sb.append(BigDecimal.valueOf(d).stripTrailingZeros().toPlainString());
            }
            case Float f -> write(f.doubleValue(), sb);
            case BigDecimal bd -> sb.append(bd.toPlainString());
            case Number n -> sb.append(n.toString());
            case Map<?, ?> m -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    string(String.valueOf(e.getKey()), sb);
                    sb.append(':');
                    write(e.getValue(), sb);
                }
                sb.append('}');
            }
            case Iterable<?> it -> {
                sb.append('[');
                boolean first = true;
                for (Object o : it) {
                    if (!first) sb.append(',');
                    first = false;
                    write(o, sb);
                }
                sb.append(']');
            }
            case Object[] arr -> write(Arrays.asList(arr), sb);
            default -> throw new IllegalArgumentException("cannot serialise " + v.getClass());
        }
    }

    private static void string(String s, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }

    // ---------------------------------------------------------------- parsing

    public static Object parse(String text) {
        Parser p = new Parser(text);
        p.ws();
        Object v = p.value();
        p.ws();
        if (p.i < p.s.length()) throw p.err("trailing content");
        return v;
    }

    private static final class Parser {
        final String s;
        int i;

        Parser(String s) { this.s = s; }

        RuntimeException err(String msg) {
            return new IllegalArgumentException("JSON error at " + i + ": " + msg);
        }

        void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }

        char peek() {
            if (i >= s.length()) throw err("unexpected end of input");
            return s.charAt(i);
        }

        Object value() {
            char c = peek();
            return switch (c) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                default -> number();
            };
        }

        Object literal(String lit, Object v) {
            if (!s.startsWith(lit, i)) throw err("expected " + lit);
            i += lit.length();
            return v;
        }

        Map<String, Object> object() {
            Map<String, Object> m = new LinkedHashMap<>();
            i++; // {
            ws();
            if (peek() == '}') { i++; return m; }
            while (true) {
                ws();
                String k = string();
                ws();
                if (peek() != ':') throw err("expected ':'");
                i++;
                ws();
                m.put(k, value());
                ws();
                char c = peek();
                if (c == ',') { i++; continue; }
                if (c == '}') { i++; return m; }
                throw err("expected ',' or '}'");
            }
        }

        List<Object> array() {
            List<Object> l = new ArrayList<>();
            i++; // [
            ws();
            if (peek() == ']') { i++; return l; }
            while (true) {
                ws();
                l.add(value());
                ws();
                char c = peek();
                if (c == ',') { i++; continue; }
                if (c == ']') { i++; return l; }
                throw err("expected ',' or ']'");
            }
        }

        String string() {
            if (peek() != '"') throw err("expected string");
            i++;
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = peek();
                i++;
                if (c == '"') return sb.toString();
                if (c != '\\') { sb.append(c); continue; }
                char e = peek();
                i++;
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> throw err("bad escape \\" + e);
                }
            }
        }

        Object number() {
            int start = i;
            if (peek() == '-') i++;
            boolean fp = false;
            while (i < s.length()) {
                char c = s.charAt(i);
                if (c >= '0' && c <= '9') { i++; continue; }
                if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') { fp = true; i++; continue; }
                break;
            }
            String raw = s.substring(start, i);
            if (raw.isEmpty()) throw err("expected number");
            return fp ? (Object) Double.valueOf(raw) : (Object) Long.valueOf(raw);
        }
    }

    // ---------------------------------------------------------------- helpers

    @SuppressWarnings("unchecked")
    public static Map<String, Object> asObject(Object o) {
        if (o == null) return new LinkedHashMap<>();
        if (o instanceof Map<?, ?> m) return (Map<String, Object>) m;
        throw new IllegalArgumentException("expected JSON object, got " + o.getClass().getSimpleName());
    }

    @SuppressWarnings("unchecked")
    public static List<Object> asArray(Object o) {
        if (o == null) return List.of();
        if (o instanceof List<?> l) return (List<Object>) l;
        throw new IllegalArgumentException("expected JSON array");
    }

    public static Map<String, Object> parseObject(String text) {
        return asObject(parse(text));
    }

    public static String str(Map<String, Object> m, String k, String def) {
        Object v = m.get(k);
        return v == null ? def : String.valueOf(v);
    }

    public static String reqStr(Map<String, Object> m, String k) {
        Object v = m.get(k);
        if (v == null) throw new IllegalArgumentException("missing field '" + k + "'");
        return String.valueOf(v);
    }

    public static long num(Map<String, Object> m, String k, long def) {
        Object v = m.get(k);
        return v instanceof Number n ? n.longValue() : def;
    }

    public static double dbl(Map<String, Object> m, String k, double def) {
        Object v = m.get(k);
        return v instanceof Number n ? n.doubleValue() : def;
    }

    public static boolean bool(Map<String, Object> m, String k, boolean def) {
        Object v = m.get(k);
        return v instanceof Boolean b ? b : def;
    }

    /**
     * Top-level keys of {@code after} whose values differ from {@code before}, with keys
     * that disappeared mapped to null.
     *
     * This is what lets parallel branches share one context document: a branch writes
     * back only what it actually changed, so two branches touching different fields
     * merge cleanly instead of the slower one clobbering the faster one with its own
     * stale copy of the whole object.
     */
    public static Object shallowDiff(Object before, Object after) {
        if (!(before instanceof Map) || !(after instanceof Map)) return after;
        Map<String, Object> b = asObject(before);
        Map<String, Object> a = asObject(after);
        Map<String, Object> delta = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : a.entrySet()) {
            if (!Objects.equals(e.getValue(), b.get(e.getKey()))) delta.put(e.getKey(), e.getValue());
        }
        for (String k : b.keySet()) {
            if (!a.containsKey(k)) delta.put(k, null);
        }
        return delta;
    }

    /** Deterministic serialisation: object keys sorted. Used for content-hash versioning. */
    public static String canonical(Object v) {
        return write(sort(v));
    }

    private static Object sort(Object v) {
        if (v instanceof Map<?, ?> m) {
            Map<String, Object> out = new TreeMap<>();
            m.forEach((k, val) -> out.put(String.valueOf(k), sort(val)));
            return out;
        }
        if (v instanceof List<?> l) {
            List<Object> out = new ArrayList<>(l.size());
            for (Object o : l) out.add(sort(o));
            return out;
        }
        return v;
    }
}
