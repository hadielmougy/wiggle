package com.wiggle.core.wire;

import com.wiggle.core.Json;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One control-plane message: a JSON object, read and built by field name. An absent field reads as
 * its type's zero value ({@code ""}, {@code 0}, {@code false}, an empty list), and a field holding a
 * value of the wrong type is an {@link IllegalArgumentException}. A null is never written.
 */
public final class Msg {

    private final Map<String, Object> fields;

    private Msg(Map<String, Object> fields) {
        this.fields = fields;
    }

    public static Msg create() {
        return new Msg(new LinkedHashMap<>());
    }

    /** The message {@code json} holds: an object, or null for an empty message. */
    public static Msg of(Object json) {
        if (json == null) return create();
        if (!(json instanceof Map<?, ?>)) throw new IllegalArgumentException("a message must be a JSON object");
        return new Msg(Json.asObject(json));
    }

    public static Msg parse(String text) {
        return of(text.isBlank() ? null : Json.parse(text));
    }

    /** Sets {@code name} unless {@code value} is null; a {@link Msg} or a list of them is stored as JSON. */
    public Msg put(String name, Object value) {
        if (value != null) fields.put(name, unwrap(value));
        return this;
    }

    /** Appends {@code value} to the list field {@code name}, creating it when absent. */
    @SuppressWarnings("unchecked")
    public Msg add(String name, Object value) {
        Object v = fields.get(name);
        if (v == null) fields.put(name, v = new ArrayList<>());
        ((List<Object>) v).add(unwrap(value));
        return this;
    }

    private static Object unwrap(Object value) {
        if (value instanceof Msg m) return m.fields;
        if (value instanceof Collection<?> c) {
            List<Object> out = new ArrayList<>(c.size());
            for (Object o : c) out.add(unwrap(o));
            return out;
        }
        return value;
    }

    public boolean has(String name) {
        return fields.get(name) != null;
    }

    public String str(String name) {
        String s = optStr(name);
        return s == null ? "" : s;
    }

    public String optStr(String name) {
        Object v = fields.get(name);
        if (v == null || v instanceof String) return (String) v;
        throw wrongType(name, "a string");
    }

    public long lng(String name) {
        Long l = optLng(name);
        return l == null ? 0 : l;
    }

    public Long optLng(String name) {
        Object v = fields.get(name);
        if (v == null) return null;
        if (v instanceof Long l) return l;
        if (v instanceof Number n && n.doubleValue() == Math.rint(n.doubleValue())) return n.longValue();
        throw wrongType(name, "a whole number");
    }

    public int integer(String name) {
        Integer i = optInt(name);
        return i == null ? 0 : i;
    }

    public Integer optInt(String name) {
        Long l = optLng(name);
        if (l == null) return null;
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) throw wrongType(name, "a 32-bit whole number");
        return l.intValue();
    }

    public double dbl(String name) {
        Object v = fields.get(name);
        if (v == null) return 0;
        if (v instanceof Number n) return n.doubleValue();
        throw wrongType(name, "a number");
    }

    public boolean bool(String name) {
        Boolean b = optBool(name);
        return b != null && b;
    }

    public Boolean optBool(String name) {
        Object v = fields.get(name);
        if (v == null || v instanceof Boolean) return (Boolean) v;
        throw wrongType(name, "true or false");
    }

    /** The field's JSON value as it is, or null. */
    public Object value(String name) {
        return fields.get(name);
    }

    /** The field as a JSON object, or null when absent. */
    public Map<String, Object> obj(String name) {
        Object v = fields.get(name);
        if (v == null) return null;
        if (v instanceof Map<?, ?>) return Json.asObject(v);
        throw wrongType(name, "an object");
    }

    /** The field as a nested message; an empty one when absent. */
    public Msg msg(String name) {
        Object v = fields.get(name);
        if (v == null) return create();
        if (v instanceof Map<?, ?>) return new Msg(Json.asObject(v));
        throw wrongType(name, "an object");
    }

    public List<Msg> msgs(String name) {
        List<Msg> out = new ArrayList<>();
        for (Object o : list(name)) {
            if (!(o instanceof Map<?, ?>)) throw wrongType(name, "a list of objects");
            out.add(new Msg(Json.asObject(o)));
        }
        return out;
    }

    public List<String> strs(String name) {
        List<String> out = new ArrayList<>();
        for (Object o : list(name)) {
            if (!(o instanceof String s)) throw wrongType(name, "a list of strings");
            out.add(s);
        }
        return out;
    }

    private List<Object> list(String name) {
        Object v = fields.get(name);
        if (v == null) return List.of();
        if (v instanceof List<?>) return Json.asArray(v);
        throw wrongType(name, "a list");
    }

    /** The message as a JSON object. */
    public Map<String, Object> json() {
        return fields;
    }

    @Override public String toString() {
        return Json.write(fields);
    }

    private static IllegalArgumentException wrongType(String name, String expected) {
        return new IllegalArgumentException("field '" + name + "' must be " + expected);
    }
}
