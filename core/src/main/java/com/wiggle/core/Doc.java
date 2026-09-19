package com.wiggle.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A workflow's data, as a value the engine can carry without knowing what is in it. The context is
 * the caller's own JSON — the engine has no schema for it and wants none — so this exposes only the
 * handful of operations the engine genuinely performs on it: replace, merge, read one key, overlay.
 * Everything else about the document is the handler's business.
 *
 * <p>It exists so JSON stops being a step in the engine's logic. A {@code Doc} read from a store
 * holds the stored text and parses on first use, so a path that never looks inside — claiming a
 * batch of tokens, listing instances — pays nothing; a {@code Doc} built from a value holds the
 * value and serialises only when something stores it.
 *
 * <p>Immutable: every operation returns a new {@code Doc}, and two are equal when their values are,
 * however each was built.
 */
public final class Doc {

    /** An empty object -- what an instance started with no context carries. */
    public static final Doc EMPTY = new Doc(Map.of(), "{}");

    private volatile Object value;
    private volatile String json;

    private Doc(Object value, String json) {
        this.value = value;
        this.json = json;
    }

    /** Wraps an already-parsed value (a map, list, scalar, or null for an empty object). */
    public static Doc of(Object value) {
        if (value instanceof Doc d) return d;
        return value == null ? EMPTY : new Doc(value, null);
    }

    /** Wraps stored text, parsed on first read. Null or blank is {@link #EMPTY}. */
    public static Doc parse(String json) {
        return json == null || json.isBlank() ? EMPTY : new Doc(null, json);
    }

    /** The document as a JSON tree: maps, lists and scalars. */
    public Object raw() {
        Object v = value;
        if (v == null && json != null) value = v = Json.parse(json);
        return v;
    }

    /** The document as text -- what a store writes. */
    public String json() {
        String j = json;
        if (j == null) json = j = Json.write(value);
        return j;
    }

    public boolean isObject() {
        return raw() instanceof Map;
    }

    /** One top-level key, or null when absent or when this is not an object. */
    public Object get(String key) {
        return raw() instanceof Map<?, ?> m ? m.get(key) : null;
    }

    /**
     * Folds an external value in: a map merges key by key (a null value deletes its key), anything
     * else replaces the document wholesale. This is what a delivered signal and a finished
     * sub-workflow do; a step's own return replaces instead, and never merges.
     */
    public Doc merge(Object result) {
        if (!(result instanceof Map)) return of(result);
        Map<String, Object> out = isObject()
                ? new LinkedHashMap<>(Json.asObject(raw())) : new LinkedHashMap<>();
        ((Map<?, ?>) result).forEach((k, v) -> {
            String key = String.valueOf(k);
            if (v == null) out.remove(key);
            else out.put(key, v);
        });
        return of(out);
    }

    /** Drops top-level null values: a null means "this key is absent", never a stored JSON null. */
    public Doc withoutNulls() {
        if (!isObject()) return this;
        Map<String, Object> out = new LinkedHashMap<>();
        ((Map<?, ?>) raw()).forEach((k, v) -> { if (v != null) out.put(String.valueOf(k), v); });
        return of(out);
    }

    /** This document with {@code extra} laid over it. A non-object document yields {@code extra}. */
    public Doc plusAll(Map<String, Object> extra) {
        if (extra.isEmpty()) return this;
        if (!isObject()) return of(extra);
        Map<String, Object> out = new LinkedHashMap<>(Json.asObject(raw()));
        out.putAll(extra);
        return of(out);
    }

    @Override public boolean equals(Object o) {
        return o instanceof Doc d && Objects.equals(raw(), d.raw());
    }

    @Override public int hashCode() {
        return Objects.hashCode(raw());
    }

    @Override public String toString() {
        return json();
    }
}
