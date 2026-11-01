package com.wiggle.core.criteria;

import com.wiggle.core.Json;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A value a flow declares for search to filter on: {@code path} is a dotted path into the
 * instance's context, read as {@code type}. The search indexer extracts it each time it indexes
 * the instance.
 */
public record SearchAttribute(String name, AttrType type, String path) {

    public static final int MAX_PER_FLOW = 16;
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");

    public SearchAttribute {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("search attribute name '" + name + "' must match " + NAME.pattern());
        }
        if (type == null) throw new IllegalArgumentException("search attribute '" + name + "' needs a type");
        if (path == null || path.isBlank() || List.of(path.split("\\.", -1)).contains("")) {
            throw new IllegalArgumentException("search attribute '" + name + "' needs a dotted path, got '" + path + "'");
        }
    }

    /** @throws IllegalArgumentException when a flow declares too many search attributes, or one name twice */
    public static void validate(Collection<SearchAttribute> attributes) {
        if (attributes.size() > MAX_PER_FLOW) {
            throw new IllegalArgumentException("a flow declares at most " + MAX_PER_FLOW
                    + " search attributes, got " + attributes.size());
        }
        Set<String> names = new HashSet<>();
        for (SearchAttribute a : attributes) {
            if (!names.add(a.name())) throw new IllegalArgumentException("search attribute '" + a.name() + "' is declared twice");
        }
    }

    /** This attribute's value in {@code context} (a JSON tree), or null when it is absent. */
    public Object extract(Object context) {
        Object at = context;
        for (String key : path.split("\\.")) {
            if (!(at instanceof Map<?, ?> m)) return null;
            at = m.get(key);
        }
        return type.read(at);
    }

    /** Every attribute present in {@code context}, by name; empty when none is. */
    public static Map<String, Object> extractAll(Collection<SearchAttribute> attributes, Object context) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (SearchAttribute a : attributes) {
            Object v = a.extract(context);
            if (v != null) out.put(a.name(), v);
        }
        return out;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("type", type.name());
        m.put("path", path);
        return m;
    }

    public static SearchAttribute fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        return new SearchAttribute(Json.reqStr(m, "name"), AttrType.valueOf(Json.reqStr(m, "type")),
                Json.reqStr(m, "path"));
    }
}
