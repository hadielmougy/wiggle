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
 * A value a flow declares for queue criteria to test: {@code path} is a dotted path into the context
 * a step receives, read as {@code type}. The server extracts it each time the step becomes ready.
 */
public record DispatchAttribute(String name, AttrType type, String path) {

    public static final int MAX_PER_FLOW = 16;
    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");

    public DispatchAttribute {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("attribute name '" + name + "' must match " + NAME.pattern());
        }
        if (type == null) throw new IllegalArgumentException("attribute '" + name + "' needs a type");
        if (path == null || path.isBlank() || List.of(path.split("\\.", -1)).contains("")) {
            throw new IllegalArgumentException("attribute '" + name + "' needs a dotted path, got '" + path + "'");
        }
    }

    /** @throws IllegalArgumentException when a flow declares too many attributes, or one name twice */
    public static void validate(Collection<DispatchAttribute> attributes) {
        if (attributes.size() > MAX_PER_FLOW) {
            throw new IllegalArgumentException("a flow declares at most " + MAX_PER_FLOW + " attributes, got " + attributes.size());
        }
        Set<String> names = new HashSet<>();
        for (DispatchAttribute a : attributes) {
            if (!names.add(a.name())) throw new IllegalArgumentException("attribute '" + a.name() + "' is declared twice");
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
    public static Map<String, Object> extractAll(Collection<DispatchAttribute> attributes, Object context) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (DispatchAttribute a : attributes) {
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

    public static DispatchAttribute fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        return new DispatchAttribute(Json.reqStr(m, "name"), AttrType.valueOf(Json.reqStr(m, "type")),
                Json.reqStr(m, "path"));
    }
}
