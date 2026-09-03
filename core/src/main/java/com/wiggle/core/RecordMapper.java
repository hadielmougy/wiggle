package com.wiggle.core;

import java.lang.reflect.*;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Small reflective binder between Java records and the {@link Json} object model.
 *
 * Supported component types: record, String, boolean/int/long/double (and boxes),
 * BigDecimal, Instant, Duration, enum, List&lt;X&gt;, Set&lt;X&gt;, Map&lt;String,X&gt;, Optional&lt;X&gt;.
 */
public final class RecordMapper {
    private RecordMapper() {}

    public static Object toJson(Object value) {
        switch (value) {
            case null -> { return null; }
            case String s -> { return s; }
            case Boolean b -> { return b; }
            case Integer i -> { return i.longValue(); }
            case Long l -> { return l; }
            case Short s -> { return s.longValue(); }
            case Byte b -> { return b.longValue(); }
            case Double d -> { return d; }
            case Float f -> { return f.doubleValue(); }
            case BigDecimal bd -> { return bd; }
            case Instant t -> { return t.toString(); }
            case Duration d -> { return d.toString(); }
            case Enum<?> e -> { return e.name(); }
            case Optional<?> o -> { return o.map(RecordMapper::toJson).orElse(null); }
            case Map<?, ?> m -> {
                Map<String, Object> out = new LinkedHashMap<>();
                m.forEach((k, v) -> out.put(String.valueOf(k), toJson(v)));
                return out;
            }
            case Collection<?> c -> {
                List<Object> out = new ArrayList<>(c.size());
                for (Object o : c) out.add(toJson(o));
                return out;
            }
            default -> { /* fall through */ }
        }
        Class<?> type = value.getClass();
        if (!type.isRecord()) {
            throw new IllegalArgumentException("unsupported context type: " + type.getName()
                    + " (use a record or a Map)");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (RecordComponent rc : type.getRecordComponents()) {
            try {
                Method accessor = rc.getAccessor();
                // Local and non-public records have inaccessible accessors by default.
                accessor.setAccessible(true);
                Object v = accessor.invoke(value);
                out.put(rc.getName(), toJson(v));
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("cannot read component " + rc.getName(), e);
            }
        }
        return out;
    }

    public static Object fromJson(Object json, Type target) {
        Class<?> raw = raw(target);

        if (raw == Optional.class) {
            Type arg = typeArg(target, 0);
            return json == null ? Optional.empty() : Optional.ofNullable(fromJson(json, arg));
        }
        if (json == null) return defaultValue(raw);

        if (raw == Object.class) return json;
        if (raw == String.class) return String.valueOf(json);
        if (raw == boolean.class || raw == Boolean.class) return asBool(json);
        if (raw == int.class || raw == Integer.class) return (int) asLong(json);
        if (raw == long.class || raw == Long.class) return asLong(json);
        if (raw == short.class || raw == Short.class) return (short) asLong(json);
        if (raw == byte.class || raw == Byte.class) return (byte) asLong(json);
        if (raw == double.class || raw == Double.class) return asDouble(json);
        if (raw == float.class || raw == Float.class) return (float) asDouble(json);
        if (raw == BigDecimal.class) return new BigDecimal(String.valueOf(json));
        if (raw == Instant.class) return Instant.parse(String.valueOf(json));
        if (raw == Duration.class) return Duration.parse(String.valueOf(json));
        if (raw.isEnum()) return enumOf(raw, String.valueOf(json));

        if (List.class.isAssignableFrom(raw) || Set.class.isAssignableFrom(raw)) {
            Type arg = typeArg(target, 0);
            Collection<Object> out = Set.class.isAssignableFrom(raw) ? new LinkedHashSet<>() : new ArrayList<>();
            for (Object o : Json.asArray(json)) out.add(fromJson(o, arg));
            return List.class.isAssignableFrom(raw) ? List.copyOf((List<Object>) out) : Set.copyOf(out);
        }
        if (Map.class.isAssignableFrom(raw)) {
            Type arg = typeArg(target, 1);
            Map<String, Object> out = new LinkedHashMap<>();
            Json.asObject(json).forEach((k, v) -> out.put(k, fromJson(v, arg)));
            return out;
        }
        if (raw.isRecord()) return record(raw, Json.asObject(json));

        throw new IllegalArgumentException("unsupported target type: " + target);
    }

    private static Object record(Class<?> type, Map<String, Object> json) {
        RecordComponent[] comps = type.getRecordComponents();
        Class<?>[] paramTypes = new Class<?>[comps.length];
        Object[] args = new Object[comps.length];
        for (int i = 0; i < comps.length; i++) {
            paramTypes[i] = comps[i].getType();
            args[i] = fromJson(json.get(comps[i].getName()), comps[i].getGenericType());
        }
        try {
            Constructor<?> ctor = type.getDeclaredConstructor(paramTypes);
            ctor.setAccessible(true);
            return ctor.newInstance(args);
        } catch (ReflectiveOperationException e) {
            Throwable cause = e instanceof InvocationTargetException ite ? ite.getCause() : e;
            throw new IllegalStateException("cannot instantiate " + type.getName() + ": " + cause, cause);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object enumOf(Class<?> raw, String name) {
        return Enum.valueOf((Class<Enum>) raw.asSubclass(Enum.class), name);
    }

    private static Object defaultValue(Class<?> raw) {
        if (!raw.isPrimitive()) return null;
        if (raw == boolean.class) return Boolean.FALSE;
        if (raw == double.class) return 0d;
        if (raw == float.class) return 0f;
        if (raw == long.class) return 0L;
        if (raw == int.class) return 0;
        if (raw == short.class) return (short) 0;
        if (raw == byte.class) return (byte) 0;
        return null;
    }

    private static boolean asBool(Object o) {
        if (o instanceof Boolean b) return b;
        return Boolean.parseBoolean(String.valueOf(o));
    }

    private static long asLong(Object o) {
        if (o instanceof Number n) return n.longValue();
        return Long.parseLong(String.valueOf(o));
    }

    private static double asDouble(Object o) {
        if (o instanceof Number n) return n.doubleValue();
        return Double.parseDouble(String.valueOf(o));
    }

    private static Class<?> raw(Type t) {
        if (t instanceof Class<?> c) return c;
        if (t instanceof ParameterizedType p) return (Class<?>) p.getRawType();
        if (t instanceof GenericArrayType) return Object[].class;
        if (t instanceof WildcardType w) return raw(w.getUpperBounds()[0]);
        if (t instanceof TypeVariable<?> v) return raw(v.getBounds()[0]);
        throw new IllegalArgumentException("unsupported type " + t);
    }

    private static Type typeArg(Type t, int index) {
        if (t instanceof ParameterizedType p) {
            Type[] args = p.getActualTypeArguments();
            if (index < args.length) return args[index];
        }
        return Object.class;
    }
}
