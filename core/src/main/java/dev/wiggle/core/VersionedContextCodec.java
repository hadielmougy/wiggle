package dev.wiggle.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.UnaryOperator;

/**
 * A {@link ContextCodec} that wraps a record context in a versioned envelope so its schema can
 * evolve without corrupting in-flight instances. The stored JSON looks like:
 *
 * <pre>{@code
 * { "_schema": "order", "_v": 3, "_meta": {...}, "data": { ...the record... } }
 * }</pre>
 *
 * <p><b>Upcast to current.</b> On {@code decode}, data written at an older {@code _v} is migrated
 * forward through the registered {@link Builder#upcast upcast} steps until it matches the current
 * schema, then mapped to {@code type}. Activities therefore only ever see the current record shape.
 * On {@code encode} the data is re-stamped at the current version, so an instance is migrated
 * forward the first time any step writes to it.
 *
 * <p><b>Version visible to handlers.</b> {@code decode} publishes the version the context was
 * <em>persisted</em> at via {@link ContextVersion}, so a handler can branch on it (e.g. to treat an
 * instance created under an older schema specially). Note this reflects the stored version at read
 * time: a migrated instance reads at its old version on the first step that touches it, then -- once
 * that step re-stores the context at the current version -- reads as current thereafter. For a
 * durable origin marker that survives every step, stamp it into {@code data} from an upcast (it then
 * persists as an ordinary field).
 *
 * <p><b>Legacy contexts.</b> A bare (pre-envelope) context -- no {@code _v}/{@code data} -- is read
 * as version 1 with the whole object treated as {@code data}, so instances that predate the codec
 * upgrade transparently.
 *
 * <p><b>Overlay keys.</b> Non-reserved top-level keys (e.g. {@code item}/{@code itemIndex} injected
 * by {@code forkEach}) are folded into {@code data} before mapping; folded nulls are ignored so that
 * residual keys left behind by a legacy upgrade never clobber a real field.
 */
public final class VersionedContextCodec<T> implements ContextCodec<T> {

    private static final String SCHEMA = "_schema";
    private static final String VERSION = "_v";
    private static final String META = "_meta";
    private static final String DATA = "data";
    private static final Set<String> RESERVED = Set.of(SCHEMA, VERSION, META, DATA);

    private final Class<T> type;
    private final String schema;
    private final int currentVersion;
    private final Map<Integer, UnaryOperator<Map<String, Object>>> upcasts;
    private final Map<String, Object> meta;

    private VersionedContextCodec(Builder<T> b) {
        this.type = b.type;
        this.schema = b.schema != null ? b.schema : b.type.getSimpleName();
        this.currentVersion = b.currentVersion;
        this.upcasts = Map.copyOf(b.upcasts);
        this.meta = b.meta == null ? null : Map.copyOf(b.meta);
    }

    @Override
    public Object encode(T value) {
        Map<String, Object> env = new LinkedHashMap<>();
        env.put(SCHEMA, schema);
        env.put(VERSION, currentVersion);
        if (meta != null && !meta.isEmpty()) env.put(META, meta);
        env.put(DATA, RecordMapper.toJson(value));
        return env;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T decode(Object json) {
        Map<String, Object> map = Json.asObject(json);
        int storedV;
        Map<String, Object> data;

        if (map.containsKey(VERSION) || map.containsKey(DATA)) {
            storedV = intOr(map.get(VERSION), 1);
            Object d = map.get(DATA);
            data = d == null ? new LinkedHashMap<>() : new LinkedHashMap<>(Json.asObject(d));
            // Fold overlay keys (e.g. forkEach item/itemIndex) into data; skip nulls so that
            // residual top-level keys left behind by a legacy upgrade don't clobber real fields.
            for (Map.Entry<String, Object> e : map.entrySet()) {
                if (!RESERVED.contains(e.getKey()) && e.getValue() != null) data.put(e.getKey(), e.getValue());
            }
        } else {
            // Bare, pre-envelope context: the whole object is the data, written at version 1.
            storedV = 1;
            data = new LinkedHashMap<>(map);
        }

        for (int v = storedV; v < currentVersion; v++) {
            UnaryOperator<Map<String, Object>> up = upcasts.get(v);
            if (up == null) {
                throw new IllegalStateException("no upcast from schema version " + v + " to " + (v + 1)
                        + " for context '" + schema + "'");
            }
            data = up.apply(data);
        }

        ContextVersion.set(storedV);
        return (T) RecordMapper.fromJson(data, type);
    }

    private static int intOr(Object o, int fallback) {
        return o instanceof Number n ? n.intValue() : fallback;
    }

    public static <T> Builder<T> builder(Class<T> type, int currentVersion) {
        return new Builder<>(type, currentVersion);
    }

    /** Fluent builder. Register one {@code upcast(v, ...)} for every version {@code 1..currentVersion-1}. */
    public static final class Builder<T> {
        private final Class<T> type;
        private final int currentVersion;
        private final Map<Integer, UnaryOperator<Map<String, Object>>> upcasts = new TreeMap<>();
        private String schema;
        private Map<String, Object> meta;

        private Builder(Class<T> type, int currentVersion) {
            if (currentVersion < 1) throw new IllegalArgumentException("currentVersion must be >= 1");
            this.type = type;
            this.currentVersion = currentVersion;
        }

        /** Schema name recorded in the envelope; defaults to the type's simple name. */
        public Builder<T> schema(String name) { this.schema = name; return this; }

        /** Static metadata carried in every envelope under {@code _meta}. */
        public Builder<T> meta(Map<String, Object> meta) { this.meta = meta; return this; }

        /** Migrates a raw context map written at {@code fromVersion} to the shape of {@code fromVersion + 1}. */
        public Builder<T> upcast(int fromVersion, UnaryOperator<Map<String, Object>> migrate) {
            if (fromVersion < 1 || fromVersion >= currentVersion) {
                throw new IllegalArgumentException("upcast fromVersion must be in 1.." + (currentVersion - 1));
            }
            if (upcasts.putIfAbsent(fromVersion, migrate) != null) {
                throw new IllegalArgumentException("duplicate upcast from version " + fromVersion);
            }
            return this;
        }

        public VersionedContextCodec<T> build() {
            for (int v = 1; v < currentVersion; v++) {
                if (!upcasts.containsKey(v)) {
                    throw new IllegalStateException("missing upcast from schema version " + v + " to " + (v + 1));
                }
            }
            return new VersionedContextCodec<>(this);
        }
    }
}
