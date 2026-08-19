package dev.wiggle.core;

import java.util.Map;

/**
 * Converts a workflow context object to/from the JSON representation used on the wire
 * and in storage. The engine never sees the typed form -- only workers do.
 */
public interface ContextCodec<T> {

    Object encode(T value);

    T decode(Object json);

    /** Contexts that are plain JSON documents. */
    static ContextCodec<Map<String, Object>> jsonMap() {
        return new ContextCodec<>() {
            @Override public Object encode(Map<String, Object> value) { return value; }
            @Override public Map<String, Object> decode(Object json) { return Json.asObject(json); }
        };
    }

    /** Contexts modelled as Java records (the common case). */
    static <T> ContextCodec<T> records(Class<T> type) {
        return new ContextCodec<>() {
            @Override public Object encode(T value) { return RecordMapper.toJson(value); }
            @SuppressWarnings("unchecked")
            @Override public T decode(Object json) { return (T) RecordMapper.fromJson(json, type); }
        };
    }
}
