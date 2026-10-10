package com.wiggle.relay;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.RecordMapper;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * A six-stage linear relay. Each stage runs on its own queue, named after the stage, so each is
 * served by its own worker process. The context travels as a raw JSON map: a stage sees exactly what
 * arrived on the wire, extra or retyped fields included.
 */
public final class Relay {

    public static final String NAME = "relay";
    public static final int VERSION = 1;

    public static final List<String> STAGES = List.of("intake", "enrich", "price", "reserve", "dispatch", "settle");

    private static final List<UnaryOperator<Shipment>> TRANSFORMS = List.of(
            Shipment::intake, Shipment::enrich, Shipment::price,
            Shipment::reserve, Shipment::dispatch, Shipment::settle);

    private Relay() {}

    public interface Steps {
        Map<String, Object> intake(Map<String, Object> in);
        Map<String, Object> enrich(Map<String, Object> in);
        Map<String, Object> price(Map<String, Object> in);
        Map<String, Object> reserve(Map<String, Object> in);
        Map<String, Object> dispatch(Map<String, Object> in);
        Map<String, Object> settle(Map<String, Object> in);
    }

    public static FlowSpec flowSpec() {
        return FlowSpec.define(NAME, VERSION, Map.class, Steps.class, (f, s) -> f
                .thenApply(s::intake, "intake")
                .thenApply(s::enrich, "enrich")
                .thenApply(s::price, "price")
                .thenApply(s::reserve, "reserve")
                .thenApply(s::dispatch, "dispatch")
                .thenApply(s::settle, "settle"));
    }

    /** The shipment after the first {@code stages} stages have run on it. */
    public static Shipment after(String id, int stages) {
        Shipment s = Shipment.seed(id);
        for (int i = 0; i < stages; i++) s = TRANSFORMS.get(i).apply(s);
        return s;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> json(Shipment s) {
        return (Map<String, Object>) RecordMapper.toJson(s);
    }

    /**
     * Every leaf of {@code expected} and {@code actual} compared one by one, as {@code path: expected
     * != actual} lines. Empty when they match. An absent key equals null; numbers compare by value,
     * so 3 and 3L match but 3 and 3.5 do not. Keys only in {@code actual} are reported too.
     */
    public static List<String> diff(Object expected, Object actual, Counter fields) {
        List<String> out = new ArrayList<>();
        diff(expected, actual, "$", out, fields);
        return out;
    }

    /** Counts compared leaves. */
    public static final class Counter {
        public long leaves;
    }

    private static void diff(Object e, Object a, String path, List<String> out, Counter fields) {
        if (e instanceof Map<?, ?> em) {
            if (!(a instanceof Map<?, ?> am)) {
                out.add(path + ": expected object, got " + describe(a));
                return;
            }
            Set<Object> keys = new LinkedHashSet<>(em.keySet());
            keys.addAll(am.keySet());
            for (Object k : keys) diff(em.get(k), am.get(k), path + "." + k, out, fields);
        } else if (e instanceof List<?> el) {
            if (!(a instanceof List<?> al)) {
                out.add(path + ": expected array, got " + describe(a));
                return;
            }
            if (el.size() != al.size()) out.add(path + ": expected " + el.size() + " elements, got " + al.size());
            for (int i = 0; i < Math.min(el.size(), al.size()); i++) diff(el.get(i), al.get(i), path + "[" + i + "]", out, fields);
        } else {
            fields.leaves++;
            if (!leafEquals(e, a)) out.add(path + ": expected " + describe(e) + ", got " + describe(a));
        }
    }

    private static boolean leafEquals(Object e, Object a) {
        if (e instanceof Number en && a instanceof Number an) {
            boolean integral = !(en instanceof Double || en instanceof Float) && !(an instanceof Double || an instanceof Float);
            return integral ? en.longValue() == an.longValue() : Double.compare(en.doubleValue(), an.doubleValue()) == 0;
        }
        return e == null ? a == null : e.equals(a);
    }

    private static String describe(Object v) {
        if (v == null) return "null";
        if (v instanceof String s) return "\"" + s.replace("\n", "\\n").replace("\t", "\\t") + "\" (string)";
        return v + " (" + v.getClass().getSimpleName() + ")";
    }
}
