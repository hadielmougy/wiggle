package com.wiggle.proto;

import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts between the dependency-free JSON model used by {@code com.wiggle.core.Json}
 * (object -&gt; Map, array -&gt; List, number -&gt; Long|Double, plus String/Boolean/null) and
 * protobuf's well-known {@link Value}/{@link Struct} types, so arbitrary workflow
 * contexts and node graphs can travel over gRPC without a bespoke message per shape.
 */
public final class ProtoJson {
    private ProtoJson() {}

    @SuppressWarnings("unchecked")
    public static Value toValue(Object json) {
        Value.Builder v = Value.newBuilder();
        switch (json) {
            case null -> v.setNullValue(NullValue.NULL_VALUE);
            case Boolean b -> v.setBoolValue(b);
            case Number n -> v.setNumberValue(n.doubleValue());
            case String s -> v.setStringValue(s);
            case Map<?, ?> m -> v.setStructValue(toStruct((Map<String, Object>) m));
            case Iterable<?> it -> {
                ListValue.Builder lv = ListValue.newBuilder();
                for (Object o : it) lv.addValues(toValue(o));
                v.setListValue(lv);
            }
            default -> throw new IllegalArgumentException("cannot convert to protobuf Value: " + json.getClass());
        }
        return v.build();
    }

    public static Struct toStruct(Map<String, Object> json) {
        Struct.Builder s = Struct.newBuilder();
        for (Map.Entry<String, Object> e : json.entrySet()) {
            s.putFields(e.getKey(), toValue(e.getValue()));
        }
        return s.build();
    }

    public static Object fromValue(Value v) {
        return switch (v.getKindCase()) {
            case NULL_VALUE, KIND_NOT_SET -> null;
            case BOOL_VALUE -> v.getBoolValue();
            case NUMBER_VALUE -> {
                double d = v.getNumberValue();
                yield d == Math.rint(d) && !Double.isInfinite(d)
                        ? (Object) (long) d
                        : (Object) d;
            }
            case STRING_VALUE -> v.getStringValue();
            case STRUCT_VALUE -> fromStruct(v.getStructValue());
            case LIST_VALUE -> {
                List<Object> out = new ArrayList<>(v.getListValue().getValuesCount());
                for (Value item : v.getListValue().getValuesList()) out.add(fromValue(item));
                yield out;
            }
        };
    }

    public static Map<String, Object> fromStruct(Struct s) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Value> e : s.getFieldsMap().entrySet()) {
            out.put(e.getKey(), fromValue(e.getValue()));
        }
        return out;
    }
}
