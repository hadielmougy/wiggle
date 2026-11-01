package com.wiggle.core;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.io.CharacterEscapes;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.util.JsonRecyclerPools;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * JSON reader/writer over Jackson's streaming API.
 *
 * Java representation:
 *   object -> LinkedHashMap<String,Object>
 *   array  -> ArrayList<Object>
 *   string -> String
 *   number -> Long (integral) or Double
 *   bool   -> Boolean
 *   null   -> null
 *
 * A double is written in plain notation without trailing zeros ({@code 1.0} as {@code 1}), and a
 * control character without a short escape as a unicode escape in lowercase hex: definition fingerprints
 * hash {@link #canonical}, so its bytes must not change.
 */
public final class Json {
    private Json() {}

    private static final JsonFactory FACTORY = new JsonFactoryBuilder()
            .enable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS)
            .enable(JsonReadFeature.ALLOW_LEADING_ZEROS_FOR_NUMBERS)
            .streamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build())
            .characterEscapes(new LowercaseEscapes())
            .recyclerPool(JsonRecyclerPools.sharedConcurrentDequePool())
            .build();

    public static String write(Object v) {
        StringWriter out = new StringWriter(256);
        try (JsonGenerator g = FACTORY.createGenerator(out)) {
            write(v, g);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toString();
    }

    private static void write(Object v, JsonGenerator g) throws IOException {
        switch (v) {
            case null -> g.writeNull();
            case String s -> g.writeString(s);
            case Boolean b -> g.writeBoolean(b);
            case Double d -> {
                if (d.isNaN() || d.isInfinite()) throw new IllegalArgumentException("non-finite number: " + d);
                g.writeNumber(BigDecimal.valueOf(d).stripTrailingZeros().toPlainString());
            }
            case Float f -> write(f.doubleValue(), g);
            case BigDecimal bd -> g.writeNumber(bd.toPlainString());
            case Long l -> g.writeNumber(l);
            case Integer i -> g.writeNumber(i);
            case Number n -> g.writeNumber(n.toString());
            case Map<?, ?> m -> {
                g.writeStartObject();
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    g.writeFieldName(String.valueOf(e.getKey()));
                    write(e.getValue(), g);
                }
                g.writeEndObject();
            }
            case Iterable<?> it -> {
                g.writeStartArray();
                for (Object o : it) write(o, g);
                g.writeEndArray();
            }
            case Object[] arr -> write(Arrays.asList(arr), g);
            default -> throw new IllegalArgumentException("cannot serialise " + v.getClass());
        }
    }

    public static Object parse(String text) {
        try (JsonParser p = FACTORY.createParser(text)) {
            JsonToken t = p.nextToken();
            if (t == null) throw new IllegalArgumentException("JSON error at 0: unexpected end of input");
            Object v = read(p, t);
            if (p.nextToken() != null) {
                throw new IllegalArgumentException("JSON error at " + p.currentTokenLocation().getCharOffset()
                        + ": trailing content");
            }
            return v;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON error at " + e.getLocation().getCharOffset() + ": "
                    + e.getOriginalMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Object read(JsonParser p, JsonToken t) throws IOException {
        return switch (t) {
            case START_OBJECT -> {
                Map<String, Object> m = new LinkedHashMap<>();
                String name;
                while ((name = p.nextFieldName()) != null) m.put(name, read(p, p.nextToken()));
                yield m;
            }
            case START_ARRAY -> {
                List<Object> l = new ArrayList<>();
                JsonToken next;
                while ((next = p.nextToken()) != JsonToken.END_ARRAY) l.add(read(p, next));
                yield l;
            }
            case VALUE_STRING -> p.getText();
            case VALUE_NUMBER_INT -> {
                if (p.getNumberType() == JsonParser.NumberType.BIG_INTEGER) {
                    BigInteger big = p.getBigIntegerValue();
                    throw new IllegalArgumentException("JSON error at " + p.currentTokenLocation().getCharOffset()
                            + ": " + big + " does not fit in a long");
                }
                yield p.getLongValue();
            }
            case VALUE_NUMBER_FLOAT -> p.getDoubleValue();
            case VALUE_TRUE -> Boolean.TRUE;
            case VALUE_FALSE -> Boolean.FALSE;
            case VALUE_NULL -> null;
            default -> throw new IllegalArgumentException("JSON error at " + p.currentTokenLocation().getCharOffset()
                    + ": unexpected " + t);
        };
    }

    /** Jackson's escapes, except a control character with no short form is escaped in lowercase hex. */
    private static final class LowercaseEscapes extends CharacterEscapes {
        private static final long serialVersionUID = 1L;
        private final int[] codes;
        private final SerializableString[] lowercase = new SerializableString[0x20];

        LowercaseEscapes() {
            codes = standardAsciiEscapesForJSON();
            for (int c = 0; c < 0x20; c++) {
                if (codes[c] == ESCAPE_STANDARD) {
                    codes[c] = ESCAPE_CUSTOM;
                    lowercase[c] = new SerializedString(String.format("\\u%04x", c));
                }
            }
        }

        @Override public int[] getEscapeCodesForAscii() { return codes; }

        @Override public SerializableString getEscapeSequence(int ch) {
            return ch < 0x20 ? lowercase[ch] : null;
        }
    }

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

    /** Deterministic serialisation: object keys sorted. Used for definition fingerprinting. */
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
