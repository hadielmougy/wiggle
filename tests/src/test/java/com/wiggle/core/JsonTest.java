package com.wiggle.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@link Json} against {@link LegacyJson}, the implementation it replaced: the same bytes out, the
 * same values in, and the same inputs refused. Fingerprints hash {@link Json#canonical}, so a
 * difference there would give every registered graph a new version.
 */
class JsonTest {

    private static final List<Object> SCALARS = List.of(
            "", "plain", "quote \" backslash \\ slash /", "tab\tnewline\nreturn\rback\bform\f",
            "\u0000\u0001\u001f\u007f", "é ü 中文 \uD83D\uDE00", "\u2028\u2029",
            0L, -0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 42, (short) 7, (byte) -3,
            0.0, -0.0, 1.0, -1.5, 0.1, 1e20, 1e-20, 123456789.123456789, Double.MIN_VALUE, Double.MAX_VALUE,
            1.5f, new BigDecimal("1.2300"), new BigDecimal("1E+3"), BigInteger.TEN.pow(30),
            true, false);

    @Test @DisplayName("write and canonical are byte-identical to the implementation they replaced")
    void writeMatches() {
        Random r = new Random(7);
        for (int i = 0; i < 2_000; i++) {
            Object v = value(r, 4);
            assertEquals(LegacyJson.write(v), Json.write(v), () -> "write of " + v);
            assertEquals(LegacyJson.canonical(v), Json.canonical(v), () -> "canonical of " + v);
        }
        for (char c = 0; c < 0x80; c++) {
            String s = "a" + c + "b";
            assertEquals(LegacyJson.write(s), Json.write(s), "character " + (int) c);
        }
        Map<Object, Object> keys = new LinkedHashMap<>();
        keys.put(1, "int key");
        keys.put("\n", null);
        assertEquals(LegacyJson.write(keys), Json.write(keys));
        Object[] array = {1L, "two", null};
        assertEquals(LegacyJson.write(array), Json.write(array));
    }

    @Test @DisplayName("parse reads what was written, and what others write, to the same values")
    void parseMatches() {
        Random r = new Random(11);
        for (int i = 0; i < 2_000; i++) {
            sameParse(LegacyJson.write(value(r, 4)));
        }
        for (String text : List.of(
                " { \"a\" : [ 1 , 2.5 , -0 , 1e5 , 1E-2 ] , \"b\" : null } ", "\"\\u00e9\\/\\n\\\"\"",
                "{\"a\":1,\"a\":2}", "\"raw\ttab\"", "007", "[]", "{}", "true", "-9223372036854775808",
                "1.0", "[[[[[]]]]]", "\"\\uD83D\\uDE00\"")) {
            assertEquals(LegacyJson.parse(text), Json.parse(text), text);
        }
    }

    /** The same value, or both refuse: a double of 2^63 or more is written as an integer neither reads. */
    private static void sameParse(String text) {
        Object expected;
        try {
            expected = LegacyJson.parse(text);
        } catch (IllegalArgumentException e) {
            assertThrows(IllegalArgumentException.class, () -> Json.parse(text), text);
            return;
        }
        assertEquals(expected, Json.parse(text), text);
    }

    @Test @DisplayName("what the old reader refused is refused, as an IllegalArgumentException")
    void refusals() {
        for (String text : List.of("", "   ", "{", "[1,]", "tru", "1 2", "{\"a\" 1}", "\"\\x\"", "{\"a\":}",
                "9223372036854775808", "[1 2]", "{1:2}", "\"unterminated")) {
            assertThrows(IllegalArgumentException.class, () -> LegacyJson.parse(text), "legacy: " + text);
            assertThrows(IllegalArgumentException.class, () -> Json.parse(text), text);
        }
        assertThrows(IllegalArgumentException.class, () -> Json.write(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> Json.write(new Object()));
    }

    private static Object value(Random r, int depth) {
        int kind = depth == 0 ? 0 : r.nextInt(4);
        return switch (kind) {
            case 1 -> {
                Map<String, Object> m = new LinkedHashMap<>();
                for (int i = r.nextInt(5); i > 0; i--) m.put(string(r), value(r, depth - 1));
                yield m;
            }
            case 2 -> {
                List<Object> l = new ArrayList<>();
                for (int i = r.nextInt(5); i > 0; i--) l.add(value(r, depth - 1));
                yield l;
            }
            case 3 -> r.nextBoolean() ? null : r.nextDouble() * Math.pow(10, r.nextInt(40) - 20);
            default -> r.nextInt(3) == 0 ? string(r) : SCALARS.get(r.nextInt(SCALARS.size()));
        };
    }

    private static String string(Random r) {
        StringBuilder sb = new StringBuilder();
        for (int i = r.nextInt(8); i > 0; i--) {
            sb.append(switch (r.nextInt(4)) {
                case 0 -> (char) r.nextInt(0x20);
                case 1 -> (char) (0x20 + r.nextInt(0x60));
                case 2 -> (char) (0xA0 + r.nextInt(0x700));
                default -> "\uD83D\uDE00";
            });
        }
        return sb.toString();
    }
}
