package com.wiggle.core;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@code conformance/shard-ids-v1.json} against {@link ShardIds}: one dynamic test per case, so a
 * case added to the file runs without touching Java.
 */
class ShardIdsConformanceTest {

    private static final Path FIXTURE = Path.of("../conformance/shard-ids-v1.json");

    /** Bumped by hand with the file, so a case cannot be added here and quietly not run. */
    private static final int CASES = 28;

    private static Map<String, Object> fixture() throws IOException {
        return Json.parseObject(Files.readString(FIXTURE));
    }

    private static List<Map<String, Object>> cases(Map<String, Object> f, String kind) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : Json.asArray(f.get(kind))) out.add(Json.asObject(o));
        return out;
    }

    private static OptionalInt shard(Map<String, Object> c) {
        Object s = c.get("shard");
        return s == null ? OptionalInt.empty() : OptionalInt.of(((Number) s).intValue());
    }

    @TestFactory
    List<DynamicTest> conformance() throws IOException {
        Map<String, Object> f = fixture();
        List<DynamicTest> tests = new ArrayList<>();
        for (Map<String, Object> c : cases(f, "format")) {
            String prefix = (String) c.get("prefix");
            int shardNo = ((Number) c.get("shard")).intValue();
            String rest = (String) c.get("rest");
            tests.add(DynamicTest.dynamicTest("format: " + c.get("name"), () -> {
                if (c.containsKey("error")) {
                    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                            () -> ShardIds.format(prefix, shardNo, rest));
                    assertTrue(e.getMessage().contains((String) c.get("error")), e.getMessage());
                } else {
                    assertEquals(c.get("expect"), ShardIds.format(prefix, shardNo, rest));
                }
            }));
        }
        for (Map<String, Object> c : cases(f, "parse")) {
            tests.add(DynamicTest.dynamicTest("parse: " + c.get("name"),
                    () -> assertEquals(shard(c), ShardIds.shardOf((String) c.get("id")))));
        }
        for (Map<String, Object> c : cases(f, "inherit")) {
            String prefix = (String) c.get("prefix");
            tests.add(DynamicTest.dynamicTest("inherit: " + c.get("name"), () -> {
                String id = ShardIds.inherit(prefix, (String) c.get("owner"));
                assertEquals(shard(c), ShardIds.shardOf(id), id);
                assertEquals(c.get("bare"), id.startsWith(prefix + "_"), id);
            }));
        }
        return tests;
    }

    @Test
    void everyCaseRuns() throws IOException {
        Map<String, Object> f = fixture();
        int total = cases(f, "format").size() + cases(f, "parse").size() + cases(f, "inherit").size();
        assertEquals(CASES, total, "update CASES with the fixture so a new case is never silently skipped");
    }
}
