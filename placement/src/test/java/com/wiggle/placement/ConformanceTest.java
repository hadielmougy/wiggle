package com.wiggle.placement;

import com.wiggle.core.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@code conformance/placement-v1.json} against this implementation. Java, Go and Python each
 * carry their own id codec and resolver; the file is the shared answer and this is one of three
 * runners.
 *
 * <p>Data-driven on purpose: a case added to the JSON runs here without anyone editing this file.
 */
class ConformanceTest {

    /** Repo-relative, so the same path works from the module and from a vendoring client. */
    private static final Path FIXTURES = Path.of("../conformance/placement-v1.json");

    @TestFactory
    @DisplayName("conformance/placement-v1.json")
    List<DynamicTest> conformance() throws IOException {
        assertTrue(Files.exists(FIXTURES), "fixtures not found at " + FIXTURES.toAbsolutePath()
                + " -- they are vendored by the Go and Python clients, so the path is a contract");
        Map<String, Object> doc = Json.asObject(Json.parse(Files.readString(FIXTURES)));

        Map<String, Ring.Policy> policies = policies(asObject(doc.get("policies")));
        List<DynamicTest> tests = new ArrayList<>();

        for (Map<String, Object> c : cases(doc, "format")) {
            tests.add(DynamicTest.dynamicTest("format: " + c.get("name"), () -> runFormat(c)));
        }
        for (Map<String, Object> c : cases(doc, "parse")) {
            tests.add(DynamicTest.dynamicTest("parse: " + c.get("name"), () -> runParse(c)));
        }
        for (Map<String, Object> c : cases(doc, "resolve")) {
            tests.add(DynamicTest.dynamicTest("resolve: " + c.get("name"), () -> runResolve(c, policies)));
        }
        for (Map<String, Object> c : cases(doc, "mintable")) {
            tests.add(DynamicTest.dynamicTest("mintable: " + c.get("name"), () -> runMintable(c, policies)));
        }

        assertEquals(31, tests.size(), "every case in the file must run; found " + tests.size());
        return tests;
    }


    private static void runFormat(Map<String, Object> c) {
        String ns = (String) c.get("namespace");
        String cell = (String) c.get("cell");
        long epoch = num(c.get("epoch"));
        long shard = num(c.get("shard"));
        String ulid = (String) c.get("ulid");

        if (c.get("error") != null) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> IdCodec.format(ns, cell, epoch, shard, ulid));
            assertTrue(e.getMessage().contains((String) c.get("error")),
                    "message should mention '" + c.get("error") + "': " + e.getMessage());
            return;
        }
        assertEquals(c.get("expect"), IdCodec.format(ns, cell, epoch, shard, ulid));
    }

    private static void runParse(Map<String, Object> c) {
        String id = (String) c.get("id");
        Optional<IdCodec.Placement> parsed = IdCodec.parse(id);

        if (Boolean.TRUE.equals(c.get("legacy"))) {
            assertTrue(parsed.isEmpty(), "expected a legacy id: " + id);
            assertTrue(IdCodec.isLegacy(id));
            return;
        }
        IdCodec.Placement p = parsed.orElseThrow(() -> new AssertionError("did not parse: " + id));
        assertEquals(c.get("namespace"), p.namespace(), id);
        assertEquals(c.get("cell"), p.cellId(), id);
        assertEquals(num(c.get("epoch")), p.epoch(), id);
        assertEquals(num(c.get("shard")), p.shard(), id);
        assertEquals(c.get("ulid"), p.ulid(), id);
    }

    private static void runResolve(Map<String, Object> c, Map<String, Ring.Policy> policies) {
        Ring.Policy policy = policies.get((String) c.get("policy"));
        IdCodec.Placement id = IdCodec.parse((String) c.get("id"))
                .orElseThrow(() -> new AssertionError("fixture id does not parse: " + c.get("id")));
        Set<String> live = Set.copyOf(strings(c.get("live")));

        Optional<String> got = Placements.resolve(id, policy, live::contains);
        assertEquals(Optional.ofNullable((String) c.get("expect")), got, String.valueOf(c.get("why")));
    }

    private static void runMintable(Map<String, Object> c, Map<String, Ring.Policy> policies) {
        Ring.Policy policy = policies.get((String) c.get("policy"));
        Map<String, Object> expect = asObject(c.get("expect"));

        Placements.Mintable m = Placements.mintable(policy, (String) c.get("cell"));
        assertEquals(num(expect.get("epoch")), m.epoch(), "epoch");
        assertEquals(ints(expect.get("shards")), m.shards(), "shards");
        assertEquals(String.valueOf(expect.get("reason")), m.reason().name(), "reason");
        assertEquals("OK".equals(expect.get("reason")), m.allowed(), "allowed");
        if (!m.allowed()) {
            assertTrue(m.shards().isEmpty(),
                    "a cell that may not mint must be offered no shards, or a caller that checks the "
                    + "list instead of the reason would mint anyway: " + m.shards());
        }
    }


    private static Map<String, Ring.Policy> policies(Map<String, Object> raw) {
        Map<String, Ring.Policy> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : raw.entrySet()) {
            if (e.getValue() == null) { out.put(e.getKey(), null); continue; }   // "unplaced"
            Map<String, Object> p = asObject(e.getValue());
            Map<Long, Ring.Epoch> epochs = new LinkedHashMap<>();
            for (Map.Entry<String, Object> ep : asObject(p.get("epochs")).entrySet()) {
                Map<String, Object> er = asObject(ep.getValue());
                List<Ring.Slot> ring = new ArrayList<>();
                for (Object o : Json.asArray(er.get("ring"))) {
                    Map<String, Object> slot = asObject(o);
                    ring.add(new Ring.Slot((int) num(slot.get("shard")),
                            (String) slot.get("cellId"), (String) slot.get("region")));
                }
                epochs.put(Long.parseLong(ep.getKey()),
                        new Ring.Epoch(ring, Ring.Status.valueOf((String) er.get("status"))));
            }
            out.put(e.getKey(), new Ring.Policy((String) p.get("namespace"),
                    num(p.get("currentEpoch")), epochs));
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> cases(Map<String, Object> doc, String key) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Object o : Json.asArray(doc.get(key))) out.add(asObject(o));
        return out;
    }

    private static Map<String, Object> asObject(Object o) { return Json.asObject(o); }

    private static long num(Object o) { return ((Number) o).longValue(); }

    private static List<String> strings(Object o) {
        List<String> out = new ArrayList<>();
        for (Object x : Json.asArray(o)) out.add((String) x);
        return out;
    }

    private static List<Integer> ints(Object o) {
        List<Integer> out = new ArrayList<>();
        for (Object x : Json.asArray(o)) out.add((int) ((Number) x).longValue());
        return out;
    }
}
