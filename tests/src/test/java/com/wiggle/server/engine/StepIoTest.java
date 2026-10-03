package com.wiggle.server.engine;

import com.wiggle.core.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class StepIoTest {

    @Test @DisplayName("a recording within the cap is stored as it is")
    void withinCapUnchanged() {
        String json = "{\"a\":1}";
        assertSame(json, StepIo.cap(json, 100));
    }

    @Test @DisplayName("a recording over the cap becomes a marker with its length and head, still valid JSON")
    void overCapBecomesMarker() {
        String json = "\"" + "x".repeat(10_000) + "\"";
        Map<String, Object> marker = Json.asObject(Json.parse(StepIo.cap(json, 5_000)));
        assertEquals((long) json.length(), marker.get("$truncated"));
        assertEquals(json.substring(0, StepIo.HEAD_CHARS), marker.get("head"));
    }

    @Test @DisplayName("a cap smaller than the head keeps only the cap's worth")
    void tinyCap() {
        Map<String, Object> marker = Json.asObject(Json.parse(StepIo.cap("[1,2,3,4,5]", 4)));
        assertEquals("[1,2", marker.get("head"));
    }
}
