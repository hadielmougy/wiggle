package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.Json;
import com.wiggle.server.store.Rows.Token;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Records a step's input and output on its token, as JSON, so an operator can see what each step
 * ran with and returned. On by default; {@code wiggle.stepIo.record} / {@code WIGGLE_RECORD_STEP_IO}
 * = false turns it off. Each side is capped at {@code wiggle.stepIo.maxChars} /
 * {@code WIGGLE_STEP_IO_MAX_CHARS} characters (default 65536): a longer one is stored as
 * {@code {"$truncated": <length>, "head": "<its first 4096 characters>"}}.
 */
final class StepIo {

    static final boolean ENABLED = config("wiggle.stepIo.record", "WIGGLE_RECORD_STEP_IO", "true").equals("true");
    static final int MAX_CHARS = intConfig("wiggle.stepIo.maxChars", "WIGGLE_STEP_IO_MAX_CHARS", 65_536);
    static final int HEAD_CHARS = 4096;

    private StepIo() {}

    /** Sets {@code t}'s recorded input and output; a no-op when recording is off. */
    static void record(Token t, Doc input, Object output) {
        if (!ENABLED) return;
        t.stepInput = input == null ? null : cap(input.json(), MAX_CHARS);
        t.stepOutput = output == null ? null : cap(Json.write(output), MAX_CHARS);
    }

    static String cap(String json, int maxChars) {
        if (json.length() <= maxChars) return json;
        Map<String, Object> marker = new LinkedHashMap<>();
        marker.put("$truncated", json.length());
        marker.put("head", json.substring(0, Math.min(HEAD_CHARS, maxChars)));
        return Json.write(marker);
    }

    private static String config(String prop, String env, String def) {
        String v = System.getProperty(prop);
        if (v == null || v.isBlank()) v = System.getenv(env);
        return v == null || v.isBlank() ? def : v.trim().toLowerCase();
    }

    private static int intConfig(String prop, String env, int def) {
        try {
            return Integer.parseInt(config(prop, env, Integer.toString(def)));
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
