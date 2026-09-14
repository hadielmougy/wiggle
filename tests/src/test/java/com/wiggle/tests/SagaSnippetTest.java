package com.wiggle.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the marker comments in the saga fixtures, which {@code scripts/snippets.py} in the
 * wiggle-site repo extracts to rewrite the saga pattern page's Java.
 *
 * <p>The compiler already guarantees the code between the markers is real -- that is the point of
 * keeping it as source rather than as text in a markdown file. What the compiler cannot see is the
 * markers themselves: rename a region, drop an {@code end}, leave a {@code docs:skip} unresumed,
 * and the extraction yields nothing or the wrong thing while the build stays green. The page then
 * publishes an empty block, which is a worse failure than the stale one this machinery replaced,
 * because it looks deliberate.
 *
 * <p>The region names and file paths below are a contract with a script in another repository.
 * Changing one here means changing it there in the same breath, so they are pinned by name.
 */
class SagaSnippetTest {

    /** region -> the fixture that owns it; mirrors SOURCES in wiggle-site's scripts/snippets.py. */
    private static final Map<String, String> REGIONS = new LinkedHashMap<>();
    static {
        REGIONS.put("contract", "SagaSnippet.java");
        REGIONS.put("topology", "SagaSnippet.java");
        REGIONS.put("handlers", "BookingHandlers.java");
    }

    private static final Pattern BEGIN = Pattern.compile("^\\s*// docs:begin (\\S+)\\s*$");
    private static final Pattern END = Pattern.compile("^\\s*// docs:end (\\S+)\\s*$");
    private static final Pattern SKIP = Pattern.compile("^\\s*// docs:skip\\s*$");
    private static final Pattern RESUME = Pattern.compile("^\\s*// docs:resume\\s*$");

    /** The test runs from the :tests module; the fixtures live in :example. */
    private static Path fixture(String name) {
        Path p = Path.of("../example/src/main/java/com/wiggle/docs/" + name);
        assertTrue(Files.exists(p), "fixture not found at " + p.toAbsolutePath()
                + " -- if it moved, wiggle-site's scripts/snippets.py must move with it");
        return p;
    }

    @Test @DisplayName("every docs region opens, closes, and closes the one it opened")
    void markersAreWellFormed() throws IOException {
        for (String file : REGIONS.values().stream().distinct().toList()) {
            List<String> lines = Files.readAllLines(fixture(file));
            String open = null;
            boolean skipping = false;
            List<String> seen = new ArrayList<>();

            for (int i = 0; i < lines.size(); i++) {
                String where = file + ":" + (i + 1) + ": ";
                Matcher b = BEGIN.matcher(lines.get(i));
                if (b.matches()) {
                    assertTrue(open == null, where + "'" + b.group(1) + "' opens inside '" + open
                            + "'; the extractor does not nest");
                    assertFalse(seen.contains(b.group(1)), where + "'" + b.group(1) + "' opens twice");
                    open = b.group(1);
                    seen.add(open);
                    skipping = false;
                    continue;
                }
                Matcher e = END.matcher(lines.get(i));
                if (e.matches()) {
                    assertFalse(open == null, where + "'" + e.group(1) + "' closes nothing");
                    assertEquals(open, e.group(1),
                            where + "closes '" + e.group(1) + "' but '" + open + "' is open");
                    assertFalse(skipping, where + "'" + open + "' ends with docs:skip unresumed, "
                            + "so everything after the skip would silently vanish from the page");
                    open = null;
                    continue;
                }
                if (SKIP.matcher(lines.get(i)).matches()) {
                    assertTrue(open != null, where + "docs:skip outside a region");
                    assertFalse(skipping, where + "docs:skip while already skipping");
                    skipping = true;
                } else if (RESUME.matcher(lines.get(i)).matches()) {
                    assertTrue(skipping, where + "docs:resume without a docs:skip");
                    skipping = false;
                }
            }
            assertTrue(open == null, file + ": region '" + open + "' is never closed");

            List<String> expected = REGIONS.entrySet().stream()
                    .filter(e -> e.getValue().equals(file)).map(Map.Entry::getKey).toList();
            assertEquals(expected, seen, file + " must hold exactly these regions, in this order; "
                    + "wiggle-site's snippets.py names them, so renaming one breaks the page silently");
        }
    }

    @Test @DisplayName("each region shows the current API, and the published text shows none of the removed one")
    void regionsShowTheCurrentApi() throws IOException {
        assertTrue(region("contract").contains("CompensableActivity<Booking, Booking> reserveStock();"),
                "the contract region must declare the step as a zero-argument factory");
        assertTrue(region("topology").contains(".thenApplyCompensable(s::reserveStock)"),
                "the topology region must use thenApplyCompensable");

        String handlers = region("handlers");
        assertTrue(handlers.contains("@ForFlow(\"booking\")"),
                "the handlers region must show the annotation that binds the class to the workflow");
        assertTrue(handlers.contains("public Booking execute(Booking b) {"), "execute(A) -> B");
        assertTrue(handlers.contains("public void compensate(Compensation<Booking, Booking> c) {"),
                "compensate takes the two-parameter Compensation carrier");
        assertFalse(handlers.contains("static class"),
                "the page would publish 'static class', an artifact of how the fixture is stored");

        // What this page actually published for weeks after these stopped existing. Checked against
        // the regions only -- the fixtures' javadoc names them on purpose, to say what went wrong.
        String published = REGIONS.keySet().stream().map(SagaSnippetTest::regionQuietly)
                .reduce("", String::concat);
        for (String gone : List.of(".compensate()", "Activity<Booking>", "Compensable<Booking>",
                                   "Compensation<Booking>", "thenActivity")) {
            assertFalse(published.contains(gone),
                    "removed API '" + gone + "' is back in what the saga page publishes");
        }
    }

    /** A region's text, minus anything the extractor is told to skip. */
    private static String region(String name) throws IOException {
        List<String> lines = Files.readAllLines(fixture(REGIONS.get(name)));
        StringBuilder out = new StringBuilder();
        boolean in = false, skipping = false;
        for (String line : lines) {
            Matcher b = BEGIN.matcher(line);
            if (b.matches() && b.group(1).equals(name)) { in = true; continue; }
            Matcher e = END.matcher(line);
            if (e.matches() && e.group(1).equals(name)) break;
            if (!in) continue;
            if (SKIP.matcher(line).matches()) skipping = true;
            else if (RESUME.matcher(line).matches()) skipping = false;
            else if (!skipping) out.append(line).append('\n');
        }
        assertTrue(in, "region '" + name + "' not found in " + REGIONS.get(name));
        return out.toString();
    }

    private static String regionQuietly(String name) {
        try {
            return region(name);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
