package com.wiggle.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the marker comments in the {@code com.wiggle.docs} fixtures, which
 * {@code scripts/snippets.py} in the wiggle-site repo extracts to rewrite the Java on the pattern
 * pages.
 *
 * <p>Those pages are site-only -- they live in the other repo and are not among the docs
 * {@code sync-docs.sh} vendors -- so nothing used to compile their code, and it rotted. The saga
 * page published a removed API for weeks; the fan-out page named a handler {@code load} against a
 * contract that said {@code loadOrder}, a step that could never have bound. Keeping the code as
 * source fixes that, because the compiler reads it.
 *
 * <p>What the compiler cannot read is the markers. Rename a region, drop an {@code end}, leave a
 * {@code docs:skip} unresumed, and extraction silently yields nothing or too little while the build
 * stays green -- and the page publishes an empty block, which reads as deliberate and so is a worse
 * failure than the stale text this replaced. That is what this test is for.
 *
 * <p>The region names are a contract with a script in another repository, so the whole map is
 * pinned: a fixture that gains a region nobody asked for, or loses one a page needs, fails here
 * rather than on the published page.
 */
class DocSnippetsTest {

    /** Every region the site asks for. Mirrors SOURCES in wiggle-site's scripts/snippets.py. */
    private static final Map<String, List<String>> EXPECTED = new LinkedHashMap<>();
    static {
        EXPECTED.put("SagaSnippet.java", List.of("contract", "topology"));
        EXPECTED.put("BookingHandlers.java", List.of("handlers"));
        EXPECTED.put("ForkJoinSnippet.java", List.of("contract", "topology"));
        EXPECTED.put("OrderHandlers.java", List.of("handlers"));
        EXPECTED.put("FanOutSnippet.java", List.of("contract", "topology"));
        EXPECTED.put("PricingHandlers.java", List.of("handlers"));
        EXPECTED.put("ApprovalSnippet.java", List.of("contract", "topology", "signal"));
        EXPECTED.put("ExpenseHandlers.java", List.of("handlers"));
        EXPECTED.put("MicroservicesSnippet.java", List.of("contract", "topology", "worker"));
        EXPECTED.put("RetriesSnippet.java", List.of("retry-line", "gate-chain", "poll-loop"));
        EXPECTED.put("RetriesHandlers.java", List.of("gate-handler", "poll-handlers"));
        EXPECTED.put("ScheduledSnippet.java", List.of("contract", "topology"));
        EXPECTED.put("CellsSnippet.java", List.of("connect"));
    }

    private static final Path DIR = Path.of("../example/src/main/java/com/wiggle/docs");

    private static final Pattern BEGIN = Pattern.compile("^\\s*// docs:begin (\\S+)\\s*$");
    private static final Pattern END = Pattern.compile("^\\s*// docs:end (\\S+)\\s*$");
    private static final Pattern SKIP = Pattern.compile("^\\s*// docs:skip\\s*$");
    private static final Pattern RESUME = Pattern.compile("^\\s*// docs:resume\\s*$");

    private static Path fixture(String name) {
        Path p = DIR.resolve(name);
        assertTrue(Files.exists(p), "fixture not found at " + p.toAbsolutePath()
                + " -- if it moved, wiggle-site's scripts/snippets.py must move with it");
        return p;
    }

    @Test @DisplayName("every fixture's regions open, close, and close the one they opened")
    void markersAreWellFormed() throws IOException {
        for (String file : EXPECTED.keySet()) {
            List<String> lines = Files.readAllLines(fixture(file));
            String open = null;
            boolean skipping = false;
            List<String> seen = new ArrayList<>();

            for (int i = 0; i < lines.size(); i++) {
                String where = file + ":" + (i + 1) + ": ";
                Matcher b = BEGIN.matcher(lines.get(i));
                if (b.matches()) {
                    assertTrue(open == null,
                            where + "'" + b.group(1) + "' opens inside '" + open + "'; no nesting");
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
                    assertFalse(skipping, where + "'" + open + "' ends with docs:skip unresumed, so "
                            + "everything after the skip would silently vanish from the page");
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
            assertEquals(EXPECTED.get(file), seen, file + " must hold exactly these regions, in this "
                    + "order; wiggle-site's snippets.py names them, so a rename breaks a page silently");
        }
    }

    @Test @DisplayName("no fixture carries a region no page asks for, and none is missing")
    void theFixtureSetMatchesWhatTheSiteAsksFor() throws IOException {
        Set<String> withRegions = new TreeSet<>();
        try (Stream<Path> files = Files.list(DIR)) {
            for (Path p : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                if (Files.readString(p).contains("// docs:begin")) {
                    withRegions.add(p.getFileName().toString());
                }
            }
        }
        assertEquals(new TreeSet<>(EXPECTED.keySet()), withRegions,
                "a fixture gained or lost docs regions without the site being told");
    }

    @Test @DisplayName("what the pages publish shows the current API, not the shapes that were removed")
    void publishedTextUsesTheCurrentApi() throws IOException {
        assertTrue(region("SagaSnippet.java", "contract")
                        .contains("CompensableActivity<Booking, Booking> reserveStock();"),
                "the saga contract must declare the step as a zero-argument factory");
        assertTrue(region("SagaSnippet.java", "topology").contains(".thenApplyCompensable(s::reserveStock)"),
                "the saga topology must use thenApplyCompensable");
        assertTrue(region("BookingHandlers.java", "handlers")
                        .contains("public void compensate(Compensation<Booking, Booking> c) {"),
                "compensate takes the two-parameter Compensation carrier");
        assertTrue(region("FanOutSnippet.java", "topology").contains(".thenForEach(Order::items,"),
                "the fan-out page should show the accessor form, not a string key");

        String published = published();
        for (String gone : List.of(".compensate()", ".withRetry(", ".onQueue(", "Wiggle.graph",
                                   "@Handlers(", "thenActivity")) {
            assertFalse(published.contains(gone),
                    "removed API '" + gone + "' is back in what the pattern pages publish");
        }
        // the fan-out page named a handler the contract did not declare; it could never have bound
        assertFalse(region("PricingHandlers.java", "handlers").contains("public Order load("),
                "the fan-out handler must match the contract's loadOrder, not 'load'");
    }

    /** Every region's text, concatenated -- exactly what the pages end up showing. */
    private static String published() throws IOException {
        StringBuilder all = new StringBuilder();
        for (Map.Entry<String, List<String>> e : EXPECTED.entrySet()) {
            for (String r : e.getValue()) all.append(region(e.getKey(), r));
        }
        return all.toString();
    }

    /** A region's text, minus anything the extractor is told to skip. */
    private static String region(String file, String name) throws IOException {
        List<String> lines = Files.readAllLines(fixture(file));
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
        assertTrue(in, "region '" + name + "' not found in " + file);
        return out.toString();
    }

    /** Kept so the set above cannot silently become empty. */
    @Test @DisplayName("the fixture set is not empty")
    void notEmpty() {
        assertFalse(EXPECTED.isEmpty());
        assertEquals(13, EXPECTED.size(), "every site-only page with Java should have a fixture");
        assertEquals(new LinkedHashSet<>(EXPECTED.keySet()).size(), EXPECTED.size());
    }
}
