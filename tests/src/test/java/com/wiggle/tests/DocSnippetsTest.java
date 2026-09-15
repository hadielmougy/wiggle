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

    /** Regions the main repo's own docs draw on, beyond the site fixtures above. */
    private static final Map<String, List<String>> DOC_SOURCES = Map.of(
            "../example/src/main/java/com/wiggle/cookbook/Cookbook.java",
            List.of("linear-gate", "choose-fork", "foreach-queues", "poll-until-ready",
                    "approval-escalation", "parent", "batched-loop", "kitchen-sink"));

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

    @Test @DisplayName("a doc's marked block matches its region -- hand-edit it and this fails")
    void docsMatchTheirRegions() throws IOException {
        int checked = 0;
        for (Path doc : docs()) {
            List<String> lines = Files.readAllLines(doc);
            for (int i = 0; i < lines.size(); i++) {
                Matcher m = Pattern.compile("^<!-- snippet: ([\\w-]+)/([\\w,-]+) -->$")
                        .matcher(lines.get(i));
                if (!m.matches()) continue;
                assertTrue(i + 1 < lines.size() && lines.get(i + 1).startsWith("```"),
                        doc + ":" + (i + 2) + ": marker is not followed by a fence");

                List<String> shown = new ArrayList<>();
                for (int j = i + 2; j < lines.size() && !lines.get(j).startsWith("```"); j++) {
                    shown.add(lines.get(j));
                }
                String expected = String.join("\n", dedent(regionLines(sourcePath(m.group(1)),
                        m.group(2).split(","))));
                assertEquals(expected, String.join("\n", shown),
                        doc + ": the block after <!-- snippet: " + m.group(1) + "/" + m.group(2)
                        + " --> is not what that region says. Re-run scripts/docs-snippets.py; "
                        + "the source owns this code, the markdown only displays it.");
                checked++;
            }
        }
        assertTrue(checked >= 8, "expected the cookbook's wired blocks at least; checked " + checked);
    }

    @Test @DisplayName("no java block anywhere in docs/ names an API that was removed")
    void noDocBlockNamesARemovedApi() throws IOException {
        // The net under the blocks that are not wired to source yet. It is a denylist, not a
        // compiler -- but "a removed API is still quoted" is the exact failure that went unnoticed
        // on the site for weeks, and a denylist catches that much without a toolchain.
        List<String> gone = List.of(".compensate()", ".withRetry(", ".onQueue(", "Wiggle.graph",
                                    "@Handlers(", "thenActivity", "thenSubWorkflow");
        List<String> hits = new ArrayList<>();
        for (Path doc : docs()) {
            Matcher b = Pattern.compile("(?m)^```java\\n(.*?)^```", Pattern.DOTALL)
                    .matcher(Files.readString(doc));
            while (b.find()) {
                for (String g : gone) {
                    if (b.group(1).contains(g)) hits.add(doc.getFileName() + ": " + g);
                }
            }
        }
        assertEquals(List.of(), hits, "removed API quoted in docs/");
    }

    private static List<Path> docs() throws IOException {
        try (Stream<Path> s = Files.list(Path.of("../docs"))) {
            return s.filter(p -> p.toString().endsWith(".md")).sorted().toList();
        }
    }

    private static Path sourcePath(String source) {
        for (String p : DOC_SOURCES.keySet()) {
            if (p.endsWith("/" + capitalise(source) + ".java")) return Path.of(p);
        }
        return DIR.resolve(capitalise(source) + "Snippet.java");
    }

    private static String capitalise(String s) {
        StringBuilder b = new StringBuilder();
        for (String part : s.split("-")) b.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
        return b.toString();
    }

    /** The lines of one or more regions, joined by a blank line -- what the extractor emits. */
    private static List<String> regionLines(Path file, String[] names) throws IOException {
        List<String> all = new ArrayList<>();
        for (String name : names) {
            if (!all.isEmpty()) all.add("");
            List<String> lines = Files.readAllLines(file);
            boolean in = false, skipping = false;
            List<String> body = new ArrayList<>();
            for (String line : lines) {
                Matcher b = BEGIN.matcher(line);
                if (b.matches() && b.group(1).equals(name)) { in = true; continue; }
                Matcher e = END.matcher(line);
                if (e.matches() && e.group(1).equals(name)) break;
                if (!in) continue;
                if (SKIP.matcher(line).matches()) skipping = true;
                else if (RESUME.matcher(line).matches()) skipping = false;
                else if (!skipping) body.add(line);
            }
            assertTrue(in, "region '" + name + "' not found in " + file);
            all.addAll(dedent(body));
        }
        return all;
    }

    /** Mirrors dedent() in the extractor scripts. */
    private static List<String> dedent(List<String> lines) {
        List<String> ls = new ArrayList<>(lines);
        while (!ls.isEmpty() && ls.get(0).isBlank()) ls.remove(0);
        while (!ls.isEmpty() && ls.get(ls.size() - 1).isBlank()) ls.remove(ls.size() - 1);
        int pad = ls.stream().filter(l -> !l.isBlank())
                .mapToInt(l -> l.length() - l.stripLeading().length()).min().orElse(0);
        List<String> out = new ArrayList<>(ls.size());
        for (String l : ls) out.add(l.isBlank() ? "" : l.substring(pad));
        return out;
    }

    /** Kept so the set above cannot silently become empty. */
    @Test @DisplayName("the fixture set is not empty")
    void notEmpty() {
        assertFalse(EXPECTED.isEmpty());
        assertEquals(13, EXPECTED.size(), "every site-only page with Java should have a fixture");
        assertEquals(new LinkedHashSet<>(EXPECTED.keySet()).size(), EXPECTED.size());
    }
}
