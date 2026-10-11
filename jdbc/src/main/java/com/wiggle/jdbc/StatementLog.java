package com.wiggle.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link StatementListener} that keeps what it is told, in order, as {@linkplain #summary
 * summaries}: {@code SELECT wf_token FOR UPDATE}, {@code INSERT wf_token}, {@code COMMIT}. Thread
 * safe; {@link #take} reads and clears it, to bracket one operation.
 */
public final class StatementLog implements StatementListener {

    public static final String COMMIT = "COMMIT";
    public static final String ROLLBACK = "ROLLBACK";

    private static final Pattern TABLE = Pattern.compile(
            "\\b(?:FROM|INTO|UPDATE|JOIN|USING)\\s+(wf_\\w+)", Pattern.CASE_INSENSITIVE);

    private final List<String> entries = new ArrayList<>();

    @Override public synchronized void executed(String sql) {
        entries.add(summary(sql));
    }

    @Override public synchronized void committed() {
        entries.add(COMMIT);
    }

    @Override public synchronized void rolledBack() {
        entries.add(ROLLBACK);
    }

    /** Everything recorded since the last {@link #take}, in order; the log is left empty. */
    public synchronized List<String> take() {
        List<String> out = List.copyOf(entries);
        entries.clear();
        return out;
    }

    /** How many times each summary occurs in {@code entries}. */
    public static Map<String, Long> counts(List<String> entries) {
        Map<String, Long> out = new TreeMap<>();
        for (String e : entries) out.merge(e, 1L, Long::sum);
        return out;
    }

    /**
     * {@code sql} as its verb and the first {@code wf_} table it names, with any row lock:
     * {@code SELECT wf_instance FOR UPDATE SKIP LOCKED}. A statement naming no {@code wf_} table is
     * its first two words.
     */
    public static String summary(String sql) {
        String flat = sql.replaceAll("\\s+", " ").trim();
        String upper = flat.toUpperCase(Locale.ROOT);
        String verb = upper.startsWith("WITH") ? "WITH" : upper.split(" ", 2)[0];
        Matcher m = TABLE.matcher(flat);
        StringBuilder out = new StringBuilder(verb);
        if (m.find()) {
            out.append(' ').append(m.group(1).toLowerCase(Locale.ROOT));
        } else {
            String[] words = flat.split(" ", 3);
            if (words.length > 1) out.append(' ').append(words[1]);
        }
        if (upper.contains(" FOR UPDATE")) out.append(" FOR UPDATE");
        if (upper.contains(" SKIP LOCKED")) out.append(" SKIP LOCKED");
        return out.toString();
    }
}
