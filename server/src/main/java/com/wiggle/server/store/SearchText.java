package com.wiggle.server.store;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/** How a store without its own full-text engine splits text into words, and orders hits. */
public final class SearchText {

    /** Best score first, then the most recently changed instance, then by id so the order is total. */
    public static final Comparator<Rows.SearchHit> BEST_FIRST = Comparator
            .comparingDouble(Rows.SearchHit::score).reversed()
            .thenComparing(Comparator.comparingLong((Rows.SearchHit h) -> h.doc().updatedAt()).reversed())
            .thenComparing(h -> h.doc().instanceId());

    /** The lower-cased runs of letters and digits in {@code text}; none for null. */
    public static List<String> terms(String text) {
        List<String> out = new ArrayList<>();
        if (text == null) return out;
        for (String w : text.toLowerCase(Locale.ROOT).split("[^\\p{L}\\p{N}]+")) {
            if (!w.isEmpty()) out.add(w);
        }
        return out;
    }

    /**
     * The documents containing every word of {@code text} (all of them when it has none), scored by
     * how often those words occur, best first, at most {@code limit}.
     */
    public static List<Rows.SearchHit> match(Iterable<Rows.SearchDoc> docs, String text, int limit) {
        List<String> terms = terms(text);
        List<Rows.SearchHit> hits = new ArrayList<>();
        for (Rows.SearchDoc d : docs) {
            List<String> words = terms(d.text());
            double score = 0;
            boolean all = true;
            for (String t : terms) {
                long n = words.stream().filter(t::equals).count();
                if (n == 0) { all = false; break; }
                score += n;
            }
            if (all) hits.add(new Rows.SearchHit(d, score));
        }
        hits.sort(BEST_FIRST);
        return hits.size() > limit ? List.copyOf(hits.subList(0, limit)) : hits;
    }

    private SearchText() { }
}
