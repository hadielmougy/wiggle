package com.wiggle.server.engine;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A consumer's place in the event log: the last seq it has been served or has acknowledged on each
 * shard, written {@code shard:seq} pairs joined by commas, e.g. {@code 0:12,3:40}. Clients treat it
 * as opaque and hand it back on an ack.
 */
record FeedCursor(SortedMap<Integer, Long> positions) {

    FeedCursor {
        positions = Collections.unmodifiableSortedMap(new TreeMap<>(positions));
    }

    /** The position on {@code shard}, or 0 when the cursor has none there. */
    long at(int shard) {
        return positions.getOrDefault(shard, 0L);
    }

    /** This cursor moved to {@code seq} on {@code shard}. */
    FeedCursor with(int shard, long seq) {
        SortedMap<Integer, Long> next = new TreeMap<>(positions);
        next.put(shard, seq);
        return new FeedCursor(next);
    }

    String format() {
        StringBuilder out = new StringBuilder();
        for (Map.Entry<Integer, Long> e : positions.entrySet()) {
            if (!out.isEmpty()) out.append(',');
            out.append(e.getKey()).append(':').append(e.getValue());
        }
        return out.toString();
    }

    /** Reads a cursor a client handed back; anything malformed is a bad request. */
    static FeedCursor parse(String text) {
        SortedMap<Integer, Long> positions = new TreeMap<>();
        if (text == null || text.isBlank()) throw bad(text);
        for (String pair : text.split(",")) {
            int colon = pair.indexOf(':');
            if (colon <= 0) throw bad(text);
            try {
                int shard = Integer.parseInt(pair.substring(0, colon).trim());
                long seq = Long.parseLong(pair.substring(colon + 1).trim());
                if (shard < 0 || seq < 0 || positions.put(shard, seq) != null) throw bad(text);
            } catch (NumberFormatException e) {
                throw bad(text);
            }
        }
        return new FeedCursor(positions);
    }

    private static EngineException bad(String text) {
        return EngineException.badRequest("not an event cursor: '" + text + "'; ack with the cursor of the "
                + "last event you handled");
    }
}
