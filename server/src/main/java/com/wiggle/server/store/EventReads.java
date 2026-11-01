package com.wiggle.server.store;

import java.util.List;
import java.util.Map;

/**
 * The event log, and its consumer cursors: the reads, which a read-only connection such as a read
 * replica can serve. Held on an instance shard; the cursors on the home shard.
 */
public interface EventReads {

    /** A consumer's acknowledged event position on each shard other than home, by shard; its home
     *  position is its {@link #eventCursor}. Held on the home shard. */
    Map<Integer, Long> eventPositions(String consumer);

    /** The lowest position any consumer holds on {@code shard}, a consumer with none holding 0, or null
     *  when no consumer exists. For a shard other than home; held on the home shard. */
    Long oldestEventPosition(int shard);

    /** Up to {@code max} events with seq greater than {@code afterSeq}, ascending. */
    default List<Rows.Event> eventsAfter(long afterSeq, int max) {
        return eventsAfter(afterSeq, Long.MAX_VALUE, max);
    }

    /**
     * {@link #eventsAfter(long, int)} restricted to events appended before {@code createdBefore}.
     * The feed holds this line back from now: seq is store-assigned, so an uncommitted append may
     * still hold a seq below one already visible, and a consumer that read past it would never be
     * offered it. Waiting out the window costs latency, not correctness.
     */
    List<Rows.Event> eventsAfter(long afterSeq, long createdBefore, int max);

    /** The highest seq the log has assigned, or 0 when it is empty. */
    long latestEventSeq();

    /** One consumer's cursor, or null when it has never polled. */
    Rows.EventCursor eventCursor(String consumer);

    /** The lowest seq any consumer cursor has acknowledged, or null when no cursor exists. */
    Long oldestAckedSeq();
}
