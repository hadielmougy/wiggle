package com.wiggle.server.store;

/**
 * The event log, and its consumer cursors: the writes, and the reads that must see the primary.
 * Held on an instance shard; the cursors on the home shard.
 */
public interface EventRepository extends EventReads {

    /** Moves a consumer's event position on {@code shard}, a shard other than home, to
     *  {@code ackedSeq}, never backwards. Held on the home shard. */
    void advanceEventPosition(String consumer, int shard, long ackedSeq);

    /** Appends to the event log and returns the seq the store assigned; visible with the transaction. */
    long appendEvent(Rows.Event event);

    /** Registers {@code cursor} if that consumer has none; leaves an existing one untouched. */
    void createEventCursorIfAbsent(Rows.EventCursor cursor);

    /** Moves the consumer's cursor to {@code ackedSeq}, never backwards, and stamps {@code now}. */
    void advanceEventCursor(String consumer, long ackedSeq, long now);

    /**
     * Deletes up to {@code max} of the oldest events created before {@code createdBefore}; when
     * {@code upToSeq} is non-null, only events with seq at or below it, so an unacknowledged
     * event outlives the age cap for as long as a consumer is still on its way to it.
     */
    int deleteEvents(long createdBefore, Long upToSeq, int max);
}
