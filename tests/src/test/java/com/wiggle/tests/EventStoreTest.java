package com.wiggle.tests;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The event log's storage contract, against both backends: the in-memory store and the real SQL
 * (H2 by default, a live database when one is configured). Every assertion is relative to what
 * the log held when the test started, so a shared database other tests also write to is fine.
 */
class EventStoreTest {

    private static Storage jdbc() {
        String url = TestStorage.url("events");
        var dialect = url.startsWith("jdbc:postgresql") ? new PostgresDialect() : new H2Dialect();
        Storage storage = new JdbcStorage(url, TestStorage.user(), TestStorage.password(), 4, dialect);
        storage.migrate();
        return storage;
    }

    private static Rows.Event event(String instance, String type, long at) {
        return new Rows.Event(0, instance, "store-flow", 1, "key-" + instance, type, null, 1,
                "{\"reason\":\"" + type + "\"}", at);
    }

    @Test @DisplayName("in memory: append, read, cursors and trim behave as the feed expects")
    void inMemory() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            roundTrip(storage);
        }
    }

    @Test @DisplayName("on SQL: append, read, cursors and trim behave as the feed expects")
    void onJdbc() {
        try (Storage storage = jdbc()) {
            roundTrip(storage);
        }
    }

    private static void roundTrip(Storage storage) {
        String instance = "wfi-" + System.nanoTime();
        String consumer = "consumer-" + System.nanoTime();
        long base = storage.inTx(tx -> tx.latestEventSeq());
        long now = System.currentTimeMillis();

        long first = storage.inTx(tx -> tx.appendEvent(event(instance, "wf.started", now - 10_000)));
        long second = storage.inTx(tx -> tx.appendEvent(event(instance, "wf.completed", now - 9_000)));
        assertTrue(first > base, "seq is assigned by the store");
        assertTrue(second > first, "and only goes forward");
        long head = storage.inTx(tx -> tx.latestEventSeq());
        assertEquals(second, head, "the head is the newest seq");

        List<Rows.Event> mine = storage.inTx(tx -> tx.eventsAfter(base, 100));
        assertEquals(List.of("wf.started", "wf.completed"), mine.stream().map(Rows.Event::type).toList());
        assertEquals(instance, mine.get(0).instanceId());
        assertEquals("key-" + instance, mine.get(0).correlationId());
        assertEquals(1, mine.get(0).payloadVer());
        assertNull(mine.get(0).nodeId(), "a lifecycle entry names no step");
        assertTrue(mine.get(0).payload().contains("wf.started"), mine.get(0).payload());
        assertEquals(1, storage.inTx(tx -> tx.eventsAfter(base, 1)).size(), "max bounds the read");

        long emitted = storage.inTx(tx -> tx.appendEvent(new Rows.Event(0, instance, "store-flow", 1,
                "key-" + instance, "payment.captured", "n2", 1, "{\"amount\":42}", now - 8_000)));
        Rows.Event back = storage.inTx(tx -> tx.eventsAfter(second, 100)).getFirst();
        assertEquals(emitted, back.seq());
        assertEquals("n2", back.nodeId(), "an emitted entry names the step it came from");
        assertEquals("payment.captured", back.type());
        assertTrue(back.payload().contains("42"), back.payload());

        // The visibility window: an entry appended now is not served by a feed reading as of a
        // moment before it, which is what keeps a consumer from stepping over an in-flight append.
        long fresh = storage.inTx(tx -> tx.appendEvent(event(instance, "wf.cancelled", now)));
        assertTrue(storage.inTx(tx -> tx.eventsAfter(emitted, now - 1_000, 100)).isEmpty(),
                "nothing younger than the window is served");
        assertEquals(1, storage.inTx(tx -> tx.eventsAfter(emitted, now + 1_000, 100)).size(),
                "and it is served once the window has passed it");

        assertNull(storage.inTx(tx -> tx.eventCursor(consumer)), "an unknown consumer has no cursor");
        storage.inTx(tx -> { tx.createEventCursorIfAbsent(new Rows.EventCursor(consumer, first, now, now)); return null; });
        assertEquals(first, storage.inTx(tx -> tx.eventCursor(consumer)).ackedSeq());
        storage.inTx(tx -> { tx.createEventCursorIfAbsent(new Rows.EventCursor(consumer, 0, now, now)); return null; });
        assertEquals(first, storage.inTx(tx -> tx.eventCursor(consumer)).ackedSeq(),
                "registering again leaves a consumer where it was");

        storage.inTx(tx -> { tx.advanceEventCursor(consumer, second, now + 1); return null; });
        assertEquals(second, storage.inTx(tx -> tx.eventCursor(consumer)).ackedSeq());
        storage.inTx(tx -> { tx.advanceEventCursor(consumer, first, now + 2); return null; });
        assertEquals(second, storage.inTx(tx -> tx.eventCursor(consumer)).ackedSeq(), "an ack never moves back");

        String other = "consumer-" + System.nanoTime();
        storage.inTx(tx -> { tx.advanceEventCursor(other, first, now); return null; });
        assertEquals(first, storage.inTx(tx -> tx.eventCursor(other)).ackedSeq(),
                "an ack from a consumer that never registered creates its cursor");
        long slowest = storage.inTx(tx -> tx.oldestAckedSeq());
        assertTrue(slowest <= first, "the slowest cursor is what retention sees");

        // Retention: below the cutoff and at or below the given seq, oldest first.
        int trimmed = storage.inTx(tx -> tx.deleteEvents(now - 9_500, first, 100));
        assertEquals(1, trimmed, "only the entry that is both old enough and acknowledged");
        List<Rows.Event> left = storage.inTx(tx -> tx.eventsAfter(base, 100));
        assertEquals(List.of(second, emitted, fresh), left.stream().map(Rows.Event::seq).toList());
    }
}
