package com.wiggle.tests;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.store.StorageException.Classification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the JDBC store does with a failure it did not expect: which ones it replays, which ones it
 * refuses to replay, and what it tells the caller about the work either way.
 *
 * <p>The distinction under test is not "did it throw" but <em>what the throw says about the rows</em>.
 * A transaction that rolled back on a momentary failure applied nothing and may simply be run again,
 * which is what makes a database blip invisible to a workflow instead of a failed step. A statement
 * the database refused on its own terms is final. A commit whose outcome is unknown is neither, and
 * must never be replayed or reported as "nothing happened".
 *
 * <p>Runs on H2 by default and against {@code WIGGLE_TEST_DB_URL} when one is configured -- the
 * failures here are injected from the body, so the classification is exercised the same either way.
 * {@code postgres/PostgresDeadlockRetryTest} is the companion that provokes a real one.
 */
class TransientFailureTest {

    /** A rolled-back-and-retryable failure as a driver reports it: a serialization failure. */
    private static RuntimeException serializationFailure() {
        return new RuntimeException(new SQLException("could not serialize access", "40001"));
    }

    /** A failure the database will refuse identically every time: a syntax error. */
    private static RuntimeException syntaxError() {
        return new RuntimeException(new SQLException("syntax error at or near \"slect\"", "42601"));
    }

    /** A store whose replay bound is {@code attempts}, read at construction as the store reads it. */
    private static Storage storage(String label, int attempts) {
        String url = TestStorage.url(label);
        String previous = System.getProperty("wiggle.jdbc.txAttempts");
        System.setProperty("wiggle.jdbc.txAttempts", String.valueOf(attempts));
        System.setProperty("wiggle.jdbc.txRetryDelayMillis", "1");
        try {
            var dialect = url.startsWith("jdbc:postgresql") ? new PostgresDialect() : new H2Dialect();
            Storage storage = new JdbcStorage(url, TestStorage.user(), TestStorage.password(), 4, dialect);
            storage.migrate();
            return storage;
        } finally {
            if (previous == null) System.clearProperty("wiggle.jdbc.txAttempts");
            else System.setProperty("wiggle.jdbc.txAttempts", previous);
            System.clearProperty("wiggle.jdbc.txRetryDelayMillis");
        }
    }

    /** A name no other run shares: a live database keeps its rows between tests. */
    private static String unique(String prefix) {
        return prefix + "-" + System.nanoTime();
    }

    private static Optional<String> definition(Storage storage, String name) {
        return storage.inTx(tx -> tx.definition(name, 1));
    }

    @Test @DisplayName("a transaction that rolled back on a transient failure is replayed, and its writes are gone")
    void transientFailureIsReplayed() {
        String doomed = unique("doomed");
        String kept = unique("kept");
        try (Storage storage = storage("retry", 3)) {
            AtomicInteger attempts = new AtomicInteger();
            String written = storage.inTx(tx -> {
                // The first attempt writes, then dies: the replay must not see this row.
                if (attempts.incrementAndGet() == 1) {
                    tx.putDefinition(doomed, 1, "{}", "fp", "sha-256");
                    throw serializationFailure();
                }
                tx.putDefinition(kept, 1, "{}", "fp", "sha-256");
                return kept;
            });

            assertEquals(2, attempts.get(), "the body ran twice: the failure, then the replay");
            assertEquals(kept, written, "the replay's value is the one returned");
            assertTrue(definition(storage, kept).isPresent(), "the replay committed");
            assertFalse(definition(storage, doomed).isPresent(),
                    "the failed attempt rolled back, so nothing it wrote survived");
        }
    }

    @Test @DisplayName("a failure the database refuses on its own terms is not replayed and reaches the caller unchanged")
    void permanentFailureIsNotReplayed() {
        try (Storage storage = storage("no-retry", 3)) {
            AtomicInteger attempts = new AtomicInteger();
            RuntimeException thrown = syntaxError();
            Supplier<String> run = () -> storage.inTx(tx -> {
                attempts.incrementAndGet();
                throw thrown;
            });

            RuntimeException caught = assertThrows(RuntimeException.class, run::get);
            assertSame(thrown, caught, "a permanent failure is not relabelled on its way out");
            assertEquals(1, attempts.get(), "a permanent failure gets one attempt, not three");
        }
    }

    @Test @DisplayName("replays that run out leave a TRANSIENT StorageException: nothing was applied")
    void exhaustedReplaysSayNothingWasApplied() {
        try (Storage storage = storage("exhausted", 3)) {
            AtomicInteger attempts = new AtomicInteger();
            Supplier<String> run = () -> storage.inTx(tx -> {
                attempts.incrementAndGet();
                throw serializationFailure();
            });

            StorageException e = assertThrows(StorageException.class, run::get);
            assertEquals(3, attempts.get(), "every attempt the bound allows was used");
            assertEquals(Classification.TRANSIENT, e.classification());
            assertTrue(e.repeatable(), "the caller may run the whole call again");
        }
    }

    @Test @DisplayName("one attempt disables the replay without changing the classification")
    void replayCanBeTurnedOff() {
        try (Storage storage = storage("single", 1)) {
            AtomicInteger attempts = new AtomicInteger();
            Supplier<String> run = () -> storage.inTx(tx -> {
                attempts.incrementAndGet();
                throw serializationFailure();
            });

            StorageException e = assertThrows(StorageException.class, run::get);
            assertEquals(1, attempts.get(), "txAttempts=1 is one attempt and no replay");
            assertEquals(Classification.TRANSIENT, e.classification(), "it is still the same failure");
        }
    }

    @Test @DisplayName("an unknown commit is not repeatable, and an unclassified failure is permanent")
    void theClassificationsCarryTheirOwnPromises() {
        StorageException ambiguous = new StorageException("commit failed", null, Classification.AMBIGUOUS);
        assertFalse(ambiguous.repeatable(), "a commit that may have landed must not be re-applied");

        StorageException unclassified = new StorageException("something", null);
        assertEquals(Classification.PERMANENT, unclassified.classification(),
                "the default is the safe answer: cost a retry rather than re-apply durable work");
        assertFalse(unclassified.repeatable());
    }
}
