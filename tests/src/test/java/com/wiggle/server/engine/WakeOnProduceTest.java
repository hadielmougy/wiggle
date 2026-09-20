package com.wiggle.server.engine;

import com.wiggle.core.Node;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wake-on-produce, pinned at the seam it actually crosses: a token parked READY inside a
 * transaction must signal its queue to the {@link DispatchNotifier} once that transaction commits.
 *
 * <p>This is Layer 1 of dispatch (docs/in-memory-dispatch.md) and it is invisible to every other
 * test in the suite -- if the signal never fires, work is still delivered, just a fallback poll
 * interval later. Correctness tests stay green while throughput quietly collapses, so the contract
 * needs a test of its own rather than being inferred from behaviour.
 */
class WakeOnProduceTest {

    private static final String QUEUE = "q";

    @Test
    @DisplayName("parking a token READY signals its queue after the transaction commits")
    void parkingReadySignalsTheQueue() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DispatchNotifier notifier = new DispatchNotifier();
            Transactions transactions = new Transactions(storage, notifier);
            Tokens tokens = new Tokens(new DefinitionRegistry(storage), transactions::wake);
            Node node = Node.task("one", "one", "one", QUEUE, null);

            Map<String, Long> before = notifier.snapshot(Set.of(QUEUE));
            transactions.inTxVoid(tx -> {
                long now = 1_000L;
                Instance inst = Instances.create(tx, "wfi_1", "w", 1, Map.of(), null, null, now);
                Token t = Tokens.create(inst, "one", "", null, now);
                tx.insertToken(t);
                tokens.markReady(tx, t, node, now);
            });
            Map<String, Long> after = notifier.snapshot(Set.of(QUEUE));

            assertTrue(after.getOrDefault(QUEUE, 0L) > before.getOrDefault(QUEUE, 0L),
                    "a token parked READY on '" + QUEUE + "' did not signal its queue; every poll for it "
                            + "now waits out the fallback interval instead of being woken");
        }
    }

    /**
     * A storage transaction is a borrowed connection, so a nested one is an independent transaction
     * on a second connection: it commits regardless of the outer, cannot see the outer's uncommitted
     * writes, and blocks on the instance row lock the outer already holds -- a deadlock against
     * itself. Nothing in the engine nests today; this keeps it that way by failing at the call
     * rather than hanging at the lock.
     */
    @Test
    @DisplayName("opening a transaction inside a transaction is refused, not silently nested")
    void nestedScopesAreRefused() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            Transactions transactions = new Transactions(storage, new DispatchNotifier());

            IllegalStateException nested = assertThrows(IllegalStateException.class,
                    () -> transactions.inTxVoid(outer -> transactions.inTxVoid(inner -> { })));
            assertTrue(nested.getMessage().contains("nested transaction scope"), nested.getMessage());
        }
    }

    @Test
    @DisplayName("a refused nested scope leaves the thread able to open a fresh one")
    void aFailedScopeUnwindsCleanly() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DispatchNotifier notifier = new DispatchNotifier();
            Transactions transactions = new Transactions(storage, notifier);

            assertThrows(IllegalStateException.class,
                    () -> transactions.inTxVoid(outer -> transactions.inTxVoid(inner -> { })));

            Map<String, Long> before = notifier.snapshot(Set.of(QUEUE));
            transactions.inTxVoid(tx -> transactions.wake(QUEUE));
            assertTrue(notifier.snapshot(Set.of(QUEUE)).getOrDefault(QUEUE, 0L)
                            > before.getOrDefault(QUEUE, 0L),
                    "the failed scope must not have left this thread's state stuck");
        }
    }
}
