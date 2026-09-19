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
            TokenLifecycle tokens = new TokenLifecycle(new DefinitionRegistry(storage), transactions::wake);
            Node node = Node.task("one", "one", "one", QUEUE, null);

            Map<String, Long> before = notifier.snapshot(Set.of(QUEUE));
            transactions.inTxVoid(tx -> {
                long now = 1_000L;
                Instance inst = InstanceState.mint(tx, "wfi_1", "w", 1, Map.of(), null, null, now);
                Token t = TokenState.mint(inst, "one", "", null, now);
                tx.insertToken(t);
                tokens.parkReady(tx, inst, t, node, now);
            });
            Map<String, Long> after = notifier.snapshot(Set.of(QUEUE));

            assertTrue(after.getOrDefault(QUEUE, 0L) > before.getOrDefault(QUEUE, 0L),
                    "a token parked READY on '" + QUEUE + "' did not signal its queue; every poll for it "
                            + "now waits out the fallback interval instead of being woken");
        }
    }

    @Test
    @DisplayName("a queue marked inside a nested transaction is signalled by the outermost commit")
    void nestedScopesSignalOnce() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DispatchNotifier notifier = new DispatchNotifier();
            Transactions transactions = new Transactions(storage, notifier);

            Map<String, Long> before = notifier.snapshot(Set.of(QUEUE));
            transactions.inTxVoid(outer -> transactions.inTxVoid(inner -> transactions.wake(QUEUE)));
            Map<String, Long> after = notifier.snapshot(Set.of(QUEUE));

            assertTrue(after.getOrDefault(QUEUE, 0L) > before.getOrDefault(QUEUE, 0L),
                    "an inner scope's queue must reach the outermost commit's signal");
        }
    }
}
