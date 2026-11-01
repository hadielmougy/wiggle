package com.wiggle.server.engine;

import com.wiggle.core.Node;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.CurrentTx;
import com.wiggle.server.store.ReadTx;
import com.wiggle.server.store.Repositories;
import com.wiggle.server.store.ShardedStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
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
    private static final ClaimFilter QUEUE_FILTER = ClaimFilter.byName(Set.of(QUEUE), null);
    private static final FallbackPollPolicy FALLBACK =
            new FallbackPollPolicy(new PollerRegistry(60_000), 100, 100, false);

    @Test
    @DisplayName("parking a token READY signals its queue after the transaction commits")
    void parkingReadySignalsTheQueue() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DispatchNotifier notifier = new DispatchNotifier();
            Transactions transactions = new Transactions(storage, notifier);
            Tokens tokens = new Tokens(new DefinitionRegistry(storage), transactions::wake);
            Node node = Node.task("one", "one", "one", QUEUE, null);

            DispatchNotifier.Watch watch = notifier.watch(QUEUE_FILTER, 1, FALLBACK.schedule("w", QUEUE_FILTER));
            transactions.inTxVoid("wfi_1", () -> {
                long now = 1_000L;
                Instance inst = Instances.create("wfi_1", "w", 1, Map.of(), null, null, now);
                Token t = Tokens.create(inst, "one", "", null, now);
                Repositories.tokens().insertToken(t);
                tokens.markReady(inst, t, node, now);
            });

            assertTrue(watch.await(System.currentTimeMillis()),
                    "a token parked READY on '" + QUEUE + "' did not signal its queue; every poll for it "
                            + "now waits out the fallback interval instead of being woken");
        }
    }

    /**
     * A scope opened inside another on the same shard joins it: one transaction, on one connection,
     * so it cannot block on the instance lock the outer holds. Its queues are woken with the outer's,
     * once the outer commits.
     */
    @Test
    @DisplayName("a scope opened inside another on the same shard joins it, and wakes only after the outer commits")
    void nestedScopeOnTheSameShardJoins() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DispatchNotifier notifier = new DispatchNotifier();
            Transactions transactions = new Transactions(storage, notifier);

            DispatchNotifier.Watch watch = notifier.watch(QUEUE_FILTER, 1, FALLBACK.schedule("w", QUEUE_FILTER));
            transactions.inTxVoid("wfi_1", () -> {
                ReadTx outer = CurrentTx.require();
                transactions.inTxVoid("wfi_1", () -> {
                    assertSame(outer, CurrentTx.require(), "the nested scope runs in the outer transaction");
                    transactions.wake(QUEUE, null, null);
                });
                assertFalse(watch.await(System.currentTimeMillis()), "nothing is woken before the outer commits");
            });
            assertTrue(watch.await(System.currentTimeMillis()), "the nested scope's queue is woken after the commit");
        }
    }

    /**
     * A write scope on another shard would be a second, independent transaction: it would commit
     * whatever became of the outer one. It is refused at the call, and the thread can open a fresh
     * scope afterwards.
     */
    @Test
    @DisplayName("a write scope inside one on another shard is refused, and leaves the thread able to open a fresh one")
    void nestedScopeOnAnotherShardIsRefused() {
        try (InMemoryStorage seven = new InMemoryStorage(); InMemoryStorage two = new InMemoryStorage()) {
            Storage storage = new ShardedStorage(Map.of(7, seven, 2, two), 2);
            DispatchNotifier notifier = new DispatchNotifier();
            Transactions transactions = new Transactions(storage, notifier);

            IllegalStateException nested = assertThrows(IllegalStateException.class,
                    () -> transactions.inTxVoid("wfi.s7.01k6abc", () -> transactions.inHome(() -> null)));
            assertTrue(nested.getMessage().contains("would commit on its own"), nested.getMessage());

            DispatchNotifier.Watch watch = notifier.watch(QUEUE_FILTER, 1, FALLBACK.schedule("w", QUEUE_FILTER));
            transactions.inTxVoid("wfi.s7.01k6abc", () -> transactions.wake(QUEUE, null, null));
            assertTrue(watch.await(System.currentTimeMillis()),
                    "the refused scope must not have left this thread's state stuck");
        }
    }
}
