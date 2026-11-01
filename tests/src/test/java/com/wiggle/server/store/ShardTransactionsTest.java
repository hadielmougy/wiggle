package com.wiggle.server.store;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Routed transactions bound to the calling thread, and how they nest. */
class ShardTransactionsTest {

    private static final String ON_SEVEN = "wfi.s7.01k6abc";

    @Test @DisplayName("the repositories run on the transaction bound for the thread, and refuse without one")
    void repositoriesFollowTheBoundTransaction() {
        try (InMemoryStorage store = new InMemoryStorage()) {
            ShardTransactions shards = new ShardTransactions(store);
            shards.inHome(() -> Repositories.schedules().putSchedule(schedule("s1")));
            assertTrue(shards.readHome(() -> Repositories.schedules().scheduleByWorkflow("w-s1")).isPresent());
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> Repositories.schedules().schedules());
            assertTrue(e.getMessage().contains("no transaction"), e.getMessage());
        }
    }

    @Test @DisplayName("a read inside a transaction on the same shard joins it; on another shard it gets its own")
    void readsJoinOnTheSameShardOnly() {
        try (InMemoryStorage seven = new InMemoryStorage(); InMemoryStorage two = new InMemoryStorage()) {
            ShardTransactions shards = new ShardTransactions(new ShardedStorage(Map.of(7, seven, 2, two), 2));
            shards.forInstance(ON_SEVEN, () -> {
                ReadTx outer = CurrentTx.require();
                shards.read(7, () -> assertSame(outer, CurrentTx.require(), "same shard: the outer transaction"));
                shards.readHome(() -> assertNotSame(outer, CurrentTx.require(), "another shard: one of its own"));
                assertSame(outer, CurrentTx.require(), "the outer transaction is bound again afterwards");
            });
        }
    }

    @Test @DisplayName("a write inside a write on the same shard joins it; on another shard it is refused")
    void writesJoinOrAreRefused() {
        try (InMemoryStorage seven = new InMemoryStorage(); InMemoryStorage two = new InMemoryStorage()) {
            ShardTransactions shards = new ShardTransactions(new ShardedStorage(Map.of(7, seven, 2, two), 2));
            shards.forInstance(ON_SEVEN, () -> {
                ReadTx outer = CurrentTx.require();
                shards.inShard(7, () -> assertSame(outer, CurrentTx.require()));
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> shards.inHome(() -> { }));
                assertTrue(e.getMessage().contains("would commit on its own"), e.getMessage());
            });
            shards.inHome(() -> { });   // nothing left bound
        }
    }

    private static Rows.Schedule schedule(String id) {
        Rows.Schedule s = new Rows.Schedule();
        s.id = id;
        s.workflow = "w-" + id;
        s.intervalMillis = 1000;
        s.nextFireAt = 1;
        s.createdAt = 1;
        return s;
    }
}
