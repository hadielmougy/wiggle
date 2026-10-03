package com.wiggle.tests;

import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.ShardedStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Every engine scenario again, on two shards. Instances are minted on the shards in turn, shard 1
 * first, so even a scenario that starts one instance runs it off the home shard; every transaction
 * must find the shard that holds what it touches.
 */
class ShardedConformanceTest extends EngineConformanceTest {

    @BeforeEach
    void twoShards() {
        Scenarios.useStorage(config -> {
            Map<Integer, Storage> shards = new LinkedHashMap<>();
            shards.put(1, new InMemoryStorage());
            shards.put(0, new InMemoryStorage());
            return new ShardedStorage(shards, 0);
        });
    }

    @AfterEach
    void restore() {
        Scenarios.useStorage(null);
    }
}
