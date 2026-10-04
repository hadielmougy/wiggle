package com.wiggle.tests;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.server.store.ShardedStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.LinkedHashMap;
import java.util.Map;

/** {@link ShardedConformanceTest} with each shard its own H2 database behind {@link JdbcStorage}. */
class ShardedJdbcConformanceTest extends EngineConformanceTest {

    private static Storage h2(String name) {
        return new JdbcStorage("jdbc:h2:mem:" + name + "-" + System.nanoTime()
                + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 8, new H2Dialect());
    }

    @BeforeEach
    void twoDatabases() {
        Scenarios.useStorage(config -> {
            Map<Integer, Storage> shards = new LinkedHashMap<>();
            shards.put(1, h2("shard1"));
            shards.put(0, h2("shard0"));
            return new ShardedStorage(shards, 0);
        });
    }

    @AfterEach
    void restore() {
        Scenarios.useStorage(null);
    }
}
