package com.wiggle.tests;

import com.wiggle.server.store.InMemoryStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Every engine scenario again, on a store that refuses unrouted transactions: each one a scenario
 * opens must name the shard it runs on, the way a sharded store needs.
 */
class RoutedConformanceTest extends EngineConformanceTest {

    @BeforeEach
    void routeChecked() {
        Scenarios.useStorage(config -> new RouteCheckingStorage(new InMemoryStorage()));
    }

    @AfterEach
    void restore() {
        Scenarios.useStorage(null);
    }
}
