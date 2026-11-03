package com.wiggle.boot;

import com.wiggle.boot.test.WiggleTestServer;
import com.wiggle.client.WiggleClient;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.testfixtures.TestPorts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/** A node the test configures itself runs on its config and its storage factory. */
class WiggleTestServerTest {

    @Test @DisplayName("the test's config and storage factory replace the application's")
    void runsOnTheTestsConfig() {
        ServerConfig config = new ServerConfig(TestPorts.free(), "own-config", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(700), Duration.ofHours(1), 100, 0);
        AtomicInteger built = new AtomicInteger();
        try (WiggleTestServer server = WiggleTestServer.start(config, c -> {
            built.incrementAndGet();
            return new PostgresStorageFactory().create(c);
        }); WiggleClient client = new WiggleClient(server.baseUrl())) {
            assertSame(config, server.node().config());
            assertEquals(1, built.get(), "the store came from the test's factory");
            client.cluster();
        }
    }
}
