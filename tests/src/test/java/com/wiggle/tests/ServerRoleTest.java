package com.wiggle.tests;

import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A {@link WiggleServer} runs the engine and serves the control plane on a bound gRPC port. */
class ServerRoleTest {

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "cell-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("a server serves the engine on a bound gRPC port")
    void cellServesEngine() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start()) {
            assertNotNull(server.engine(), "a server exposes the engine");
            assertTrue(server.port() > 0, "a server binds the WiggleControlPlane port");
        }
    }
}
