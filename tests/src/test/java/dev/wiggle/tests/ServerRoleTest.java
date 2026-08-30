package dev.wiggle.tests;

import dev.wiggle.core.Tls;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.coord.CoordinatorServer;
import dev.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@link WiggleServer} is a cell (the engine + control plane); the coordinator is a separate,
 * engine-free {@link CoordinatorServer}. The two share nothing in source but the gRPC contract.
 */
class ServerRoleTest {

    private static ServerConfig config() {
        return new ServerConfig(0, "cell-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("a cell serves the engine on a bound gRPC port")
    void cellServesEngine() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start()) {
            assertNotNull(server.engine(), "a cell exposes the engine");
            assertTrue(server.port() > 0, "a cell binds the WiggleControlPlane port");
        }
    }

    @Test @DisplayName("the coordinator is a standalone, engine-free server on its own gRPC port")
    void coordinatorServerStandalone() throws Exception {
        try (CoordinatorServer coordinator = new CoordinatorServer(
                new InMemoryCoordinatorStore(), 0, Tls.Options.DISABLED, 3, "coord-1").start()) {
            assertTrue(coordinator.port() > 0, "coordinator binds its CellCoordinator gRPC port");
        }
    }
}
