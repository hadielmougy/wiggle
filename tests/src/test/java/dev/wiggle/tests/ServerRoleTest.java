package dev.wiggle.tests;

import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.ServerRole;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 0 / T1: a server composes subsystems by {@link ServerRole}. The default is {@code cell}
 * (unchanged behaviour); a {@code coordinator} runs no engine.
 */
class ServerRoleTest {

    private static ServerConfig config() {
        return new ServerConfig(0, "role-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("the default role is cell")
    void defaultRoleIsCell() {
        assertEquals(ServerRole.CELL, config().role());
    }

    @Test @DisplayName("cell role serves the engine on a bound gRPC port")
    void cellRoleServesEngine() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start()) {
            assertNotNull(server.engine(), "cell role exposes the engine");
            assertTrue(server.port() > 0, "cell role binds the WiggleControlPlane port");
        }
    }

    @Test @DisplayName("coordinator role starts cleanly and runs no engine")
    void coordinatorRoleRunsNoEngine() throws Exception {
        try (WiggleServer server = new WiggleServer(config().withRole(ServerRole.COORDINATOR)).start()) {
            assertThrows(IllegalStateException.class, server::engine, "no engine in coordinator role");
            assertEquals(-1, server.dashboardPort(), "coordinator serves no dashboard");
        }
    }

    @Test @DisplayName("WIGGLE_ROLE parsing: blank -> cell, case-insensitive, unknown rejected")
    void roleParsing() {
        assertEquals(ServerRole.CELL, ServerRole.fromString(null));
        assertEquals(ServerRole.CELL, ServerRole.fromString(""));
        assertEquals(ServerRole.CELL, ServerRole.fromString("cell"));
        assertEquals(ServerRole.COORDINATOR, ServerRole.fromString("COORDINATOR"));
        assertThrows(IllegalArgumentException.class, () -> ServerRole.fromString("bogus"));
    }
}
