package dev.wiggle.tests;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.IdCodec;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2 / T8: a cell with a placement namespace mints epoch-aware instance ids; a standalone cell
 * (no namespace) keeps the legacy {@code wfi_} form. End-to-end through the real start path.
 */
class EpochAwareIdTest {

    private static ServerConfig config() {
        return new ServerConfig(0, "id-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static Blueprint<Map<String, Object>> workflow() {
        return Workflow.define("wf").step("a", c -> c).build();
    }

    @Test @DisplayName("a namespace-configured cell mints ns.e0.s0.<ulid> ids")
    void namespacedCellMintsEpochAwareIds() throws Exception {
        try (WiggleServer server = new WiggleServer(config().withNamespace("acme")).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(workflow());
            String id = client.start("wf", Map.of());
            IdCodec.Placement p = IdCodec.parse(id)
                    .orElseThrow(() -> new AssertionError("expected an epoch-aware id, got: " + id));
            assertEquals("acme", p.namespace());
            assertEquals(0, p.epoch());
            assertEquals(0, p.shard());
        }
    }

    @Test @DisplayName("a standalone cell (no namespace) keeps legacy wfi_ ids")
    void standaloneMintsLegacyIds() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(workflow());
            String id = client.start("wf", Map.of());
            assertTrue(IdCodec.isLegacy(id), "standalone id should be legacy: " + id);
            assertTrue(id.startsWith("wfi_"), "legacy id keeps the wfi_ prefix: " + id);
        }
    }
}
