package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.placement.IdCodec;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A running cell stamps its own id into every instance id it mints.
 *
 * <p>The point of the label is routing <em>without</em> a ring to consult: epoch and shard say where
 * an instance belongs under the current placement, the cell says where it was actually written. So
 * the case that matters most here is the one with no coordinator at all — if the label only appeared
 * on coordinator-managed cells it would be useless to the deployment that needs it.
 */
class CellLabelledIdTest {

    interface Steps {
        Map<String, Object> work(Map<String, Object> c);
    }

    private static ServerConfig config() {
        String url = "jdbc:h2:mem:cid-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        return new ServerConfig(0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static FlowSpec spec() {
        return FlowSpec.define("labelled", Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    }

    private String startOn(ServerConfig config) throws Exception {
        try (WiggleServer server = new WiggleServer(config, new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec());
            return client.start("labelled", Map.of());
        }
    }

    @Test @DisplayName("a namespaced cell with no coordinator still labels its ids")
    void standaloneCellLabels() throws Exception {
        String id = startOn(config().withNamespace("orders").withCellId("cell-a"));

        IdCodec.Placement p = IdCodec.parse(id).orElseThrow(
                () -> new AssertionError("expected a routable id, got " + id));
        assertEquals("orders", p.namespace());
        assertEquals("cell-a", p.cellId(), "the id must name the cell that wrote it: " + id);
        assertTrue(p.hasCell());
        assertEquals(0, p.epoch(), "no coordinator, so no ring: epoch stays 0");
    }

    @Test @DisplayName("without a cell id the format is unchanged -- adopting the label is opt-in")
    void noCellIdMeansNoSegment() throws Exception {
        String id = startOn(config().withNamespace("orders"));

        IdCodec.Placement p = IdCodec.parse(id).orElseThrow();
        assertEquals("orders", p.namespace());
        assertFalse(p.hasCell(), "no WIGGLE_CELL_ID, no segment: " + id);
    }

    @Test @DisplayName("without a namespace the id stays legacy, labelled or not")
    void noNamespaceStaysLegacy() throws Exception {
        String id = startOn(config().withCellId("cell-a"));

        assertTrue(IdCodec.isLegacy(id),
                "a cell id alone does not make an id routable -- the namespace is the first segment: " + id);
        assertTrue(id.startsWith("wfi_"), id);
    }

    @Test @DisplayName("the label survives the round trip through storage, not just the mint")
    void labelSurvivesStorage() throws Exception {
        ServerConfig config = config().withNamespace("orders").withCellId("cell-a");
        try (WiggleServer server = new WiggleServer(config, new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec());
            String id = client.start("labelled", Map.of());

            // read it back from the store rather than trusting the value the client was handed
            String persisted = client.instance(id).id();
            assertEquals(id, persisted);
            assertEquals("cell-a", IdCodec.parse(persisted).orElseThrow().cellId());
        }
    }

    @Test @DisplayName("a long namespace and cell round-trip through storage -- what widening bought")
    void longIdsFitTheWidenedColumn() throws Exception {
        // 64 characters would not have held this; schema v9 widened the three instance-id columns.
        String ns = "orders-fulfilment-eu-west";          // 25
        String cell = "pooled-cell-frankfurt-03";         // 24
        ServerConfig config = config().withNamespace(ns).withCellId(cell);

        try (WiggleServer server = new WiggleServer(config, new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec());
            String id = client.start("labelled", Map.of());

            assertTrue(id.length() > 64, "the point of the migration: " + id + " is " + id.length());
            assertTrue(id.length() <= IdCodec.MAX_LENGTH, id);

            // it is not enough that the id was minted -- it has to survive the insert and come back
            assertEquals(id, client.instance(id).id());
            IdCodec.Placement p = IdCodec.parse(id).orElseThrow();
            assertEquals(ns, p.namespace());
            assertEquals(cell, p.cellId());
        }
    }
}
