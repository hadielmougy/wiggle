package com.wiggle.tests;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.placement.IdCodec;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.proto.RingSlot;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.coord.CoordinatorApi;
import com.wiggle.server.coord.CoordinatorService;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Routing an instance to the cell that actually wrote it, rather than to the cell the ring says owns
 * its shard.
 *
 * <p>Those two answers agree whenever the minting cell owned the shard in that epoch, which is the
 * normal case. They come apart in the one the genesis default creates: a cell mints into epoch 0 /
 * shard 0 until the coordinator places it, so two cells in a namespace that start while the
 * coordinator is unreachable mint ids differing only by their label. The ring names one of them, and
 * without the label every instance on the other is unreachable — resolved to a cell that has never
 * held it.
 */
class LabelledRoutingTest {

    interface OneStep {
        Map<String, Object> a(Map<String, Object> ctx);
    }

    static class Handlers implements OneStep {
        @Override public Map<String, Object> a(Map<String, Object> ctx) { return Map.of("ran", true); }
    }

    private static ServerConfig config(String cellId) {
        String url = "jdbc:h2:mem:lr-" + cellId + "-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        return new ServerConfig(TestPorts.free(), "node-" + cellId, url, "sa", "", 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10))
                .withNamespace("acme").withCellId(cellId);
    }

    private static FlowSpec workflow() {
        return FlowSpec.define("wf", 1, Map.class, OneStep.class, (f, s) -> f.thenApply(s::a));
    }

    @Test @DisplayName("an instance resolves to the cell named in its id, not the one the ring owns")
    void labelBeatsTheRing() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer cellA = new WiggleServer(config("cell-a"), new WiggleStorageFactory()).start();
             WiggleServer cellB = new WiggleServer(config("cell-b"), new WiggleStorageFactory()).start();
             CoordinatorService svc = new CoordinatorService(store);
             CoordinatorApi coord = new CoordinatorApi(svc, TestPorts.free(), Tls.Options.DISABLED)) {
            coord.start();

            svc.doRegister("acme", RegisteredNode.newBuilder().setCellId("cell-a")
                    .setName("node-cell-a").setEndpoint(cellA.baseUrl()).setRegion("eu-west").build());
            svc.doRegister("acme", RegisteredNode.newBuilder().setCellId("cell-b")
                    .setName("node-cell-b").setEndpoint(cellB.baseUrl()).setRegion("eu-west").build());

            // the ring names ONLY cell-a: resolving by shard sends everything there
            svc.doOpenEpoch("acme", List.of(RingSlot.newBuilder().setShard(0).setCellId("cell-a").build()));

            // both cells are still on their genesis placement (nothing pushed the ring to them), so
            // both mint epoch 0 / shard 0 -- ids that differ only by their cell label
            String onB;
            try (WiggleClient direct = new WiggleClient(cellB.baseUrl())) {
                direct.register(workflow());
                onB = direct.start("wf", Map.of());
            }
            IdCodec.Placement p = IdCodec.parse(onB).orElseThrow();
            assertEquals("cell-b", p.cellId(), "minted on cell-b: " + onB);
            assertEquals(0, p.epoch());
            assertEquals(0, p.shard(), "the shard the ring gives to cell-a");

            try (CoordinatedConnection resolver = WiggleConnection.coordinator(
                    "127.0.0.1:" + coord.port(), Tls.Options.DISABLED, "eu-west")) {

                // the instance lives on cell-b; the ring would send us to cell-a
                InstanceView v = resolver.clientForInstance(onB).instance(onB);
                assertEquals(onB, v.id(), "resolved to the cell that actually holds it");

                // and resolving the namespace (no instance) still goes through the ring
                assertEquals(1, resolver.activeCellTargets("acme").size(), "one cell in the ring");
            }
        }
    }

    @Test @DisplayName("two cells minting the same epoch and shard do not share a cached endpoint")
    void theClientCacheDoesNotCrossCells() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer cellA = new WiggleServer(config("cell-a"), new WiggleStorageFactory()).start();
             WiggleServer cellB = new WiggleServer(config("cell-b"), new WiggleStorageFactory()).start();
             CoordinatorService svc = new CoordinatorService(store);
             CoordinatorApi coord = new CoordinatorApi(svc, TestPorts.free(), Tls.Options.DISABLED)) {
            coord.start();
            svc.doRegister("acme", RegisteredNode.newBuilder().setCellId("cell-a")
                    .setName("node-cell-a").setEndpoint(cellA.baseUrl()).setRegion("eu-west").build());
            svc.doRegister("acme", RegisteredNode.newBuilder().setCellId("cell-b")
                    .setName("node-cell-b").setEndpoint(cellB.baseUrl()).setRegion("eu-west").build());
            svc.doOpenEpoch("acme", List.of(RingSlot.newBuilder().setShard(0).setCellId("cell-a").build()));

            String onA;
            String onB;
            try (WiggleClient a = new WiggleClient(cellA.baseUrl());
                 WiggleClient b = new WiggleClient(cellB.baseUrl())) {
                a.register(workflow());
                b.register(workflow());
                onA = a.start("wf", Map.of());
                onB = b.start("wf", Map.of());
            }
            // identical but for the label -- the shape that shared one cache entry before
            assertNotEquals(IdCodec.parse(onA).orElseThrow().cellId(),
                    IdCodec.parse(onB).orElseThrow().cellId());
            assertEquals(IdCodec.parse(onA).orElseThrow().shard(),
                    IdCodec.parse(onB).orElseThrow().shard());

            try (CoordinatedConnection resolver = WiggleConnection.coordinator(
                    "127.0.0.1:" + coord.port(), Tls.Options.DISABLED, "eu-west")) {
                // resolve A first so its endpoint is cached, then B: B must not inherit A's entry
                assertEquals(onA, resolver.clientForInstance(onA).instance(onA).id());
                assertEquals(onB, resolver.clientForInstance(onB).instance(onB).id(),
                        "B resolved to A's cached endpoint -- the cache key is missing the cell");
            }
        }
    }
}
