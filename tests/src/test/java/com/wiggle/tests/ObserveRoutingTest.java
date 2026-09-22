package com.wiggle.tests;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;
import com.wiggle.observe.Observed;
import com.wiggle.observe.Observer;
import com.wiggle.observe.ObserverOptions;
import com.wiggle.observe.Run;
import com.wiggle.placement.IdCodec;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.proto.RingSlot;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.coord.CoordinatorApi;
import com.wiggle.server.coord.CoordinatorService;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Under a coordinator, every reporter of one observed run must reach the one cell that owns the
 * run's key: the coordinator resolves the key to a cell, and every cell derives the same id for
 * it, so wherever a service happens to sit, its steps land on the same instance.
 */
class ObserveRoutingTest {

    static {
        System.setProperty("wiggle.observe.settleMillis", "100");
    }

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
    }

    static final class Impl implements Steps {
        public Map<String, Object> a(Map<String, Object> ctx) { return ctx; }
        public Map<String, Object> b(Map<String, Object> ctx) { return ctx; }
    }

    private static ServerConfig cell(String cellId) {
        return new ServerConfig(0, "cell-" + cellId, null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)).withNamespace("acme").withCellId(cellId);
    }

    private static <T> T await(Supplier<T> probe) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            T v = probe.get();
            if (v != null) return v;
            if (System.currentTimeMillis() > deadline) fail("nothing landed in time");
            Thread.sleep(20);
        }
    }

    @Test @DisplayName("two services on two cells report one key and the owner cell holds the one instance")
    void keyRoutesToOneCell() throws Exception {
        FlowSpec spec = FlowSpec.define("checkout", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::a).thenApply(s::b));
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (WiggleServer cellA = new WiggleServer(cell("A")).start();
             WiggleServer cellB = new WiggleServer(cell("B")).start();
             CoordinatorService svc = new CoordinatorService(store);
             CoordinatorApi coord = new CoordinatorApi(svc, 0, Tls.Options.DISABLED)) {
            coord.start();
            svc.doRegister("acme", RegisteredNode.newBuilder().setCellId("A").setName("a").setEndpoint(cellA.baseUrl()).build());
            svc.doRegister("acme", RegisteredNode.newBuilder().setCellId("B").setName("b").setEndpoint(cellB.baseUrl()).build());
            svc.doOpenEpoch("acme", List.of(
                    RingSlot.newBuilder().setShard(0).setCellId("A").build(),
                    RingSlot.newBuilder().setShard(1).setCellId("B").build()));

            try (CoordinatedConnection conn = WiggleConnection.coordinator("127.0.0.1:" + coord.port(), Tls.Options.DISABLED, null);
                 Observer gateway = Observer.connect(conn, "acme", ObserverOptions.defaults().withReporter("gateway"));
                 Observer payments = Observer.connect(conn, "acme", ObserverOptions.defaults().withReporter("payments"))) {
                Observed<Steps> front = gateway.observe(spec, Steps.class, new Impl());
                Observed<Steps> back = payments.observe(spec, Steps.class, new Impl());

                // one key, two reporters, each connected only through the coordinator
                try (Run run = front.begin("order-9")) { front.steps().a(Map.of()); }
                try (Run run = back.join("order-9")) { back.steps().b(Map.of()); }

                WiggleServer owner = IdCodec.runKeyShard("checkout", "order-9") % 2 == 0 ? cellA : cellB;
                WiggleServer other = owner == cellA ? cellB : cellA;
                InstanceView v = await(() -> owner.engine().findByCorrelation("order-9", 1).stream()
                        .filter(InstanceView::isTerminal).findFirst().orElse(null));
                assertEquals("COMPLETED", v.status(), "both halves reached the owner");
                assertTrue(other.engine().findByCorrelation("order-9", 1).isEmpty(), "the other cell never saw the run");
                assertEquals(2, owner.engine().tokens(v.id()).stream().map(t -> t.leaseOwner)
                        .filter(o -> o != null).distinct().count(), "both reporters on one instance");
                assertEquals(IdCodec.runKeyId("acme", 0, "checkout", "order-9"), v.id(), "the derived, label-free id");

                // the id agrees across cells even when a report is misrouted: the same key on the
                // other cell derives the same id, so nothing can silently become a second run
                String misrouted = other.engine().observe("checkout", null, null, "order-9", "stray",
                        List.of(new StepInput(spec.definition().startNode(), null, null, null, 1L, 2L)), false).instanceId();
                assertEquals(v.id(), misrouted);
            }
        }
    }
}
