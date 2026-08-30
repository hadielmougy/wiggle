package com.wiggle.client.worker;

import com.wiggle.client.CellResolver;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.Tls;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.coord.CoordinatorApi;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The coordinator-aware worker fans polling out across a namespace's active cells and drops a cell's
 * worker when it leaves the set (drain/retire). The active-cell set is driven here so add/remove is
 * deterministic; a second test wires it through a real {@link CellResolver}/coordinator.
 */
class NamespaceWorkerTest {

    private static ServerConfig config() {
        return new ServerConfig(0, "nw", null, null, null, 4,
                Duration.ofMillis(50), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static Blueprint<Map<String, Object>> workflow() {
        return Workflow.define("wf").step("a", c -> c).build();
    }

    private static final Duration NEVER = Duration.ofHours(1);   // pin the auto-reconcile out of the way

    @Test @DisplayName("serves every active cell, and stops a cell's worker when it retires")
    void fanOutAcrossCellsAndRetire() throws Exception {
        Blueprint<Map<String, Object>> bp = workflow();
        try (WiggleServer a = new WiggleServer(config()).start();
             WiggleServer b = new WiggleServer(config()).start();
             WiggleClient ca = new WiggleClient(a.baseUrl());
             WiggleClient cb = new WiggleClient(b.baseUrl())) {

            ca.register(bp);   // the workflow must exist on each cell (fan-out, in production)
            cb.register(bp);

            AtomicReference<List<String>> cells = new AtomicReference<>(List.of(a.baseUrl(), b.baseUrl()));
            try (NamespaceWorker nw = new NamespaceWorker(cells::get, WiggleClient::new, "w",
                    WorkerOptions.defaults(), w -> w.register(bp))) {
                nw.reconcileEvery(NEVER).start();
                assertEquals(Set.of(a.baseUrl(), b.baseUrl()), nw.activeCells(), "one worker per active cell");

                // work in either cell is picked up and completed
                assertEquals("COMPLETED", ca.awaitCompletion(ca.start("wf", Map.of()), Duration.ofSeconds(5)).status());
                assertEquals("COMPLETED", cb.awaitCompletion(cb.start("wf", Map.of()), Duration.ofSeconds(5)).status());

                // cell B retires -> it leaves the active set and its worker stops
                cells.set(List.of(a.baseUrl()));
                nw.reconcile();
                assertEquals(Set.of(a.baseUrl()), nw.activeCells());

                // a new instance in the retired cell is no longer served; cell A still is
                String stranded = cb.start("wf", Map.of());
                Thread.sleep(800);
                assertEquals("RUNNING", cb.instance(stranded).status(), "no worker polls the retired cell");
                assertEquals("COMPLETED", ca.awaitCompletion(ca.start("wf", Map.of()), Duration.ofSeconds(5)).status());
            }
        }
    }

    @Test @DisplayName("wired through a real coordinator, it serves the namespace's resolved cell")
    void coordinatorWired() throws Exception {
        Blueprint<Map<String, Object>> bp = workflow();
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        CoordinatorApi coord = new CoordinatorApi(store, 0, Tls.Options.DISABLED);
        coord.start();
        try (WiggleServer cell = new WiggleServer(config().withNamespace("orders")).start();
             WiggleClient cc = new WiggleClient(cell.baseUrl())) {

            cc.register(bp);
            coord.doRegister("orders", RegisteredNode.newBuilder()
                    .setName(cell.baseUrl()).setEndpoint(cell.baseUrl()).build());

            CellResolver resolver = CellResolver.coordinator("127.0.0.1:" + coord.port(), Tls.Options.DISABLED, "");
            try (NamespaceWorker nw = new NamespaceWorker(resolver, "orders", "w", w -> w.register(bp))) {
                nw.reconcileEvery(NEVER).start();
                assertEquals(Set.of(cell.baseUrl()), nw.activeCells(), "resolved the namespace's one active cell");
                assertEquals("COMPLETED", cc.awaitCompletion(cc.start("wf", Map.of()), Duration.ofSeconds(5)).status());
            } finally {
                resolver.close();
            }
        } finally {
            coord.close();
        }
    }
}
