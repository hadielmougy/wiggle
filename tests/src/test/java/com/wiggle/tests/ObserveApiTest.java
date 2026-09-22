package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.InstanceView;
import com.wiggle.core.NodeStats;
import com.wiggle.observe.ObservedFlow;
import com.wiggle.observe.Observer;
import com.wiggle.observe.ObserverOptions;
import com.wiggle.observe.StepTimer;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.Rows.Token;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The observe module end to end: services report steps by run key, step name and times, and the
 * server ends up with one timed, conformance-checked instance per key -- without a worker, and
 * without any service ever waiting on the server.
 */
class ObserveApiTest {

    static {
        // Runs are judged by the server's settle sweep; keep its grace short so a verdict lands fast.
        System.setProperty("wiggle.observe.settleMillis", "100");
        System.setProperty("wiggle.observe.stallMillis", "600000");
    }

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        boolean keep(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
    }

    private static FlowSpec spec(String name) {
        return FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) -> f
                .thenApply(s::a).thenApply(s::b).thenFilter(s::keep).thenApply(s::c));
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "obs-api-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static <T> T await(Supplier<T> probe) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            T v = probe.get();
            if (v != null) return v;
            if (System.currentTimeMillis() > deadline) fail("nothing reported within 10s");
            Thread.sleep(20);
        }
    }

    private static InstanceView awaitTerminal(WiggleServer server, String key) throws InterruptedException {
        return await(() -> server.engine().findByCorrelation(key, 1).stream().filter(InstanceView::isTerminal).findFirst().orElse(null));
    }

    @Test @DisplayName("two services report one run by key; the server completes one timed instance with both reporters")
    void twoServicesOneRun() throws Exception {
        FlowSpec spec = spec("obs-api");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer gateway = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("gateway"));
             Observer payments = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("payments"))) {
            ObservedFlow front = gateway.publish(spec);
            ObservedFlow back = payments.publish(spec);
            long t0 = System.currentTimeMillis() - 1000;

            front.record("order-1", "a", t0, t0 + 10);
            front.record("order-1", "b", t0 + 10, t0 + 50);
            back.recordPredicate("order-1", "keep", true, t0 + 60, t0 + 61);
            StepTimer c = back.start("order-1", "c");
            Thread.sleep(15);
            c.done();

            InstanceView v = awaitTerminal(server, "order-1");
            assertEquals("COMPLETED", v.status());
            assertEquals(1, server.engine().findByCorrelation("order-1", 10).size(), "one instance for the key");
            assertTrue(server.engine().anomalies(null, v.id(), 10).isEmpty());
            Map<String, Token> done = new LinkedHashMap<>();
            for (Token t : server.engine().tokens(v.id())) if (t.startedAt != null) done.put(t.nodeId, t);
            assertEquals(4, done.size(), "every step settled, timed");
            String bNode = spec.definition().node(spec.definition().startNode()).next();
            assertEquals(40, done.get(bNode).finishedAt - done.get(bNode).startedAt, "the caller's own times are kept");
            assertEquals(Set.of("gateway", "payments"),
                    done.values().stream().map(t -> t.leaseOwner).collect(Collectors.toSet()), "each token names its reporter");
            assertTrue(done.get(spec.definition().node(spec.definition().node(bNode).next()).next()).finishedAt
                    - done.get(spec.definition().node(spec.definition().node(bNode).next()).next()).startedAt >= 15, "the timer measured c");

            List<NodeStats> stats = server.engine().stepStats(spec.name(), null, 0, 100);
            assertEquals("b", stats.getFirst().name(), "slowest p95 first");
            assertEquals(0, gateway.dropped() + payments.dropped());
            assertTrue(server.engine().poll("w1", spec.queues(), 10, null).isEmpty(), "nothing was ever dispatched");
        }
    }

    @Test @DisplayName("a step reported as failed fails the run; an unknown step name is refused in the service")
    void failureAndValidation() throws Exception {
        FlowSpec spec = spec("obs-api-fail");
        try (WiggleServer server = new WiggleServer(config()).start();
             Observer observer = Observer.connect(server.baseUrl())) {
            ObservedFlow flow = observer.publish(spec);
            long t0 = System.currentTimeMillis() - 1000;
            flow.record("order-2", "a", t0, t0 + 1);
            StepTimer b = flow.start("order-2", "b");
            b.failed(new IllegalStateException("boom"));
            InstanceView v = awaitTerminal(server, "order-2");
            assertEquals("FAILED", v.status());
            assertEquals("b: IllegalStateException: boom", v.error());

            assertThrows(IllegalArgumentException.class, () -> flow.record("order-3", "nope", t0, t0));
            assertThrows(IllegalArgumentException.class, () -> flow.record("", "a", t0, t0));
        }
    }

    @Test @DisplayName("a run ended by its originator before END is incomplete; closing the observer flushes what is queued")
    void endAndClose() throws Exception {
        FlowSpec spec = spec("obs-api-end");
        try (WiggleServer server = new WiggleServer(config()).start()) {
            Observer observer = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withLinger(Duration.ofSeconds(30)));
            ObservedFlow flow = observer.publish(spec);
            long t0 = System.currentTimeMillis() - 1000;
            flow.record("order-4", "a", t0, t0 + 1);
            flow.end("order-4");
            assertTrue(server.engine().findByCorrelation("order-4", 1).isEmpty(), "still queued behind the linger");
            observer.close();
            assertEquals(0, observer.dropped(), "close flushed rather than dropped");
            assertFalse(server.engine().findByCorrelation("order-4", 1).isEmpty(), "the run landed on close");
            InstanceView v = awaitTerminal(server, "order-4");
            assertEquals("FAILED", v.status());
            assertEquals("INCOMPLETE", server.engine().anomalies(null, v.id(), 10).getFirst().kind());
        }
    }

    @Test @DisplayName("a spec that names a worker mode is refused before anything is published")
    void refusesWorkerRunFlows() throws Exception {
        FlowSpec server = FlowSpec.define("obs-api-srv", 1, Map.class, Steps.class, (f, s) -> f.executeInServer().thenApply(s::a));
        try (WiggleServer srv = new WiggleServer(config()).start();
             Observer observer = Observer.connect(srv.baseUrl());
             WiggleClient client = new WiggleClient(srv.baseUrl())) {
            assertThrows(IllegalArgumentException.class, () -> observer.publish(server));
            assertFalse(client.workflowNames().contains("obs-api-srv"));
        }
    }
}
