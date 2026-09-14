package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A worker needs handlers, not a topology. Publishing is the author's job -- {@code client.register}
 * -- and a worker never needs the spec at all: {@code matchHandlerSet} fetches the graph from the
 * server and binds against that, as the Go and Python clients do.
 *
 * <p>This pins that a worker built with {@code handlers(...)} alone runs a flow <b>under every
 * execution mode</b>. The local modes are the interesting ones, because they are the only place
 * the worker needs the graph for itself: it traverses the compiled definition to chain the next step
 * without asking the server. That cache is filled when the handlers are bound, from the fetched
 * graph -- not from {@code register} -- so nothing about local execution depends on the convenience.
 *
 * <p>And if the graph were ever missing for a task's version, {@code Worker.execute} falls back to
 * server-driven, one step at a time: slower, still correct. Losing the spec cannot lose an instance.
 */
class HandlerOnlyWorkerTest {

    /** The steps this spec names; a worker binds them by name. */
    interface OneStep {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
        Map<String, Object> d(Map<String, Object> ctx);
        boolean keep(Map<String, Object> ctx);
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "how-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    /** A chain long enough that a local run has something to chain. */
    private static FlowSpec linear(ExecutionMode mode) {
        return Wiggle.define("how-linear", Map.class, OneStep.class, (f, s) -> f
                .execution(mode)
                .thenApply(s::a)
                .thenApply(s::b)
                .thenFilter(s::keep)
                .thenApply(s::c)
                .thenApply(s::d));
    }

    @Handlers("how-linear")
    public static final class LinearH {
        final AtomicInteger runs;
        LinearH(AtomicInteger runs) { this.runs = runs; }
        public Map<String, Object> a(Map<String, Object> c) { runs.incrementAndGet(); return put(c, "a", 1L); }
        public Map<String, Object> b(Map<String, Object> c) { runs.incrementAndGet(); return put(c, "b", (Long) c.get("a") + 1); }
        public boolean keep(Map<String, Object> c) { runs.incrementAndGet(); return (Long) c.get("b") > 0; }
        public Map<String, Object> c(Map<String, Object> c) { runs.incrementAndGet(); return put(c, "c", (Long) c.get("b") + 1); }
        public Map<String, Object> d(Map<String, Object> c) { runs.incrementAndGet(); return put(c, "d", (Long) c.get("c") + 1); }
    }

    @Test
    @DisplayName("a worker that only binds handlers runs the flow under SERVER, LOCAL_SYNC and LOCAL_ASYNC")
    void handlerOnlyWorkerRunsUnderEveryMode() throws Exception {
        for (ExecutionMode mode : new ExecutionMode[]{
                ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC}) {
            AtomicInteger runs = new AtomicInteger();
            FlowSpec spec = linear(mode);

            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl())) {

                client.register(spec);   // the author publishes the topology -- once, from the client

                try (Worker w = new Worker(client, "w-" + Ids.next("x"),
                        WorkerOptions.defaults().withConcurrency(4))
                        .handlers(new LinearH(runs))) {      // no register(spec): binds by name
                    w.start();

                    InstanceView v = client.awaitCompletion(client.start(spec, Map.of()),
                            Duration.ofSeconds(20));

                    assertEquals("COMPLETED", v.status(), mode + " status");
                    Map<String, Object> ctx = Json.asObject(v.context());
                    assertEquals(1L, ctx.get("a"), mode + " a");
                    assertEquals(2L, ctx.get("b"), mode + " b");
                    assertEquals(3L, ctx.get("c"), mode + " c");
                    assertEquals(4L, ctx.get("d"), mode + " d");
                    assertEquals(5, runs.get(), mode + " every step ran exactly once");
                }
            }
        }
    }

    @Test
    @DisplayName("a handler-only worker still runs locally: it holds the graph, so no server fallback")
    void localModesDoNotFallBackToServerDriven() throws Exception {
        // Worker.execute takes the local path only when it holds the definition for the task's exact
        // version: graphs.get(workflow + ":" + version) != null. Completing the flow does not prove
        // that -- a worker without the graph completes too, one server round trip per step. So assert
        // the branch condition itself, which is the thing removing Worker.register could have broken.
        for (ExecutionMode mode : new ExecutionMode[]{ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC}) {
            FlowSpec spec = linear(mode);
            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl())) {

                client.register(spec);

                try (Worker w = new Worker(client, "w-" + Ids.next("x"),
                        WorkerOptions.defaults().withConcurrency(4))
                        .handlers(new LinearH(new AtomicInteger()))) {
                    w.start();

                    java.lang.reflect.Field f = Worker.class.getDeclaredField("graphs");
                    f.setAccessible(true);
                    Map<?, ?> graphs = (Map<?, ?>) f.get(w);

                    String key = spec.name() + ":" + spec.version();
                    assertTrue(graphs.containsKey(key),
                            mode + ": the worker must hold " + key + " to run locally, but holds "
                                    + graphs.keySet() + " -- it would fall back to server-driven");
                }
            }
        }
    }
}
