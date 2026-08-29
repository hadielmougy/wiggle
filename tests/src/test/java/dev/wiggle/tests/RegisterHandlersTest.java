package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Object-based handler binding ({@link Worker#registerHandlers}): hand the worker an object whose
 * methods are the steps. Each method is matched to a step by case-insensitive name and its kind is
 * taken from the signature (Map = task, boolean = gate, void = effect); the graph confirms the exact
 * name and gate-vs-task.
 */
class RegisterHandlersTest {

    private static Map<String, Object> put(Object ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(Json.asObject(ctx));
        n.put(k, v);
        return n;
    }

    /** The authored topology: "authorise" sits on the "payments" queue, the rest on the default. */
    private Blueprint<Map<String, Object>> authoredGraph() {
        return Workflow.define("order-fulfilment")
                .step("validate", c -> put(c, "status", "VALIDATED"))
                .gate("in-stock", c -> ((Number) c.get("qty")).intValue() > 0)
                .step("authorise", c -> put(c, "paymentRef", "auth-" + c.get("orderId")), "payments")
                .effect("audit", c -> { /* side effect only */ })
                .build();
    }

    private void withServer(BiConsumer<WiggleClient, WiggleServer> body) throws Exception {
        ServerConfig config = new ServerConfig(0, "test-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            body.accept(client, server);
        }
    }

    /** Methods in mixed case styles; the return type picks the kind. "inStock" matches step "in-stock". */
    public static final class OrderHandlers {
        final AtomicReference<Object> audited;
        OrderHandlers(AtomicReference<Object> audited) { this.audited = audited; }

        public Map<String, Object> validate(Map<String, Object> c) { return put(c, "status", "VALIDATED"); }
        public boolean inStock(Map<String, Object> c) { return ((Number) c.get("qty")).intValue() > 0; }
        public Map<String, Object> authorise(Map<String, Object> c) { return put(c, "paymentRef", "auth-" + c.get("orderId")); }
        public void audit(Map<String, Object> c) { audited.set(c.get("paymentRef")); }
        public String describe() { return "not a handler (two-return-shape is ignored)"; } // ignored: no context param
    }

    @Test
    @DisplayName("registerHandlers matches methods to steps by name and kind, and runs to completion")
    void objectBindingRunsToCompletion() throws Exception {
        withServer((client, server) -> {
            client.register(authoredGraph());   // author registers topology only

            AtomicReference<Object> audited = new AtomicReference<>();
            try (Worker impl = new Worker(client, "obj-1")) {
                impl.registerHandlers("order-fulfilment", new OrderHandlers(audited));
                impl.start();   // reconciles: matches inStock->in-stock, discovers the payments queue

                String id = client.start("order-fulfilment", Map.of("orderId", "o1", "qty", 2));
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));

                assertEquals("COMPLETED", v.status(), "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                assertEquals("auth-o1", ctx.get("paymentRef"), "authorise ran (on the discovered payments queue)");
                assertEquals("VALIDATED", ctx.get("status"), "validate ran");
                assertEquals("auth-o1", audited.get(), "audit effect saw the final context");
            }
        });
    }

    public static final class DupHandlers {
        public boolean inStock(Map<String, Object> c) { return true; }
        public boolean in_stock(Map<String, Object> c) { return false; }  // folds to the same step name
    }

    @Test
    @DisplayName("two methods that collide under case-folding are rejected at registerHandlers")
    void caseFoldDuplicateRejected() {
        Worker w = new Worker((WiggleClient) null, "obj-dup");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> w.registerHandlers("order-fulfilment", new DupHandlers()));
        assertTrue(e.getMessage().contains("same step name 'instock'"), e.getMessage());
    }

    public static final class NoHandlers {
        public int add(int a, int b) { return a + b; }   // not a handler shape
    }

    @Test
    @DisplayName("an object with no handler-shaped method is rejected at registerHandlers")
    void noHandlerMethodsRejected() {
        Worker w = new Worker((WiggleClient) null, "obj-none");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> w.registerHandlers("order-fulfilment", new NoHandlers()));
        assertTrue(e.getMessage().contains("no handler methods"), e.getMessage());
    }

    public static final class StrayHandlers {
        public Map<String, Object> validate(Map<String, Object> c) { return c; }
        public Map<String, Object> shipItNow(Map<String, Object> c) { return c; }   // no such step
    }

    @Test
    @DisplayName("a method matching no step fails fast on start")
    void methodMatchingNoStepRejected() throws Exception {
        withServer((client, server) -> {
            client.register(authoredGraph());
            try (Worker impl = new Worker(client, "obj-stray")) {
                impl.registerHandlers("order-fulfilment", new StrayHandlers());
                IllegalStateException e = assertThrows(IllegalStateException.class, impl::start);
                assertTrue(e.getMessage().contains("shipItNow"), e.getMessage());
                assertTrue(e.getMessage().contains("matches no step"), e.getMessage());
            }
        });
    }

    public static final class ClashHandlers {
        // returns a Map (task) but the graph's "in-stock" is a gate (PREDICATE)
        public Map<String, Object> inStock(Map<String, Object> c) { return c; }
    }

    @Test
    @DisplayName("a method whose signature clashes with the graph's kind fails fast on start")
    void kindClashRejected() throws Exception {
        withServer((client, server) -> {
            client.register(authoredGraph());
            try (Worker impl = new Worker(client, "obj-clash")) {
                impl.registerHandlers("order-fulfilment", new ClashHandlers());
                IllegalStateException e = assertThrows(IllegalStateException.class, impl::start);
                assertTrue(e.getMessage().contains("is a PREDICATE"), e.getMessage());
            }
        });
    }
}
