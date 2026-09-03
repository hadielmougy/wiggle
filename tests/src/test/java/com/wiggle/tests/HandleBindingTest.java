package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.RecordMapper;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
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
 * Handler binding via {@link Handlers @Handlers} + {@link Worker#handlers(Object)}: a workflow's
 * topology is registered once by an author, and a worker implements the steps with an annotated
 * class whose method names match the steps and whose signatures define the kind (one param = input,
 * {@code boolean} = gate, {@code void} = effect). On {@link Worker#start()} the worker reconciles its
 * handlers against the server's registered graph -- discovering the queue each step polls and
 * rejecting a signature that clashes with a node's kind up front.
 */
class HandleBindingTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    /** The authored topology: two of its steps sit on the default queue, "authorise" on "payments". */
    private Blueprint authoredGraph() {
        return Workflow.define("order-fulfilment")
                .step("validate")
                .gate("in-stock")
                .step("authorise", "payments")
                .effect("audit")
                .build();
    }

    /** The step logic, bound to {@code order-fulfilment} by name; signatures define each step's kind. */
    @Handlers("order-fulfilment")
    static final class OrderImpl {
        final AtomicReference<Object> audited;
        OrderImpl(AtomicReference<Object> audited) { this.audited = audited; }
        public Map<String, Object> validate(Map<String, Object> c) { return put(c, "status", "VALIDATED"); }
        public boolean inStock(Map<String, Object> c) { return ((Number) c.get("qty")).intValue() > 0; }
        public Map<String, Object> authorise(Map<String, Object> c) { return put(c, "paymentRef", "auth-" + c.get("orderId")); }
        public void audit(Map<String, Object> c) { audited.set(c.get("paymentRef")); }
    }

    /** Binds a gate step (in the graph a PREDICATE) with a task signature -- a kind clash. */
    @Handlers("order-fulfilment")
    static final class BadKindImpl {
        public Map<String, Object> inStock(Map<String, Object> c) { return c; }   // it's a PREDICATE in the graph
    }

    /** Handlers for a workflow that was never registered on the server. */
    @Handlers("never-registered")
    static final class OrphanImpl {
        public Map<String, Object> step(Map<String, Object> c) { return c; }
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

    @Test
    @DisplayName("a worker with no blueprint drives a full instance via @Handlers bound by name")
    void handlersBindingRunsToCompletion() throws Exception {
        withServer((client, server) -> {
            client.register(authoredGraph());   // author registers topology only

            AtomicReference<Object> audited = new AtomicReference<>();
            try (Worker impl = new Worker(client, "impl-1")) {
                impl.handlers(new OrderImpl(audited));   // binds by name, no blueprint seen
                impl.start();   // reconciles: validates names/kinds, discovers the "payments" queue too

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

    @Test
    @DisplayName("a handler whose signature clashes with the node kind fails fast on start")
    void kindMismatchRejected() throws Exception {
        withServer((client, server) -> {
            client.register(authoredGraph());
            try (Worker impl = new Worker(client, "impl-3")) {
                impl.handlers(new BadKindImpl());   // inStock is a PREDICATE, bound as a task
                IllegalStateException e = assertThrows(IllegalStateException.class, impl::start);
                assertTrue(e.getMessage().contains("in-stock"), e.getMessage());
                assertTrue(e.getMessage().contains("boolean"), e.getMessage());
            }
        });
    }

    @Test
    @DisplayName("binding to an unregistered workflow fails fast on start")
    void unregisteredWorkflowRejected() throws Exception {
        withServer((client, server) -> {
            try (Worker impl = new Worker(client, "impl-4")) {
                impl.handlers(new OrphanImpl());
                IllegalStateException e = assertThrows(IllegalStateException.class, impl::start);
                assertTrue(e.getMessage().contains("is not registered"), e.getMessage());
            }
        });
    }

    /** A typed context -- the same JSON on the wire, so it interoperates with dict/JSON handlers. */
    public record Item(String id, int qty, String state) {}

    /** Typed step logic: each method takes and returns the {@link Item} record (decoded via the codec). */
    @Handlers("typed-wf")
    static final class TypedImpl {
        final AtomicReference<String> doneState;
        TypedImpl(AtomicReference<String> doneState) { this.doneState = doneState; }
        public Item check(Item i) { return new Item(i.id(), i.qty(), "CHECKED"); }
        public boolean available(Item i) { return i.qty() > 0; }
        public void done(Item i) { doneState.set(i.state()); }
    }

    @Test
    @DisplayName("typed handlers bound by name (record codec) run an instance to completion")
    void typedHandlersBinding() throws Exception {
        withServer((client, server) -> {
            client.register(Workflow.define("typed-wf")
                    .step("check")
                    .gate("available")
                    .effect("done")
                    .build());

            AtomicReference<String> doneState = new AtomicReference<>();
            try (Worker impl = new Worker(client, "typed-impl")) {
                impl.handlers(new TypedImpl(doneState));
                impl.start();

                String id = client.start("typed-wf", Map.of("id", "x1", "qty", 3));
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));

                assertEquals("COMPLETED", v.status(), "status");
                Item out = (Item) RecordMapper.fromJson(v.context(), Item.class);
                assertEquals("CHECKED", out.state(), "typed handler updated the record");
                assertEquals("CHECKED", doneState.get(), "typed effect saw the decoded record");
            }
        });
    }
}
