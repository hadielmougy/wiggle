package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
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
 * Name-only handler binding ({@link Worker#handle}): a workflow's topology is registered once by an
 * author, and a worker that never saw the blueprint implements steps by (workflow, step) name. On
 * {@link Worker#start()} the worker reconciles its bindings against the server's registered graph --
 * discovering the queue each step polls and rejecting a mistyped name or wrong kind up front.
 */
class HandleBindingTest {

    private static Map<String, Object> put(Object ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(Json.asObject(ctx));
        n.put(k, v);
        return n;
    }

    /** The authored topology: two of its steps sit on the default queue, "authorise" on "payments". */
    private Blueprint authoredGraph() {
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

    @Test
    @DisplayName("a worker with no blueprint drives a full instance via handlers bound by name")
    void nameOnlyBindingRunsToCompletion() throws Exception {
        withServer((client, server) -> {
            client.register(authoredGraph());   // author registers topology only

            AtomicReference<Object> audited = new AtomicReference<>();
            try (Worker impl = new Worker(client, "impl-1")) {
                impl.handle("order-fulfilment", "validate", c -> put(c, "status", "VALIDATED"))
                    .handleGate("order-fulfilment", "in-stock", c -> ((Number) Json.asObject(c).get("qty")).intValue() > 0)
                    .handle("order-fulfilment", "authorise", c -> put(c, "paymentRef", "auth-" + Json.asObject(c).get("orderId")))
                    .handleEffect("order-fulfilment", "audit", c -> audited.set(Json.asObject(c).get("paymentRef")));
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
    @DisplayName("binding a step name the graph does not have fails fast on start")
    void unknownStepRejected() throws Exception {
        withServer((client, server) -> {
            client.register(authoredGraph());
            try (Worker impl = new Worker(client, "impl-2")) {
                impl.handle("order-fulfilment", "autorise", c -> c);   // typo
                IllegalStateException e = assertThrows(IllegalStateException.class, impl::start);
                assertTrue(e.getMessage().contains("no step 'autorise'"), e.getMessage());
                assertTrue(e.getMessage().contains("available steps"), e.getMessage());
            }
        });
    }

    @Test
    @DisplayName("binding a predicate step as a task (or vice versa) fails fast on start")
    void kindMismatchRejected() throws Exception {
        withServer((client, server) -> {
            client.register(authoredGraph());
            try (Worker impl = new Worker(client, "impl-3")) {
                impl.handle("order-fulfilment", "in-stock", c -> c);   // it's a PREDICATE in the graph
                IllegalStateException e = assertThrows(IllegalStateException.class, impl::start);
                assertTrue(e.getMessage().contains("is a PREDICATE"), e.getMessage());
                assertTrue(e.getMessage().contains("handleGate"), e.getMessage());
            }
        });
    }

    @Test
    @DisplayName("binding to an unregistered workflow fails fast on start")
    void unregisteredWorkflowRejected() throws Exception {
        withServer((client, server) -> {
            try (Worker impl = new Worker(client, "impl-4")) {
                impl.handle("never-registered", "step", c -> c);
                IllegalStateException e = assertThrows(IllegalStateException.class, impl::start);
                assertTrue(e.getMessage().contains("is not registered"), e.getMessage());
            }
        });
    }

    /** A typed context — the same JSON on the wire, so it interoperates with dict/JSON handlers. */
    public record Item(String id, int qty, String state) {}

    @Test
    @DisplayName("typed handlers bound by name (record codec) run an instance to completion")
    void typedNameOnlyBinding() throws Exception {
        withServer((client, server) -> {
            client.register(Workflow.define("typed-wf")
                    .step("check")
                    .gate("available", Item.class, i -> i.qty() > 0)
                    .effect("done")
                    .build());

            AtomicReference<String> doneState = new AtomicReference<>();
            try (Worker impl = new Worker(client, "typed-impl")) {
                impl.handle("typed-wf", "check", Item.class, i -> new Item(i.id(), i.qty(), "CHECKED"))
                    .handleGate("typed-wf", "available", Item.class, i -> i.qty() > 0)
                    .handleEffect("typed-wf", "done", Item.class, i -> doneState.set(i.state()));
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
