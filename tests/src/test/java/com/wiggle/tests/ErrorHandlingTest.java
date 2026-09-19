package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.RetryPolicy;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The claims {@code docs/error-handling.md} makes, as tests.
 *
 * <p>The page's whole point is that three things which look like "it stopped" are different events
 * — a retried failure, an immediate one, and a clean stop that is not a failure at all — so the
 * tests count handler attempts rather than only reading the final status. A page that got the retry
 * count wrong would still look right if all you checked was FAILED.
 */
class ErrorHandlingTest {

    private static ServerConfig config() {
        String url = "jdbc:h2:mem:err-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        return new ServerConfig(0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    interface Steps {
        Map<String, Object> work(Map<String, Object> c);
    }

    interface GateSteps {
        Map<String, Object> before(Map<String, Object> c);
        boolean gate(Map<String, Object> c);
        Map<String, Object> after(Map<String, Object> c);
    }

    /** Counts its attempts, and fails the way each test asks for. */
    @ForFlow("err")
    static class Counting implements Steps {
        final AtomicInteger attempts = new AtomicInteger();
        private final RuntimeException how;

        Counting(RuntimeException how) { this.how = how; }

        @Override public Map<String, Object> work(Map<String, Object> c) {
            attempts.incrementAndGet();
            if (how != null) throw how;
            return c;
        }
    }

    private InstanceView run(FlowSpec spec, Object handlers, Map<String, Object> input) throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec);
            try (Worker w = new Worker(client, "err-" + Ids.next("x")).registerHandler(handlers).start()) {
                return client.awaitCompletion(client.start(spec, input), Duration.ofSeconds(30));
            }
        }
    }

    private static FlowSpec oneStep(String name, RetryPolicy retry) {
        return FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) ->
                retry == null ? f.thenApply(s::work) : f.thenApply(s::work, retry));
    }

    @Test @DisplayName("an ordinary exception is retried to the policy's limit, then the instance FAILS")
    void ordinaryExceptionRetries() throws Exception {
        Counting h = new Counting(new IllegalStateException("gateway timeout"));
        InstanceView v = run(oneStep("err", RetryPolicy.fixed(3, Duration.ofMillis(10))), h, Map.of());

        assertEquals("FAILED", v.status());
        assertEquals(3, h.attempts.get(), "fixed(3) means three attempts, not one and not four");
        assertTrue(String.valueOf(v.error()).contains("gateway timeout"), v.error());
    }

    @Test @DisplayName("PermanentActivityException skips the retries entirely: one attempt, then FAILED")
    void permanentSkipsRetries() throws Exception {
        // the SAME policy as above -- the difference is the exception, not the configuration
        Counting h = new Counting(new PermanentActivityException("card declined"));
        InstanceView v = run(oneStep("err", RetryPolicy.fixed(3, Duration.ofMillis(10))), h, Map.of());

        assertEquals("FAILED", v.status());
        assertEquals(1, h.attempts.get(),
                "a permanent failure must not consume the step's retry budget");
        assertTrue(String.valueOf(v.error()).contains("card declined"), v.error());
    }

    @Test @DisplayName("RetryPolicy.none() is the declarative form: one attempt for any exception")
    void noneMeansOneAttempt() throws Exception {
        Counting h = new Counting(new IllegalStateException("nope"));
        InstanceView v = run(oneStep("err", RetryPolicy.none()), h, Map.of());

        assertEquals("FAILED", v.status());
        assertEquals(1, h.attempts.get(), "none() caps the step, whatever it throws");
    }

    @Test @DisplayName("a handler that succeeds after a transient failure completes -- retries are the point")
    void retrySucceeds() throws Exception {
        AtomicInteger n = new AtomicInteger();
        @ForFlow("err")
        class Flaky implements Steps {
            @Override public Map<String, Object> work(Map<String, Object> c) {
                if (n.incrementAndGet() < 3) throw new IllegalStateException("blip " + n.get());
                return Map.of("ok", true);
            }
        }
        InstanceView v = run(oneStep("err", RetryPolicy.fixed(5, Duration.ofMillis(10))), new Flaky(), Map.of());

        assertEquals("COMPLETED", v.status());
        assertEquals(3, n.get(), "it failed twice and succeeded on the third attempt");
        assertEquals(true, Json.asObject(v.context()).get("ok"));
    }

    @Test @DisplayName("a false gate COMPLETES the instance -- it is not a failure and raises no alarm")
    void falseGateCompletes() throws Exception {
        @ForFlow("gated")
        class H implements GateSteps {
            @Override public Map<String, Object> before(Map<String, Object> c) { return Map.of("reached", "before"); }
            @Override public boolean gate(Map<String, Object> c) { return false; }
            @Override public Map<String, Object> after(Map<String, Object> c) { return Map.of("reached", "after"); }
        }
        FlowSpec spec = FlowSpec.define("gated", 1, Map.class, GateSteps.class, (f, s) -> f
                .thenApply(s::before).thenFilter(s::gate).thenApply(s::after));

        InstanceView v = run(spec, new H(), Map.of());

        assertEquals("COMPLETED", v.status(), "a false gate is a clean stop, not a FAILED instance");
        assertEquals("before", Json.asObject(v.context()).get("reached"),
                "the steps after the gate never ran");
        assertTrue(v.error() == null || v.error().isBlank(), "and nothing was recorded as an error: " + v.error());
    }

    @Test @DisplayName("cancel stops a running instance from outside, with the reason kept")
    void cancelFromOutside() throws Exception {
        interface Slow { Map<String, Object> park(Map<String, Object> c); }
        @ForFlow("cancelme")
        class H implements Slow {
            @Override public Map<String, Object> park(Map<String, Object> c) { return c; }
        }
        // a server-side timer holds the instance open without holding a worker
        FlowSpec spec = FlowSpec.define("cancelme", 1, Map.class, Slow.class, (f, s) ->
                f.thenSleep("hold", Duration.ofSeconds(20)).thenApply(s::park));

        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec);
            try (Worker w = new Worker(client, "err-" + Ids.next("x")).registerHandler(new H()).start()) {
                String id = client.start(spec, Map.of());
                Thread.sleep(300);
                assertEquals("RUNNING", client.instance(id).status());

                client.cancel(id, "customer withdrew the order");

                InstanceView v = client.instance(id);
                assertEquals("CANCELLED", v.status());
                assertTrue(String.valueOf(v.terminationReason()).contains("customer withdrew")
                                || String.valueOf(v.error()).contains("customer withdrew"),
                        "the reason should survive: " + v);
            }
        }
    }
}
