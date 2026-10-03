package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A step that finishes while the server is down is delivered once the server returns -- the step
 * does not re-run. Within the lease the worker keeps retrying; past it the result is held and
 * resent, and the server spares leases that lapsed before it took leadership for its reconnect
 * grace (the default lease). A held result arriving after that grace is refused and the step re-runs.
 */
class SettleOutageTest {

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
    }

    public static final class H {
        final AtomicInteger runsOfA = new AtomicInteger();
        final AtomicInteger runsOfB = new AtomicInteger();
        final CountDownLatch aStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        public Map<String, Object> a(Map<String, Object> ctx) {
            runsOfA.incrementAndGet();
            aStarted.countDown();
            try {
                if (!release.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("not released");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return put(ctx, "a", 1L);
        }

        public Map<String, Object> b(Map<String, Object> ctx) {
            runsOfB.incrementAndGet();
            return put(ctx, "b", 2L);
        }
    }

    private int port;
    private String jdbcUrl;

    @BeforeEach void setUp() {
        port = TestPorts.free();
        jdbcUrl = "jdbc:h2:mem:settle-outage-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        System.setProperty("wiggle.rpc.maxAttempts", "2");
        System.setProperty("wiggle.rpc.retryDelayMillis", "50");
    }

    @AfterEach void clear() {
        System.clearProperty("wiggle.rpc.maxAttempts");
        System.clearProperty("wiggle.rpc.retryDelayMillis");
    }

    private WiggleServer startServer() throws java.io.IOException {
        return startServer(Duration.ofSeconds(20));
    }

    private WiggleServer startServer(Duration defaultLease) throws java.io.IOException {
        return new WiggleServer(new ServerConfig(port, "settle-outage", jdbcUrl, "sa", "", 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, defaultLease,
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10)), new WiggleStorageFactory()).start();
    }

    private static FlowSpec flow(String name, String mode) {
        return FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) -> (switch (mode) {
            case "SERVER" -> f.executeInServer();
            case "LOCAL_SYNC" -> f.executeInLocalSync();
            default -> f.executeInLocalAsync();
        }).thenApply(s::a).thenApply(s::b));
    }

    @ParameterizedTest(name = "{0}") @Timeout(60)
    @ValueSource(strings = {"SERVER", "LOCAL_SYNC", "LOCAL_ASYNC"})
    @DisplayName("a step finished during an outage is delivered when the server returns, not re-run")
    void deliveredAfterOutage(String mode) throws Exception {
        String name = "settle-outage-" + mode.toLowerCase();
        FlowSpec spec = flow(name, mode);
        H h = new H();
        WiggleServer server = startServer();
        try (WiggleClient client = new WiggleClient("localhost:" + port);
             Worker worker = new Worker(client, "w-" + Ids.next("x"),
                     WorkerOptions.defaults().withLease(Duration.ofSeconds(20)))
                     .registerHandler(name, h)) {
            client.register(spec);
            worker.start();
            String id = client.start(spec, Map.of());
            assertTrue(h.aStarted.await(10, TimeUnit.SECONDS), "step 'a' started");

            server.close();
            h.release.countDown();
            Thread.sleep(1_500);
            server = startServer();
            System.setProperty("wiggle.rpc.maxAttempts", "60");

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(15));
            assertEquals("COMPLETED", v.status());
            assertEquals(2L, Json.asObject(v.context()).get("b"));
            assertEquals(1, h.runsOfA.get(), "'a' was delivered after the outage, not re-run");
            assertEquals(1, h.runsOfB.get());
        } finally {
            server.close();
        }
    }

    @ParameterizedTest(name = "{0}") @Timeout(60)
    @ValueSource(strings = {"SERVER", "LOCAL_SYNC", "LOCAL_ASYNC"})
    @DisplayName("a result held past the lease is delivered once the server returns within its reconnect grace")
    void heldPastLeaseDelivered(String mode) throws Exception {
        String name = "settle-held-" + mode.toLowerCase();
        FlowSpec spec = flow(name, mode);
        H h = new H();
        WiggleServer server = startServer(Duration.ofSeconds(20));
        try (WiggleClient client = new WiggleClient("localhost:" + port);
             Worker worker = new Worker(client, "w-" + Ids.next("x"),
                     WorkerOptions.defaults().withLease(Duration.ofSeconds(1)))
                     .registerHandler(name, h)) {
            client.register(spec);
            worker.start();
            String id = client.start(spec, Map.of());
            assertTrue(h.aStarted.await(10, TimeUnit.SECONDS), "step 'a' started");

            server.close();
            h.release.countDown();
            Thread.sleep(3_000);
            server = startServer(Duration.ofSeconds(20));
            System.setProperty("wiggle.rpc.maxAttempts", "60");

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            assertEquals(1, h.runsOfA.get(), "the held result was delivered, not re-run");
            assertEquals(1, h.runsOfB.get());
        } finally {
            server.close();
        }
    }

    @Test @Timeout(60)
    @DisplayName("a held result that arrives after the reconnect grace is refused: the step re-runs and the flow completes")
    void heldPastGraceReRuns() throws Exception {
        String name = "settle-held-late";
        FlowSpec spec = flow(name, "SERVER");
        H h = new H();
        WiggleServer server = startServer(Duration.ofMillis(500));
        WorkerOptions slowResend = new WorkerOptions(4, Duration.ofSeconds(1), Duration.ofSeconds(1),
                Duration.ofMillis(200), Duration.ofSeconds(5), 64, java.util.Set.of(), Duration.ZERO, false);
        try (WiggleClient client = new WiggleClient("localhost:" + port);
             Worker worker = new Worker(client, "w-" + Ids.next("x"), slowResend).registerHandler(name, h)) {
            client.register(spec);
            worker.start();
            String id = client.start(spec, Map.of());
            assertTrue(h.aStarted.await(10, TimeUnit.SECONDS), "step 'a' started");

            server.close();
            h.release.countDown();
            Thread.sleep(2_000);
            server = startServer(Duration.ofMillis(500));
            System.setProperty("wiggle.rpc.maxAttempts", "60");

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));
            assertEquals("COMPLETED", v.status());
            assertEquals(2, h.runsOfA.get(), "the late result was refused and the reclaimed step re-ran");
            assertEquals(1, h.runsOfB.get());
        } finally {
            server.close();
        }
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }
}
