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
 * A step that finishes while the server is down is delivered once the server returns, as long as
 * the worker's lease has not run out -- the step does not re-run. Past the lease the worker gives
 * up and the reclaimed task re-runs, the same as a crash.
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
        return new WiggleServer(new ServerConfig(port, "settle-outage", jdbcUrl, "sa", "", 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
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

    @Test @Timeout(60)
    @DisplayName("an outage outlasting the lease falls back to reclaim: the step re-runs and the flow completes")
    void outageBeyondLeaseReRuns() throws Exception {
        String name = "settle-outage-expired";
        FlowSpec spec = flow(name, "SERVER");
        H h = new H();
        WiggleServer server = startServer();
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
            server = startServer();
            System.setProperty("wiggle.rpc.maxAttempts", "60");

            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());
            assertEquals(2, h.runsOfA.get(), "the undelivered step re-ran after its lease expired");
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
