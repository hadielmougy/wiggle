package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.TokenInfo;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.RetryPolicy;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Each step's input, output, retries and timing reach the instance detail, whichever mode ran it
 * and whichever store kept it.
 */
class StepRecordingTest {

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        boolean ok(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
    }

    public static final class H {
        final AtomicInteger triesOfB = new AtomicInteger();

        public Map<String, Object> a(Map<String, Object> ctx) {
            return put(ctx, "a", 1L);
        }

        public boolean ok(Map<String, Object> ctx) {
            return true;
        }

        public Map<String, Object> b(Map<String, Object> ctx) {
            if (triesOfB.incrementAndGet() == 1) throw new IllegalStateException("flaky once");
            return put(ctx, "b", 2L);
        }
    }

    static Stream<Arguments> modesAndStores() {
        return Stream.of("SERVER", "LOCAL_SYNC", "LOCAL_ASYNC")
                .flatMap(mode -> Stream.of(Arguments.of(mode, "memory"), Arguments.of(mode, "h2")));
    }

    @ParameterizedTest(name = "{0} on {1}")
    @MethodSource("modesAndStores")
    @DisplayName("the instance detail carries each step's input, output, retries and timing")
    void stepsAreRecorded(String mode, String store) throws Exception {
        String name = "step-io-" + mode.toLowerCase() + "-" + store;
        FlowSpec spec = FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) -> (switch (mode) {
            case "SERVER" -> f.executeInServer();
            case "LOCAL_SYNC" -> f.executeInLocalSync();
            default -> f.executeInLocalAsync();
        }).thenApply(s::a).thenFilter(s::ok).thenApply(s::b, RetryPolicy.fixed(3, Duration.ofMillis(10))));

        try (WiggleServer server = server(store);
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "w-" + Ids.next("x")).registerHandler(name, new H())) {
            client.register(spec);
            worker.start();
            String id = client.start(spec, Map.of("n", 1L));
            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            assertEquals("COMPLETED", v.status());

            List<TokenInfo> tokens = client.instanceDetail(id).tokens();
            TokenInfo a = step(tokens, "a");
            assertEquals(Map.of("n", 1L), a.input());
            assertEquals(Map.of("n", 1L, "a", 1L), a.output());
            assertEquals(0, a.attempt());

            TokenInfo ok = step(tokens, "ok");
            assertEquals(Map.of("n", 1L, "a", 1L), ok.input());
            assertEquals(true, ok.output(), "a gate's output is its branch");

            TokenInfo b = step(tokens, "b");
            assertEquals(1, b.attempt(), "one failed try before it succeeded");
            assertTrue(b.lastError().contains("flaky once"), b.lastError());
            assertEquals(Map.of("n", 1L, "a", 1L), b.input());
            assertEquals(Map.of("n", 1L, "a", 1L, "b", 2L), b.output());

            for (TokenInfo t : List.of(a, ok, b)) {
                assertNotNull(t.startedAt(), t.activity() + " was timed");
                assertNotNull(t.finishedAt(), t.activity() + " was timed");
                assertTrue(t.finishedAt() >= t.startedAt(), t.activity() + " finished after it started");
                assertTrue(t.createdAt() > 0, t.activity() + " carries its creation time");
            }
            TokenInfo end = tokens.stream().filter(t -> t.kind().equals("END")).findFirst().orElseThrow();
            assertNull(end.input(), "a node no handler ran records nothing");
        }
    }

    private static TokenInfo step(List<TokenInfo> tokens, String activity) {
        return tokens.stream().filter(t -> t.activity() != null && t.activity().endsWith("#" + activity)).findFirst()
                .orElseThrow(() -> new AssertionError("no token for " + activity + " in " + tokens));
    }

    private static WiggleServer server(String store) throws java.io.IOException {
        String url = store.equals("h2")
                ? "jdbc:h2:mem:step-io-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1"
                : null;
        ServerConfig config = new ServerConfig(0, "step-io", url, url == null ? null : "sa", url == null ? null : "", 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        return url == null ? new WiggleServer(config).start()
                : new WiggleServer(config, new WiggleStorageFactory()).start();
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }
}
