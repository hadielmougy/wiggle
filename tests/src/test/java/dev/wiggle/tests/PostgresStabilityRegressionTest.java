package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Branch;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.client.worker.WorkerOptions;
import dev.wiggle.core.Ids;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.core.Tls;
import dev.wiggle.dist.WiggleStorageFactory;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression: a real server on a real database (PostgreSQL) with load-shedding <b>enabled</b>. It
 * proves the bounded gRPC pool + shedding path doesn't break normal execution -- linear and
 * fork/join workflows still complete, and a burst of instances all finish even when the small
 * handler pool makes the server shed and hand workers hold-offs (shedding only delays polls; no
 * work is lost). Opt-in:
 *
 * <pre>
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "dev.wiggle.tests.PostgresStabilityRegressionTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class PostgresStabilityRegressionTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    /** A real Postgres URL, with load-shedding on and a deliberately small handler pool. */
    private static ServerConfig config() {
        ServerConfig.Stability stability = new ServerConfig.Stability(
                true, /*threads*/ 4, /*high*/ 4, /*low*/ 1,
                Duration.ofMillis(50), Duration.ofMillis(150), Duration.ofMillis(100));
        return new ServerConfig(0, "pg-stab-node",
                System.getenv("WIGGLE_TEST_PG_URL"), System.getenv("WIGGLE_TEST_PG_USER"),
                System.getenv("WIGGLE_TEST_PG_PASSWORD"), 16,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(800), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10), "admin", null, Tls.Options.DISABLED, stability);
    }

    @Test @DisplayName("linear + fork/join workflows complete on Postgres with shedding enabled")
    void workflowsComplete() throws Exception {
        Blueprint<Map<String, Object>> linear = Workflow.defineJson("pg-stab-linear-" + Ids.next("wf"))
                .step("a", ctx -> put(ctx, "a", 1L))
                .step("b", ctx -> put(ctx, "b", 2L))
                .build();
        Blueprint<Map<String, Object>> forkJoin = Workflow.defineJson("pg-stab-fork-" + Ids.next("wf"))
                .fork(
                        Branch.of("left", b -> b.step("l", ctx -> put(ctx, "left", true))),
                        Branch.of("right", b -> b.step("r", ctx -> put(ctx, "right", true))))
                .step("after", ctx -> put(ctx, "joined", true))
                .build();

        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "pg-stab-w").register(linear).register(forkJoin)) {
            w.start();

            InstanceView lv = client.awaitCompletion(client.start(linear, Map.of()), Duration.ofSeconds(30));
            assertEquals("COMPLETED", lv.status());
            assertEquals(2L, Json.asObject(lv.context()).get("b"));

            InstanceView fv = client.awaitCompletion(client.start(forkJoin, Map.of()), Duration.ofSeconds(30));
            assertEquals("COMPLETED", fv.status());
            Map<String, Object> ctx = Json.asObject(fv.context());
            assertEquals(true, ctx.get("left"));
            assertEquals(true, ctx.get("right"));
            assertEquals(true, ctx.get("joined"));
        }
    }

    @Test @DisplayName("a burst of instances all complete on Postgres even while the server sheds")
    void burstCompletesUnderShedding() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.defineJson("pg-stab-burst-" + Ids.next("wf"))
                .step("s1", ctx -> put(ctx, "s1", true))
                .step("s2", ctx -> put(ctx, "s2", true))
                .build();
        int instances = 60;
        // More worker concurrency than handler threads (4), so polls queue and the server sheds --
        // yet every instance must still complete.
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "pg-stab-burst-w",
                     WorkerOptions.defaults().withConcurrency(16)).register(bp)) {
            w.start();
            java.util.List<String> ids = new java.util.ArrayList<>();
            for (int i = 0; i < instances; i++) ids.add(client.start(bp, Map.of()));
            for (String id : ids) {
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(60));
                assertEquals("COMPLETED", v.status(), "instance " + id + " completed despite shedding");
            }
        }
    }
}
