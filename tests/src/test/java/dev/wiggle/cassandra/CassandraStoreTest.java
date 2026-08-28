package dev.wiggle.cassandra;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Branch;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.Ids;
import dev.wiggle.core.InstanceView;
import dev.wiggle.core.Json;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Cassandra store against a real cluster (e.g. the {@code cassandra:5.0} image), driven both
 * end-to-end (a real server + worker) and directly (the exactly-once claim). Exercises the
 * partition-local LWT-batch commit, the dispatch-index + per-token-LWT claim, the timer sweep, and
 * a fork/join workflow (several token rows co-located in one instance partition). Opt-in:
 *
 * <pre>
 *   WIGGLE_TEST_CASSANDRA_URL="cassandra://127.0.0.1:9042/wiggle?dc=datacenter1&rf=1" \
 *     ./gradlew :tests:test --tests "dev.wiggle.cassandra.CassandraStoreTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_CASSANDRA_URL", matches = ".+")
class CassandraStoreTest {

    private static String url() { return System.getenv("WIGGLE_TEST_CASSANDRA_URL"); }

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(0, "cass-node", url(), null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private InstanceView run(Blueprint<Map<String, Object>> bp, Map<String, Object> input) throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new dev.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "cass-" + Ids.next("x")).register(bp)) {
            w.start();
            return client.awaitCompletion(client.start(bp, input), Duration.ofSeconds(30));
        }
    }

    @Test @DisplayName("a linear workflow runs to completion end-to-end on Cassandra")
    void linearWorkflow() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.define("cass-linear-" + Ids.next("wf"))
                .step("a", ctx -> put(ctx, "a", 1L))
                .step("b", ctx -> put(ctx, "b", 2L))
                .step("c", ctx -> put(ctx, "c", 3L))
                .build();
        InstanceView v = run(bp, Map.of());
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(1L, ctx.get("a"));
        assertEquals(2L, ctx.get("b"));
        assertEquals(3L, ctx.get("c"));
    }

    @Test @DisplayName("a fork/join workflow completes (many token rows in one instance partition)")
    void forkJoin() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.define("cass-fork-" + Ids.next("wf"))
                .fork(
                        Branch.of("left-branch", b -> b.step("left", ctx -> put(ctx, "left", true))),
                        Branch.of("right-branch", b -> b.step("right", ctx -> put(ctx, "right", true))))
                .step("after", ctx -> put(ctx, "joined", true))
                .build();
        InstanceView v = run(bp, Map.of());
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(true, ctx.get("left"));
        assertEquals(true, ctx.get("right"));
        assertEquals(true, ctx.get("joined"));
    }

    @Test @DisplayName("a sleep timer fires and the workflow completes (timer index + sweep)")
    void sleepTimer() throws Exception {
        Blueprint<Map<String, Object>> bp = Workflow.define("cass-sleep-" + Ids.next("wf"))
                .step("before", ctx -> put(ctx, "before", true))
                .sleep(Duration.ofMillis(300))
                .step("after", ctx -> put(ctx, "after", true))
                .build();
        InstanceView v = run(bp, Map.of());
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(true, ctx.get("before"));
        assertEquals(true, ctx.get("after"));
    }

    @Test @DisplayName("a sub-workflow runs and resumes the parent (uncontended cross-partition case)")
    void subWorkflow() throws Exception {
        Blueprint<Map<String, Object>> child = Workflow.define("cass-child")
                .step("child-work", ctx -> put(ctx, "childResult", 42L))
                .build();
        Blueprint<Map<String, Object>> parent = Workflow.define("cass-parent")
                .step("prepare", ctx -> put(ctx, "prepared", true))
                .subWorkflow("delegate", "cass-child")
                .step("wrap-up", ctx -> put(ctx, "wrapped", true))
                .build();
        try (WiggleServer server = new WiggleServer(config(), new dev.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "cass-sub-" + Ids.next("x")).register(parent).register(child)) {
            w.start();
            InstanceView v = client.awaitCompletion(client.start(parent, Map.of()), Duration.ofSeconds(30));
            assertEquals("COMPLETED", v.status());
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals(42L, ctx.get("childResult"), "the child's writes merged back into the parent");
            assertEquals(true, ctx.get("wrapped"), "the parent resumed after the child completed");
        }
    }

    @Test @DisplayName("concurrent claimers never receive the same token on Cassandra (per-token LWT)")
    void concurrentClaimsAreExclusive() throws Exception {
        try (CassandraStorage storage = CassandraStorage.fromUrl(url(), null, null)) {
            storage.migrate();
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
            // A unique queue per run keeps this isolated from tokens other tests left in the
            // shared keyspace, so the exactly-once count is over exactly the 40 we create here.
            String queue = "cass-q-" + Ids.next("q");
            Blueprint<Map<String, Object>> bp = Workflow.define("cass-claim-" + Ids.next("wf"))
                    .defaultQueue(queue)
                    .step("work", ctx -> ctx).build();
            engine.register(bp.definition());
            int tokens = 40;
            for (int i = 0; i < tokens; i++) engine.start(bp.name(), bp.version(), Map.of(), null);
            Set<String> queues = Set.of(queue);

            int workers = 5;
            ExecutorService pool = Executors.newFixedThreadPool(workers);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<List<String>>> futures = new java.util.ArrayList<>();
            for (int w = 0; w < workers; w++) {
                String workerId = "w" + w;
                futures.add(pool.submit(() -> {
                    go.await();
                    List<String> mine = new java.util.ArrayList<>();
                    for (int r = 0; r < 20; r++) {
                        var claimed = storage.inTx(tx -> tx.claimTasks(workerId, queues, 5,
                                System.currentTimeMillis(), System.currentTimeMillis() + 30_000));
                        claimed.forEach(t -> mine.add(t.id));
                    }
                    return mine;
                }));
            }
            go.countDown();
            Set<String> all = new HashSet<>();
            int total = 0;
            for (Future<List<String>> f : futures) {
                List<String> mine = f.get();
                total += mine.size();
                all.addAll(mine);
            }
            pool.shutdown();
            assertEquals(total, all.size(), "no token was handed to two workers");
            assertEquals(tokens, all.size(), "every ready token was claimed exactly once");
        }
    }
}
