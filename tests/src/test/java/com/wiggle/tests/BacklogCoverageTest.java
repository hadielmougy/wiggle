package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The coverage view: which dispatchable work no running worker can claim.
 *
 * <p>This is the one failure the rest of the console cannot show. A token whose queue nobody polls --
 * or whose version every worker has scoped itself out of -- sits READY forever. It is not failed, not
 * retried, not late in any way the engine can see; the instance reads RUNNING and the token reads
 * READY, which is precisely what a healthy system looks like a moment before a worker takes it.
 *
 * <p>Runs against whatever {@link TestStorage} points at -- H2 by default, a real PostgreSQL when
 * {@code WIGGLE_TEST_DB_URL} is set. Worth both: the grouping is a {@code GROUP BY} over the token
 * table, and H2 in PostgreSQL mode is not PostgreSQL. A live store keeps its rows between runs, so
 * each test cancels its workflow's leftovers before asserting on counts.
 */
class BacklogCoverageTest {

    /** The step this spec names; a worker binds it by name. */
    interface OneStep {
        Map<String, Object> served(Map<String, Object> ctx);
        Map<String, Object> extra(Map<String, Object> ctx);
        Map<String, Object> orphan(Map<String, Object> ctx);
    }

    private static final String QUEUES_WF = "bc-queues";
    private static final String VERSION_WF = "bc-version";
    private static final String UNSCOPED_WF = "bc-unscoped";

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "bc-node", TestStorage.url("backlog"), TestStorage.user(),
                TestStorage.password(), 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @ForFlow(QUEUES_WF)
    public static final class QueueSteps {
        public Map<String, Object> served(Map<String, Object> c) { return put(c, "served", true); }
        public Map<String, Object> orphan(Map<String, Object> c) { return put(c, "orphan", true); }
    }

    @ForFlow(VERSION_WF)
    public static final class VersionSteps {
        public Map<String, Object> served(Map<String, Object> c) { return put(c, "served", true); }
        public Map<String, Object> extra(Map<String, Object> c) { return put(c, "extra", true); }
    }

    @ForFlow(UNSCOPED_WF)
    public static final class UnscopedSteps {
        public Map<String, Object> served(Map<String, Object> c) { return put(c, "served", true); }
        public Map<String, Object> extra(Map<String, Object> c) { return put(c, "extra", true); }
    }

    @Test
    @DisplayName("a queue nobody polls shows as uncovered, with what is stranded on it")
    void aQueueNobodyPollsIsReportedUncovered() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            clear(client, QUEUES_WF);
            FlowSpec spec = FlowSpec.define(QUEUES_WF, Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::served)
                .onQueue(QUEUES_WF + "-served")
                .thenApply(s::orphan)
                .onQueue(QUEUES_WF + "-orphan"));
            client.register(spec);

            // a worker that serves ONLY the first queue -- nothing will ever claim "orphan"
            try (Worker w = new Worker(client, "served-" + Ids.next("x"),
                    WorkerOptions.defaults().withConcurrency(2).withQueues(QUEUES_WF + "-served"))
                    .registerHandler(new QueueSteps())) {
                w.start();

                String id = client.start(spec, Map.of());

                waitUntil(() -> slice(client, QUEUES_WF, QUEUES_WF + "-orphan") != null);
                WiggleClient.BacklogSlice orphan = slice(client, QUEUES_WF, QUEUES_WF + "-orphan");

                assertFalse(orphan.covered(), "nothing polls " + orphan.queue() + ", so it is uncovered");
                assertEquals(1, orphan.readyCount(), "one token stranded on it");
                assertTrue(orphan.oldestAvailableAt() > 0, "and it reports how long it has waited");
                assertEquals(spec.version(), orphan.version(), "attributed to the version that produced it");

                assertEquals("RUNNING", client.instance(id).status(),
                        "which is the point: the instance still looks perfectly healthy");

                WiggleClient.BacklogSlice served = slice(client, QUEUES_WF, QUEUES_WF + "-served");
                assertTrue(served == null || served.covered(),
                        "while the queue that IS polled is either drained or covered");
            }
        }
    }

    @Test
    @DisplayName("a version every worker scoped out of is uncovered; starting its worker covers it")
    void anUnservedVersionIsReportedUncovered() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            clear(client, VERSION_WF);
            FlowSpec v1 = FlowSpec.define(VERSION_WF, Map.class, OneStep.class, (f, s) -> f.thenApply(s::served));
            FlowSpec v2 = FlowSpec.define(VERSION_WF, Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::served)
                .thenApply(s::extra));
            client.register(v1);
            client.register(v2);

            try (Worker onlyV1 = new Worker(client, "v1-" + Ids.next("x"),
                    WorkerOptions.defaults().withConcurrency(2))
                    .registerHandler(new VersionSteps(), v1.version())) {
                onlyV1.start();

                String stranded = client.start(v2, Map.of());   // nothing serves v2

                waitUntil(() -> versionSlice(client, VERSION_WF, v2.version()) != null);
                assertFalse(versionSlice(client, VERSION_WF, v2.version()).covered(),
                        "the only worker is scoped to v" + v1.version()
                                + ", so v" + v2.version() + " has no cover");

                // stand up the worker that was missing: the same work becomes claimable and drains
                try (Worker v2Worker = new Worker(client, "v2-" + Ids.next("x"),
                        WorkerOptions.defaults().withConcurrency(2))
                        .registerHandler(new VersionSteps(), v2.version())) {
                    v2Worker.start();

                    waitUntil(() -> {
                        WiggleClient.BacklogSlice s = versionSlice(client, VERSION_WF, v2.version());
                        return s == null || s.covered();
                    });
                    // coverage flips the moment the worker polls, which is before the work drains --
                    // so wait for the instance itself, not the flag
                    assertEquals("COMPLETED",
                            client.awaitCompletion(stranded, Duration.ofSeconds(20)).status(),
                            "and the stranded instance completes once its version has a worker");
                }
            }
        }
    }

    @Test
    @DisplayName("an unscoped worker covers every version of the workflow it binds")
    void anUnscopedWorkerCoversEveryVersion() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new com.wiggle.dist.WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            clear(client, UNSCOPED_WF);
            FlowSpec v1 = FlowSpec.define(UNSCOPED_WF, Map.class, OneStep.class, (f, s) -> f.thenApply(s::served));
            FlowSpec v2 = FlowSpec.define(UNSCOPED_WF, Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::served)
                .thenApply(s::extra));
            client.register(v1);
            client.register(v2);

            try (Worker w = new Worker(client, "any-" + Ids.next("x"),
                    WorkerOptions.defaults().withConcurrency(2))
                    .registerHandler(new UnscopedSteps())) {      // unscoped: serves both
                w.start();

                assertEquals("COMPLETED", client.awaitCompletion(
                        client.start(v1, Map.of()), Duration.ofSeconds(20)).status());
                assertEquals("COMPLETED", client.awaitCompletion(
                        client.start(v2, Map.of()), Duration.ofSeconds(20)).status());

                assertTrue(mine(client, UNSCOPED_WF).stream().allMatch(WiggleClient.BacklogSlice::covered),
                        "an unscoped worker serves every version, so none of its backlog is uncovered");
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    /** Cancels leftovers: a live database keeps its rows, and these tests assert on counts. */
    private static void clear(WiggleClient client, String workflow) {
        for (InstanceView v : client.listInstances(workflow, "RUNNING", 200)) {
            try { client.cancel(v.id(), "backlog coverage test cleanup"); } catch (RuntimeException ignored) { }
        }
    }

    private static List<WiggleClient.BacklogSlice> mine(WiggleClient client, String workflow) {
        List<WiggleClient.BacklogSlice> out = new ArrayList<>();
        for (WiggleClient.BacklogSlice s : client.backlogCoverage(500)) {
            if (workflow.equals(s.workflow())) out.add(s);
        }
        return out;
    }

    private static WiggleClient.BacklogSlice slice(WiggleClient client, String workflow, String queue) {
        return mine(client, workflow).stream()
                .filter(s -> queue.equals(s.queue())).findFirst().orElse(null);
    }

    private static WiggleClient.BacklogSlice versionSlice(WiggleClient client, String workflow, int version) {
        return mine(client, workflow).stream()
                .filter(s -> s.version() == version).findFirst().orElse(null);
    }

    private static void waitUntil(java.util.function.BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(150);
        }
        throw new AssertionError("condition not met within 20s");
    }
}
