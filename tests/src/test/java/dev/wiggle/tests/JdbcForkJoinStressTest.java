package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Branch;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.client.worker.WorkerOptions;
import dev.wiggle.client.worker.Step;
import dev.wiggle.core.Ids;
import dev.wiggle.core.RetryPolicy;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.core.InstanceView;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Drives a fork whose branches carry a retry and a sleep, joined across many concurrent
 * instances on the JDBC backend (which the in-memory conformance suite never exercises).
 * The "authorise" step fails its first two tries using the engine-global attempt count, so
 * retries converge no matter which of the three nodes' workers picks up each try -- the
 * regression that made run-workers.sh look like a stall.
 */
class JdbcForkJoinStressTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static Blueprint<Map<String, Object>> blueprint() {
        return Workflow.defineJson("order-ish")
                .step("validate", ctx -> put(ctx, "validated", true))
                .gate("in-stock", ctx -> true)
                .fork(
                        Branch.of("payment", s -> s
                                .step("authorise", ctx -> {
                                    int attempt = Step.attempt();
                                    if (attempt <= 2) throw new IllegalStateException("gateway timeout " + attempt);
                                    return put(ctx, "paid", true);
                                }, RetryPolicy.exponential(5, Duration.ofMillis(50)))
                                .step("capture", ctx -> put(ctx, "captured", true))),
                        Branch.of("shipping", s -> s
                                .step("reserve", ctx -> put(ctx, "reserved", true))
                                .sleep("await", Duration.ofMillis(150))
                                .step("label", ctx -> put(ctx, "labelled", true))))
                .step("notify", ctx -> put(ctx, "fulfilled", true))
                .build();
    }

    @Test @DisplayName("many fork/join instances all complete on the JDBC backend")
    void forkJoinCompletesOnJdbc() throws Exception {
        // One shared database, three server nodes -- as close to the kind cluster as a single
        // JVM gets: real leader election, three engines driving the same store.
        String url = "jdbc:h2:mem:stress-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        Blueprint<Map<String, Object>> bp = blueprint();

        List<WiggleServer> servers = new ArrayList<>();
        List<WiggleClient> clients = new ArrayList<>();
        List<Worker> workers = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                ServerConfig config = new ServerConfig(0, "node-" + i, url, "sa", "", 8,
                        Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                        Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
                WiggleServer server = new WiggleServer(config).start();
                servers.add(server);
                WiggleClient client = new WiggleClient(server.baseUrl());
                clients.add(client);
                Worker w = new Worker(client, "w-" + i,
                        WorkerOptions.defaults().withConcurrency(8).withLongPollWait(Duration.ofMillis(250)));
                w.register(bp);
                workers.add(w.start());
            }

            WiggleClient submitter = clients.get(0);
            int n = 20;
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < n; i++) ids.add(submitter.start(bp, Map.of("id", "A-" + i)));

            int completed = 0;
            List<String> stuck = new ArrayList<>();
            for (String id : ids) {
                InstanceView v = submitter.awaitCompletion(id, Duration.ofSeconds(30));
                if ("COMPLETED".equals(v.status())) completed++;
                else stuck.add(id + "=" + v.status());
            }
            assertEquals(n, completed, "all instances should complete; not-completed: " + stuck);
        } finally {
            for (Worker w : workers) w.close();
            for (WiggleClient c : clients) c.close();
            for (WiggleServer s : servers) s.close();
        }
    }
}
