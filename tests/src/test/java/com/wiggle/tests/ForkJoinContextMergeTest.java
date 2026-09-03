package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
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

/**
 * Two parallel branches that each set a DISTINCT context field (like OrderFulfilment's payment vs
 * shipping). After the join both fields must be present -- a branch must not clobber its sibling's
 * write. Run across many concurrent instances on a shared JDBC store (3 engines), matching the kind
 * cluster's contention.
 */
class ForkJoinContextMergeTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static Blueprint blueprint() {
        return Workflow.define("merge-check")
                .step("validate")
                .fork(
                        Branch.of("payment", s -> s
                                .step("authorise")),
                        Branch.of("shipping", s -> s
                                .sleep("await", Duration.ofMillis(50))
                                .step("label")))
                .combine("merge")
                .step("notify")
                .build();
    }

    @Handlers("merge-check")
    static final class MergeH {
        public Map<String, Object> validate(Map<String, Object> ctx) { return put(ctx, "validated", true); }
        public Map<String, Object> authorise(Map<String, Object> ctx) { return put(ctx, "payment", "auth"); }
        public Map<String, Object> label(Map<String, Object> ctx) { return put(ctx, "tracking", "DHL"); }
        public Map<String, Object> notify(Map<String, Object> ctx) { return put(ctx, "done", true); }
    }

    @Test @DisplayName("both parallel branches' fields survive the join (no sibling clobber)")
    void bothBranchFieldsSurvive() throws Exception {
        String url = "jdbc:h2:mem:merge-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        Blueprint bp = blueprint();

        List<WiggleServer> servers = new ArrayList<>();
        List<WiggleClient> clients = new ArrayList<>();
        List<Worker> workers = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                ServerConfig config = new ServerConfig(0, "node-" + i, url, "sa", "", 8,
                        Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                        Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
                WiggleServer server = new WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
                servers.add(server);
                WiggleClient client = new WiggleClient(server.baseUrl());
                clients.add(client);
                Worker w = new Worker(client, "w-" + i,
                        WorkerOptions.defaults().withConcurrency(8).withLongPollWait(Duration.ofMillis(250)));
                w.register(bp).handlers(new MergeH());
                workers.add(w.start());
            }

            WiggleClient submitter = clients.get(0);
            int n = 40;
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < n; i++) ids.add(submitter.start(bp, Map.of("id", "A-" + i)));

            List<String> bad = new ArrayList<>();
            for (String id : ids) {
                InstanceView v = submitter.awaitCompletion(id, Duration.ofSeconds(30));
                Map<String, Object> ctx = asMap(v.context());
                if (!"auth".equals(ctx.get("payment")) || !"DHL".equals(ctx.get("tracking"))) {
                    bad.add(id + " payment=" + ctx.get("payment") + " tracking=" + ctx.get("tracking"));
                }
            }
            assertEquals(List.of(), bad, "every instance must keep BOTH branch fields; offenders: " + bad);
        } finally {
            for (Worker w : workers) w.close();
            for (WiggleClient c : clients) c.close();
            for (WiggleServer s : servers) s.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : Map.of();
    }

    // ---- the OrderFulfilment pattern: a typed record via a VersionedContextCodec ----

    public record Parcel(String id, String payment, String tracking) {
        Parcel withPayment(String p) { return new Parcel(id, p, tracking); }
        Parcel withTracking(String t) { return new Parcel(id, payment, t); }
    }

    private static Blueprint typedBlueprint() {
        return Workflow.define("parcel-merge")
                .step("validate")
                .fork(
                        Branch.of("payment", s -> s.step("authorise")),
                        Branch.of("shipping", s -> s
                                .sleep("await", Duration.ofMillis(50))
                                .step("label")))
                .combine("merge")
                .step("notify")
                .build();
    }

    @Handlers("parcel-merge")
    static final class ParcelH {
        public Parcel validate(Parcel p) { return p; }
        public Parcel authorise(Parcel p) { return p.withPayment("auth"); }
        public Parcel label(Parcel p) { return p.withTracking("DHL"); }
        public Parcel notify(Parcel p) { return p; }
    }

    @Test @DisplayName("typed-record branches (codec round-trip) keep both fields")
    void typedBothBranchFieldsSurvive() throws Exception {
        String url = "jdbc:h2:mem:merge2-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        Blueprint bp = typedBlueprint();

        List<WiggleServer> servers = new ArrayList<>();
        List<WiggleClient> clients = new ArrayList<>();
        List<Worker> workers = new ArrayList<>();
        try {
            for (int i = 0; i < 3; i++) {
                ServerConfig config = new ServerConfig(0, "node-" + i, url, "sa", "", 8,
                        Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                        Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
                WiggleServer server = new WiggleServer(config, new com.wiggle.dist.WiggleStorageFactory()).start();
                servers.add(server);
                WiggleClient client = new WiggleClient(server.baseUrl());
                clients.add(client);
                Worker w = new Worker(client, "w-" + i,
                        WorkerOptions.defaults().withConcurrency(8).withLongPollWait(Duration.ofMillis(250)));
                w.register(bp).handlers(new ParcelH());
                workers.add(w.start());
            }

            WiggleClient submitter = clients.get(0);
            int n = 40;
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < n; i++) ids.add(submitter.start(bp, new Parcel("A-" + i, null, null)));

            List<String> bad = new ArrayList<>();
            for (String id : ids) {
                InstanceView v = submitter.awaitCompletion(id, Duration.ofSeconds(30));
                Map<String, Object> ctx = asMap(v.context());
                if (!"auth".equals(ctx.get("payment")) || !"DHL".equals(ctx.get("tracking"))) {
                    bad.add(id + " payment=" + ctx.get("payment") + " tracking=" + ctx.get("tracking"));
                }
            }
            assertEquals(List.of(), bad, "typed branches must keep BOTH fields; offenders: " + bad);
        } finally {
            for (Worker w : workers) w.close();
            for (WiggleClient c : clients) c.close();
            for (WiggleServer s : servers) s.close();
        }
    }
}
