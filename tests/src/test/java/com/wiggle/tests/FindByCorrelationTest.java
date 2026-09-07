package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.dist.WiggleStorageFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Business-key lookup: instances started with a {@code correlationId} can be found by it (the indexed
 * {@code findByCorrelation} query), across the in-memory and JDBC stores.
 */
class FindByCorrelationTest {

    @Handlers("corr")
    static final class H {
        public Map<String, Object> work(Map<String, Object> ctx) { return ctx; }
    }

    private static Blueprint wf() {
        return Workflow.define("corr").step("work").build();
    }

    private static ServerConfig config(String jdbcUrl) {
        return new ServerConfig(0, "corr-node", jdbcUrl, jdbcUrl == null ? null : "sa",
                jdbcUrl == null ? null : "", 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private void run(String jdbcUrl) throws Exception {
        try (WiggleServer server = new WiggleServer(config(jdbcUrl), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "corr-w").register(wf()).handlers(new H())) {
            w.start();
            client.register(wf());

            // 3 instances for cust-A, 1 for cust-B, 1 with no correlation id.
            for (int i = 0; i < 3; i++) client.start("corr", Map.of("i", i), null, "cust-A");
            String bId = client.start("corr", Map.of(), null, "cust-B");
            client.start("corr", Map.of());   // no correlation id

            String tag = jdbcUrl == null ? "in-memory" : "jdbc";
            assertEquals(3, client.findByCorrelation("cust-A").size(), tag + ": cust-A count");

            List<InstanceView> b = client.findByCorrelation("cust-B");
            assertEquals(1, b.size(), tag + ": cust-B count");
            assertEquals(bId, b.get(0).id(), tag + ": cust-B is the right instance");

            assertEquals(0, client.findByCorrelation("nobody").size(), tag + ": unknown key -> empty");
            assertEquals(2, client.findByCorrelation("cust-A", 2).size(), tag + ": limit respected");
        }
    }

    @Test @DisplayName("findByCorrelation returns the instances for a business key (in-memory store)")
    void inMemory() throws Exception {
        run(null);
    }

    @Test @DisplayName("findByCorrelation returns the instances for a business key (JDBC/H2, indexed)")
    void jdbc() throws Exception {
        run("jdbc:h2:mem:corr-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
    }
}
