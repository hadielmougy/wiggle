package com.wiggle.server.search;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.SearchResult;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Tls;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.tests.TestPorts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Search through a running server: the indexer feeds documents from the event log, a search finds
 * instances by what their context says, only within what the caller may read, and a purged instance
 * stays findable, marked purged.
 */
class SearchEndToEndTest {

    interface Steps { Map<String, Object> work(Map<String, Object> ctx); }

    private static final FlowSpec ORDERS = FlowSpec.define("orders", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    private static final FlowSpec BILLING = FlowSpec.define("billing", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));

    private static ServerConfig config(boolean search, ServerConfig.GrpcAuth auth) {
        return new ServerConfig(TestPorts.free(), "search-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10))
                .withSearch(new ServerConfig.Search(search, Duration.ofDays(30), Set.of()))
                .withAuth(new ServerConfig.Auth(auth, Duration.ofSeconds(30)));
    }

    private static <T> T await(Supplier<T> read, java.util.function.Predicate<T> done) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (true) {
            T v = read.get();
            if (done.test(v)) return v;
            if (System.currentTimeMillis() > deadline) throw new AssertionError("timed out; last: " + v);
            Thread.sleep(100);
        }
    }

    private static List<String> ids(SearchResult r) {
        return r.hits().stream().map(WiggleClient.SearchHit::instanceId).toList();
    }

    @Test @DisplayName("an instance is found by the words in its context, its correlation id, and its filters")
    void findsByContext() throws Exception {
        try (WiggleServer server = new WiggleServer(config(true, ServerConfig.GrpcAuth.OFF)).start();
             WiggleClient c = new WiggleClient(server.baseUrl(), Tls.Options.DISABLED, false, null)) {
            c.register(ORDERS);
            c.register(BILLING);
            String ada = c.start("orders", Map.of("customer", "Ada Lovelace", "city", "London"), null, "order-1001");
            String alan = c.start("orders", Map.of("customer", "Alan Turing", "city", "London"), null, "order-1002");
            String bill = c.start("billing", Map.of("customer", "Ada Lovelace"), null, null);

            SearchResult r = await(() -> c.search("lovelace", null, null, null, null, 10, false), x -> x.hits().size() == 2);
            assertEquals(Set.of(ada, bill), Set.copyOf(ids(r)));
            assertEquals(List.of(ada), ids(c.search("ada london", null, null, null, null, 10, false)), "every word is needed");
            assertEquals(List.of(bill), ids(c.search("lovelace", "billing", null, null, null, 10, false)));
            assertEquals(List.of(alan), ids(c.search("order 1002", null, null, null, null, 10, false)),
                    "the correlation id is searchable");
            assertEquals(2, c.search("london", "orders", "RUNNING", null, null, 10, false).hits().size());

            c.cancel(alan, "customer called");
            await(() -> c.search("london", "orders", "CANCELLED", null, null, 10, false), x -> x.hits().size() == 1);
            assertEquals(List.of(alan), ids(c.search("customer called", null, null, null, null, 10, false)),
                    "the termination reason is searchable once the cancellation is indexed");

            server.engine().purgeTerminalInstancesOlderThan(0, 100);
            WiggleClient.SearchHit purged = c.search("turing", null, null, null, null, 10, false).hits().getFirst();
            assertTrue(purged.purged(), "a purged instance is still found, and shown as purged");
            assertFalse(c.search("lovelace", "orders", null, null, null, 10, false).hits().getFirst().purged());
        }
    }

    @Test @DisplayName("a caller sees only hits in the workflows its role may read")
    void scopedToReadableWorkflows() throws Exception {
        try (WiggleServer server = new WiggleServer(config(true, ServerConfig.GrpcAuth.ENFORCE)).start()) {
            server.accounts().putRole("ops", "orders-reader", List.of("read:orders"), true);
            String adminKey = server.accounts().createApiKey("ops", "root", "admin", null);
            String ordersKey = server.accounts().createApiKey("ops", "orders", "orders-reader", null);
            try (WiggleClient admin = new WiggleClient(server.baseUrl(), Tls.Options.DISABLED, false, adminKey);
                 WiggleClient orders = new WiggleClient(server.baseUrl(), Tls.Options.DISABLED, false, ordersKey)) {
                admin.register(ORDERS);
                admin.register(BILLING);
                String order = admin.start("orders", Map.of("note", "rhubarb"), null, null);
                admin.start("billing", Map.of("note", "rhubarb"), null, null);
                await(() -> admin.search("rhubarb", null, null, null, null, 10, false), x -> x.hits().size() == 2);

                assertEquals(List.of(order), ids(orders.search("rhubarb", null, null, null, null, 10, false)));
                assertEquals(List.of(), ids(orders.search("rhubarb", "billing", null, null, null, 10, false)),
                        "asking for a workflow it may not read finds nothing");
            }
        }
    }

    @Test @DisplayName("with search off, a search fails as a precondition and nothing is indexed")
    void off() throws Exception {
        try (WiggleServer server = new WiggleServer(config(false, ServerConfig.GrpcAuth.OFF)).start();
             WiggleClient c = new WiggleClient(server.baseUrl(), Tls.Options.DISABLED, false, null)) {
            assertTrue(server.search().isEmpty());
            assertEquals(409, assertThrows(WiggleApiException.class,
                    () -> c.search("x", null, null, null, null, 10, false)).status());
        }
    }
}
