package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.tutorial.OrderHandlers;
import com.wiggle.tutorial.Orders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs each of the tutorials' shapes against a real server.
 *
 * <p>A tutorial is the one document a reader types out verbatim and expects to work first time, so
 * "it compiles" is not enough: the steps have to bind, the fan-out has to fan out, and the combine
 * has to produce the number the page prints. The pages quote regions of {@code com.wiggle.tutorial},
 * so both tiers cover the same text.
 *
 * <p>What is and is not covered. The Java is covered exactly -- the same classes the pages show. The
 * container and compose fragments are not executed here; they are configuration, and testing them
 * would mean running Docker from a unit test. Where a tutorial's own {@code main} would reach a
 * process that only exists on the reader's machine (a server on :8080), the
 * test stands an equivalent up in-process and drives the same client code.
 */
class TutorialTest {

    private static ServerConfig config() {
        String url = "jdbc:h2:mem:tut-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        return new ServerConfig(0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static Orders.Order twoItems() {
        return new Orders.Order("A-1001",
                List.of(new Orders.Item("PEN", new BigDecimal("2.50")),
                        new Orders.Item("PAD", new BigDecimal("4.00"))),
                BigDecimal.ZERO, "NEW");
    }

    /** 2.50 and 4.00, each +20% VAT in its own branch, summed by the combine. */
    private static void assertPriced(InstanceView v) {
        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("CONFIRMED", ctx.get("status"));
        assertEquals(0, new BigDecimal(String.valueOf(ctx.get("total"))).compareTo(new BigDecimal("7.80")),
                "total was " + ctx.get("total"));
        assertEquals(2, ((List<?>) ctx.get("items")).size(), "the priced items survive the join");
    }


    @Test @DisplayName("tutorial 1 -- server embedded in the app's own JVM, on a database")
    void embedded() throws Exception {
        FlowSpec orders = Orders.spec();
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            client.register(orders);   // before the worker binds, exactly as the page says
            try (Worker worker = new Worker(client, "tut-" + Ids.next("x"))
                    .registerHandler(new OrderHandlers()).start()) {
                assertPriced(client.awaitCompletion(client.start(orders, twoItems()),
                        Duration.ofSeconds(30)));
            }
        }
    }


    @Test @DisplayName("tutorial 2 -- a separate server process; submitter and worker are clients")
    void standalone() throws Exception {
        // stands in for the container the page runs: same client code either way
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start()) {

            String id;
            try (WiggleClient submitter = new WiggleClient(server.baseUrl())) {
                FlowSpec orders = Orders.spec();
                submitter.register(orders);                   // the submitter owns no handlers
                id = submitter.start(orders, twoItems());
            }

            try (WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker worker = new Worker(client, "tut-" + Ids.next("x"))
                         .registerHandler(new OrderHandlers()).start()) {   // the worker owns no flow
                assertPriced(client.awaitCompletion(id, Duration.ofSeconds(30)));
            }
        }
    }


    @Test @DisplayName("the gate ends the instance cleanly -- the point every page makes about thenFilter")
    void theGateEndsCleanlyRatherThanFailing() throws Exception {
        Orders.Item[] tooMany = new Orders.Item[51];
        for (int i = 0; i < tooMany.length; i++) {
            tooMany[i] = new Orders.Item("SKU-" + i, BigDecimal.ONE);
        }
        FlowSpec orders = Orders.spec();
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            client.register(orders);
            try (Worker worker = new Worker(client, "tut-" + Ids.next("x"))
                    .registerHandler(new OrderHandlers()).start()) {
            InstanceView v = client.awaitCompletion(client.start(orders,
                    new Orders.Order("A-2002", List.of(tooMany), BigDecimal.ZERO, "NEW")),
                    Duration.ofSeconds(30));

            assertEquals("COMPLETED", v.status(), "a false gate is not a failure");
            Map<String, Object> ctx = Json.asObject(v.context());
            assertEquals("VALIDATED", ctx.get("status"), "it stopped before confirm");
            assertTrue(ctx.get("total") == null
                            || new BigDecimal(String.valueOf(ctx.get("total"))).signum() == 0,
                    "the fan-out never ran: " + ctx.get("total"));
            }
        }
    }
}
