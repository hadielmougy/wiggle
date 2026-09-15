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
 * Runs the program the tutorial builds, against a real server.
 *
 * <p>A tutorial is the one document a reader types out verbatim and expects to work first time, so
 * "it compiles" is not enough: the steps have to bind, the fan-out has to fan out, and the combine
 * has to produce the number the page claims. Compiling {@link Orders} proves the shapes; this proves
 * the run. The pages quote regions of that file, so both tiers cover the same text.
 */
class TutorialTest {

    private static ServerConfig config() {
        String url = "jdbc:h2:mem:tut-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        return new ServerConfig(0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static Orders.Order order(Orders.Item... items) {
        return new Orders.Order("A-1001", List.of(items), BigDecimal.ZERO, "NEW");
    }

    private InstanceView run(Orders.Order input) throws Exception {
        FlowSpec spec = Orders.spec();
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "tut-" + Ids.next("x"))
                     .registerHandler(new OrderHandlers())) {
            client.register(spec);
            w.start();
            return client.awaitCompletion(client.start(spec, input), Duration.ofSeconds(30));
        }
    }

    @Test @DisplayName("the tutorial's flow completes, and the total is the one the page shows")
    void theTutorialRuns() throws Exception {
        InstanceView v = run(order(new Orders.Item("PEN", new BigDecimal("2.50")),
                                   new Orders.Item("PAD", new BigDecimal("4.00"))));

        assertEquals("COMPLETED", v.status());
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("CONFIRMED", ctx.get("status"));
        // 2.50 and 4.00, each +20% VAT in the forEach, summed by the combine
        assertEquals(0, new BigDecimal(String.valueOf(ctx.get("total"))).compareTo(new BigDecimal("7.80")),
                "total was " + ctx.get("total"));
        assertEquals(2, ((List<?>) ctx.get("items")).size(), "the priced items survive the join");
    }

    @Test @DisplayName("the gate ends the instance cleanly -- the tutorial's point about thenFilter")
    void theGateEndsCleanlyRatherThanFailing() throws Exception {
        Orders.Item[] tooMany = new Orders.Item[51];
        for (int i = 0; i < tooMany.length; i++) {
            tooMany[i] = new Orders.Item("SKU-" + i, BigDecimal.ONE);
        }
        InstanceView v = run(order(tooMany));

        assertEquals("COMPLETED", v.status(), "a false gate is not a failure");
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("VALIDATED", ctx.get("status"), "it stopped before confirm, so status never advanced");
        assertTrue(ctx.get("total") == null
                        || new BigDecimal(String.valueOf(ctx.get("total"))).signum() == 0,
                "the fan-out never ran, so nothing was totalled: " + ctx.get("total"));
    }
}
