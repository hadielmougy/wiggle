package com.wiggle.order;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.observe.ObservedFlow;
import com.wiggle.observe.Observer;
import com.wiggle.observe.ObserverOptions;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;

import java.util.Map;
import java.util.Random;

/**
 * A single-JVM playground for observed execution: starts a server, publishes a checkout flow as
 * OBSERVED, and reports sixty runs the way two services would -- by run key, step name and
 * times -- with a few departures from the topology mixed in. Then it idles so you can explore
 * the result in the ops console's Performance tab.
 *
 * <pre>./gradlew :example:seedObserved                                       # terminal 1
 * WIGGLE_URL=localhost:8080 ./gradlew :console:run   ->   http://localhost:8090   # terminal 2</pre>
 *
 * <p>What to look for: {@code reserve} is the slowest step (its p95 rings red on the diagram),
 * and the anomaly list shows an out-of-order run, a run that ended before END, a run whose step
 * threw, and a duplicated step. Every run is judged a few seconds after its last report.
 *
 * <p>Config comes from the environment ({@link ServerConfig#fromEnvironment()}), so this runs
 * in-memory by default or against a database with {@code WIGGLE_JDBC_URL}.
 */
public final class ObservedSeed {

    interface CheckoutSteps {
        Map<String, Object> validate(Map<String, Object> o);
        Map<String, Object> reserve(Map<String, Object> o);
        boolean inStock(Map<String, Object> o);
        Map<String, Object> charge(Map<String, Object> o);
        Map<String, Object> notify(Map<String, Object> o);
    }

    public static void main(String[] args) throws Exception {
        // Judge a run two seconds after it reaches END (default: five), and call a quiet run
        // stalled after thirty seconds (default: ten minutes), so the playground reacts quickly.
        System.setProperty("wiggle.observe.settleMillis", "2000");
        System.setProperty("wiggle.observe.stallMillis", "30000");
        FlowSpec checkout = FlowSpec.define("checkout", 1, Map.class, CheckoutSteps.class, (f, s) -> f
                .thenApply(s::validate)
                .thenApply(s::reserve)
                .thenFilter(s::inStock)
                .thenApply(s::charge)
                .thenApply(s::notify));

        try (WiggleServer server = new WiggleServer(ServerConfig.fromEnvironment()).start();
             Observer gateway = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("gateway"));
             Observer warehouse = Observer.connect(server.baseUrl(), ObserverOptions.defaults().withReporter("warehouse"))) {
            ObservedFlow front = gateway.publish(checkout);
            ObservedFlow back = warehouse.publish(checkout);
            Random rnd = new Random(7);
            long now = System.currentTimeMillis();

            for (int i = 0; i < 60; i++) {
                String key = "order-" + (1000 + i);
                long t = now - rnd.nextInt(600_000);               // spread over the last ten minutes
                long validate = 5 + rnd.nextInt(10), reserve = 40 + rnd.nextInt(200);
                long charge = 120 + rnd.nextInt(80), notify = 10 + rnd.nextInt(20);
                // the gateway validates and charges; the warehouse reserves and decides stock
                front.record(key, "validate", t, t += validate);
                back.record(key, "reserve", t, t += reserve);
                boolean stocked = i % 15 != 7;                     // one in fifteen is out of stock
                back.recordPredicate(key, "inStock", stocked, t, t += 1);
                if (!stocked) continue;                            // the filter's false branch ends the run
                if (i == 20) {                                     // a payment gateway outage
                    front.recordError(key, "charge", "GatewayTimeout: no response in 30s", t, t + 30_000);
                    continue;
                }
                if (i == 33) front.record(key, "charge", t, t + charge);   // charged twice: at-least-once delivery
                front.record(key, "charge", t, t += charge);
                if (i == 41) continue;                             // notify never ran: the originator gives up
                front.record(key, "notify", t, t += notify);
            }
            // a run reported out of order: notify before charge
            long t = now - 5_000;
            front.record("order-2000", "validate", t, t += 8);
            back.record("order-2000", "reserve", t, t += 90);
            back.recordPredicate("order-2000", "inStock", true, t, t += 1);
            front.record("order-2000", "notify", t, t += 15);
            front.record("order-2000", "charge", t, t += 150);
            // a run its originator ended before finishing
            front.record("order-2001", "validate", now - 3_000, now - 2_990);
            front.end("order-2001");

            System.out.println("\nSixty-two checkout runs reported to the server at " + server.baseUrl() + ".");
            System.out.println("Runs are judged a couple of seconds after their last report; the quiet one stalls after 30s.");
            System.out.println("Explore them in the ops console (a separate process):");
            System.out.println("    WIGGLE_URL=" + server.baseUrl() + " ./gradlew :console:run   ->   http://localhost:8090");
            System.out.println("Performance tab: pick 'checkout'. Instances tab: search correlation id 'order-2000'.");
            System.out.println("Press Ctrl-C to stop.\n");
            Thread.currentThread().join();
        }
    }
}
