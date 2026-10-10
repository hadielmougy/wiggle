package com.wiggle.relay;

import com.wiggle.client.WiggleClient;
import com.wiggle.core.InstanceView;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Registers the relay and starts {@code count} shipments: {@code RelayStarter [count] [host:port]}.
 * Waits for every instance to finish and checks each final context field by field against the
 * shipment after all six stages. Exits non-zero unless every instance completed and matched.
 */
public final class RelayStarter {

    private RelayStarter() {}

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 200;
        String target = args.length > 1 ? args[1] : "127.0.0.1:18600";
        String run = Long.toString(System.currentTimeMillis(), 36);

        try (WiggleClient client = new WiggleClient(target);
             ExecutorService pool = Executors.newFixedThreadPool(32)) {
            client.register(Relay.flowSpec());
            System.out.println("[starter] registered " + Relay.NAME + " v" + Relay.VERSION + " on " + target
                    + "; stages " + String.join(" -> ", Relay.STAGES));

            long t0 = System.nanoTime();
            List<Future<String[]>> started = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                String id = "shp-" + run + "-" + i;
                started.add(pool.submit(() -> new String[] {id,
                        client.start(Relay.NAME, Relay.json(Shipment.seed(id)), Relay.VERSION, id)}));
            }
            List<String[]> instances = new ArrayList<>();
            for (Future<String[]> f : started) instances.add(f.get());
            System.out.printf("[starter] started %d shipments (run %s) in %d ms%n",
                    count, run, (System.nanoTime() - t0) / 1_000_000);

            List<Future<String>> outcomes = new ArrayList<>();
            for (String[] pair : instances) outcomes.add(pool.submit(() -> verify(client, pair[0], pair[1])));
            Map<String, Integer> tally = new TreeMap<>();
            List<String> problems = new ArrayList<>();
            for (Future<String> f : outcomes) {
                String outcome = f.get();
                String kind = outcome.startsWith("OK") ? "OK" : outcome.substring(0, outcome.indexOf(' '));
                tally.merge(kind, 1, Integer::sum);
                if (!kind.equals("OK")) problems.add(outcome);
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;

            problems.stream().limit(20).forEach(p -> System.out.println("[starter] " + p));
            System.out.printf("[starter] %d shipments finished in %d ms (%.1f/s): %s%n",
                    count, ms, count * 1000.0 / Math.max(ms, 1), tally);
            System.exit(problems.isEmpty() ? 0 : 1);
        }
    }

    private static String verify(WiggleClient client, String id, String instanceId) {
        InstanceView v;
        try {
            v = client.awaitCompletion(instanceId, Duration.ofMinutes(5));
        } catch (IllegalStateException e) {
            return "TIMEOUT " + id + ": " + e.getMessage();
        }
        if (!"COMPLETED".equals(v.status())) {
            return v.status() + " " + id + " (" + instanceId + "): " + v.error();
        }
        Relay.Counter fields = new Relay.Counter();
        List<String> diff = Relay.diff(Relay.json(Relay.after(id, Relay.STAGES.size())), v.context(), fields);
        return diff.isEmpty()
                ? "OK " + id
                : "MISMATCH " + id + " (" + instanceId + ") final context differs in " + diff.size()
                        + " of " + fields.leaves + " fields: " + diff;
    }
}
