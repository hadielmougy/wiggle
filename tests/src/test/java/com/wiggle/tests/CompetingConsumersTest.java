package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Competing consumers: two independent workers bind a handler for the <em>same</em> step and poll the
 * <em>same</em> queue. The server hands each token to exactly one of them -- claims are a
 * {@code FOR UPDATE SKIP LOCKED} + lease on the token row, so the two workers load-balance and never
 * both run the same token. There is no server-side handler registry; a step's logic lives per-worker,
 * and the token row is the only arbiter of who runs what.
 */
class CompetingConsumersTest {

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    /** A single-step workflow whose one step both workers will serve. */
    private static Blueprint oneStep() {
        return Workflow.define("competing").step("work").build();
    }

    /**
     * The same handler class, bound on both workers. Each instance is constructed with its owning
     * worker's id and shares the recording structures, so the test can see which worker ran each
     * token and how many times each token ran.
     */
    @Handlers("competing")
    static final class CountingH {
        final String worker;
        final Map<Object, String> ranBy;         // instance key -> the worker that ran it
        final Map<Object, Integer> runsPerKey;   // instance key -> how many times it ran
        final AtomicInteger totalRuns;
        final CyclicBarrier rendezvous;          // null = no rendezvous, just count
        final Duration hold;

        CountingH(String worker, Map<Object, String> ranBy, Map<Object, Integer> runsPerKey,
                  AtomicInteger totalRuns, CyclicBarrier rendezvous, Duration hold) {
            this.worker = worker;
            this.ranBy = ranBy;
            this.runsPerKey = runsPerKey;
            this.totalRuns = totalRuns;
            this.rendezvous = rendezvous;
            this.hold = hold;
        }

        public Map<String, Object> work(Map<String, Object> c) {
            Object key = c.get("n");
            totalRuns.incrementAndGet();
            runsPerKey.merge(key, 1, Integer::sum);
            ranBy.put(key, worker);
            if (rendezvous != null) {
                try {
                    // Only clears if a SECOND worker is holding another token at the same instant.
                    rendezvous.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException("no second consumer showed up at the barrier", e);
                }
            } else if (hold != null) {
                try {
                    Thread.sleep(hold.toMillis());   // hold the token so the sibling worker claims the next one
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return put(c, "ranBy", worker);
        }
    }

    private WiggleServer server() throws Exception {
        ServerConfig config = new ServerConfig(0, "test-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        return new WiggleServer(config).start();
    }

    /** One-at-a-time workers, so a poll claims a single token and the sibling gets the next one. */
    private static WorkerOptions serial() {
        return WorkerOptions.defaults().withConcurrency(1);
    }

    @Test
    @DisplayName("two workers on the same step claim tokens concurrently (a 2-party barrier clears)")
    void twoWorkersConsumeInParallel() throws Exception {
        try (WiggleServer server = server();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(oneStep());

            Map<Object, String> ranBy = new ConcurrentHashMap<>();
            Map<Object, Integer> runsPerKey = new ConcurrentHashMap<>();
            AtomicInteger totalRuns = new AtomicInteger();
            // A barrier of 2 can only clear if two distinct workers each hold a token at the same time;
            // a single worker (concurrency 1) would block here forever and the step would time out.
            CyclicBarrier rendezvous = new CyclicBarrier(2);

            try (Worker a = new Worker(client, "consumer-a", serial());
                 Worker b = new Worker(client, "consumer-b", serial())) {
                a.handlers(new CountingH("consumer-a", ranBy, runsPerKey, totalRuns, rendezvous, null));
                b.handlers(new CountingH("consumer-b", ranBy, runsPerKey, totalRuns, rendezvous, null));
                a.start();
                b.start();

                String i1 = client.start("competing", Map.of("n", 1));
                String i2 = client.start("competing", Map.of("n", 2));
                assertEquals("COMPLETED", client.awaitCompletion(i1, Duration.ofSeconds(20)).status(), "i1");
                assertEquals("COMPLETED", client.awaitCompletion(i2, Duration.ofSeconds(20)).status(), "i2");
            }

            assertEquals(2, totalRuns.get(), "each token ran exactly once");
            assertEquals(2, Set.copyOf(ranBy.values()).size(),
                    "both workers claimed a token concurrently (competing consumers)");
            assertTrue(runsPerKey.values().stream().allMatch(n -> n == 1), "no token ran twice");
        }
    }

    @Test
    @DisplayName("under contention every token runs exactly once and the load is shared")
    void eachTokenRunsExactlyOnce() throws Exception {
        int instances = 12;
        try (WiggleServer server = server();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(oneStep());

            Map<Object, String> ranBy = new ConcurrentHashMap<>();
            Map<Object, Integer> runsPerKey = new ConcurrentHashMap<>();
            AtomicInteger totalRuns = new AtomicInteger();
            Duration hold = Duration.ofMillis(60);   // holds the token long enough for the sibling to grab the next

            try (Worker a = new Worker(client, "consumer-a", serial());
                 Worker b = new Worker(client, "consumer-b", serial())) {
                a.handlers(new CountingH("consumer-a", ranBy, runsPerKey, totalRuns, null, hold));
                b.handlers(new CountingH("consumer-b", ranBy, runsPerKey, totalRuns, null, hold));
                a.start();
                b.start();

                String[] ids = new String[instances];
                for (int i = 0; i < instances; i++) ids[i] = client.start("competing", Map.of("n", i));
                for (int i = 0; i < instances; i++) {
                    assertEquals("COMPLETED", client.awaitCompletion(ids[i], Duration.ofSeconds(30)).status(),
                            "instance " + i);
                }
            }

            assertEquals(instances, totalRuns.get(), "total handler runs == instances: no double dispatch");
            assertEquals(instances, runsPerKey.size(), "every instance's step ran");
            assertTrue(runsPerKey.values().stream().allMatch(n -> n == 1), "no token ran more than once");
            assertEquals(2, Set.copyOf(ranBy.values()).size(), "both workers shared the load");
        }
    }
}
