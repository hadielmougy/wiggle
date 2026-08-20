package dev.wiggle.tests;

import dev.wiggle.client.dsl.*;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.PermanentActivityException;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.client.worker.WorkerOptions;
import dev.wiggle.core.*;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.cluster.ClusterManager;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Behavioural conformance suite for the engine. Each method is self-contained and
 * throws {@link AssertionError} on failure, so it can be driven either by
 * {@link #main} or by the JUnit wrapper in tests/src/test.
 */
public final class Scenarios {

    private Scenarios() {}

    // ------------------------------------------------------------- harness

    private interface Body {
        void run(WiggleServer server, WiggleClient client) throws Exception;
    }

    private static void withServer(Body body) throws Exception {
        ServerConfig config = new ServerConfig(0, "test-node", null, null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100);
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            body.run(server, client);
        }
    }

    private interface ClusterBody {
        void run(List<WiggleServer> servers, WiggleClient client) throws Exception;
    }

    /** Starts {@code n} server nodes sharing one H2 in-memory database -- a real cluster. */
    private static void withCluster(int n, ClusterBody body) throws Exception {
        String url = "jdbc:h2:mem:wiggle-" + Ids.next("db") + ";DB_CLOSE_DELAY=-1;MODE=PostgreSQL";
        List<WiggleServer> servers = new ArrayList<>();
        try {
            for (int i = 0; i < n; i++) {
                ServerConfig config = new ServerConfig(0, "node-" + i, null, url, null, null, 4,
                        Duration.ofMillis(100), Duration.ofMillis(300), 3, Duration.ofSeconds(20),
                        Duration.ofMillis(500), Duration.ofHours(1), 100);
                servers.add(new WiggleServer(config).start());
            }
            try (WiggleClient client = new WiggleClient(servers.get(0).baseUrl())) {
                body.run(servers, client);
            }
        } finally {
            for (WiggleServer s : servers) {
                try { s.close(); } catch (RuntimeException ignored) { }
            }
        }
    }

    private static Worker startWorker(WiggleClient client, Blueprint<?>... blueprints) {
        Worker w = new Worker(client, "w-" + Ids.next("x"),
                WorkerOptions.defaults().withConcurrency(4).withLongPollWait(Duration.ofMillis(250)));
        for (Blueprint<?> bp : blueprints) w.register(bp);
        return w.start();
    }

    private static WorkflowStream<Map<String, Object>> json(String name) {
        return Workflow.defineJson(name);
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String key, Object value) {
        Map<String, Object> next = new LinkedHashMap<>(ctx);
        next.put(key, value);
        return next;
    }

    // ------------------------------------------------------------ scenarios

    /** A linear pipeline runs its steps in order and the context accumulates. */
    public static void sequentialPipeline() throws Exception {
        Blueprint<Map<String, Object>> bp = json("seq")
                .map("one", ctx -> put(ctx, "a", 1L))
                .map("two", ctx -> put(ctx, "b", (Long) ctx.get("a") + 1))
                .map("three", ctx -> put(ctx, "c", (Long) ctx.get("b") + 1))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                Check.equal(ctx.get("a"), 1L, "a");
                Check.equal(ctx.get("b"), 2L, "b");
                Check.equal(ctx.get("c"), 3L, "c");
            }
        });
    }

    /** A false filter ends the instance successfully and skips everything downstream. */
    public static void filterShortCircuits() throws Exception {
        AtomicInteger downstream = new AtomicInteger();
        Blueprint<Map<String, Object>> bp = json("filtered")
                .map("seed", ctx -> put(ctx, "keep", false))
                .filter("gate", ctx -> Boolean.TRUE.equals(ctx.get("keep")))
                .map("never", ctx -> {
                    downstream.incrementAndGet();
                    return put(ctx, "ran", true);
                })
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Check.equal(v.terminationReason(), "filtered:gate", "termination reason");
                Check.equal(downstream.get(), 0, "downstream invocations");
            }
        });
    }

    /** Parallel branches merge field-by-field instead of clobbering each other. */
    public static void forkMergesDisjointWrites() throws Exception {
        Blueprint<Map<String, Object>> bp = json("fork-merge")
                .map("seed", ctx -> put(ctx, "seeded", true))
                .fork(
                        Branch.of("left", s -> s.map("slow-left", ctx -> {
                            Check.sleep(150);           // finishes last on purpose
                            return put(ctx, "left", "L");
                        })),
                        Branch.of("right", s -> s.map("fast-right", ctx -> put(ctx, "right", "R"))))
                .map("after", ctx -> put(ctx, "joined", true))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                Check.equal(ctx.get("left"), "L", "left branch write survived");
                Check.equal(ctx.get("right"), "R", "right branch write survived");
                Check.equal(ctx.get("joined"), true, "post-join step ran");
            }
        });
    }

    /** The step after a fork runs exactly once, no matter how many branches there were. */
    public static void joinRunsContinuationOnce() throws Exception {
        AtomicInteger afterCount = new AtomicInteger();
        Blueprint<Map<String, Object>> bp = json("join-once")
                .fork(
                        Branch.of("a", s -> s.map("a1", ctx -> put(ctx, "a", 1L))),
                        Branch.of("b", s -> s.map("b1", ctx -> put(ctx, "b", 1L))),
                        Branch.of("c", s -> s.map("c1", ctx -> put(ctx, "c", 1L))))
                .map("after", ctx -> {
                    afterCount.incrementAndGet();
                    return ctx;
                })
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Check.sleep(300);
                Check.equal(afterCount.get(), 1, "continuation invocations");
            }
        });
    }

    /** Forks nest: the join stack pops back to the right barrier. */
    public static void nestedForks() throws Exception {
        Blueprint<Map<String, Object>> bp = json("nested")
                .fork(
                        Branch.of("outer-left", s -> s.fork(
                                Branch.of("inner-a", t -> t.map("ia", ctx -> put(ctx, "ia", 1L))),
                                Branch.of("inner-b", t -> t.map("ib", ctx -> put(ctx, "ib", 1L))))
                                .map("inner-after", ctx -> put(ctx, "innerAfter", 1L))),
                        Branch.of("outer-right", s -> s.map("or", ctx -> put(ctx, "or", 1L))))
                .map("outer-after", ctx -> put(ctx, "outerAfter", 1L))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                for (String k : List.of("ia", "ib", "innerAfter", "or", "outerAfter")) {
                    Check.equal(ctx.get(k), 1L, "context key " + k);
                }
            }
        });
    }

    /** A filter inside a branch short-circuits that branch only; siblings still join. */
    public static void filterInsideBranchDoesNotStrandSiblings() throws Exception {
        Blueprint<Map<String, Object>> bp = json("branch-filter")
                .fork(
                        Branch.of("gated", s -> s
                                .filter("gate", ctx -> false)
                                .map("skipped", ctx -> put(ctx, "skipped", true))),
                        Branch.of("other", s -> s.map("ran", ctx -> put(ctx, "ran", true))))
                .map("after", ctx -> put(ctx, "after", true))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                Check.equal(ctx.get("skipped"), null, "gated branch stopped");
                Check.equal(ctx.get("ran"), true, "sibling branch ran");
                Check.equal(ctx.get("after"), true, "join released");
            }
        });
    }

    /** A transient failure is retried according to the step's policy. */
    public static void retriesTransientFailures() throws Exception {
        Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        Blueprint<Map<String, Object>> bp = json("retry")
                .map("flaky", ctx -> {
                    int n = attempts.computeIfAbsent("flaky", k -> new AtomicInteger()).incrementAndGet();
                    if (n < 3) throw new IllegalStateException("boom " + n);
                    return put(ctx, "attempts", (long) n);
                })
                .retry(RetryPolicy.fixed(5, Duration.ofMillis(50)))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Check.equal(Json.asObject(v.context()).get("attempts"), 3L, "attempts before success");
            }
        });
    }

    /** Retries stop at the policy limit and the instance fails with the last error. */
    public static void exhaustedRetriesFailInstance() throws Exception {
        Blueprint<Map<String, Object>> bp = json("retry-exhausted")
                .map("always-fails", ctx -> {
                    throw new IllegalStateException("permanent trouble");
                })
                .retry(RetryPolicy.fixed(2, Duration.ofMillis(20)))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "FAILED", "status");
                Check.contains(v.error(), "permanent trouble", "error message");
            }
        });
    }

    /** PermanentActivityException skips retries entirely. */
    public static void permanentFailureSkipsRetries() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Blueprint<Map<String, Object>> bp = json("permanent")
                .map("fatal", ctx -> {
                    calls.incrementAndGet();
                    throw new PermanentActivityException("do not retry me");
                })
                .retry(RetryPolicy.fixed(5, Duration.ofMillis(20)))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "FAILED", "status");
                Check.equal(calls.get(), 1, "invocation count");
            }
        });
    }

    /** A sleep is a server-side timer: the instance waits without occupying a worker. */
    public static void sleepDefersWithoutHoldingAWorker() throws Exception {
        Blueprint<Map<String, Object>> bp = json("sleeper")
                .map("before", ctx -> put(ctx, "before", System.currentTimeMillis()))
                .sleep(Duration.ofMillis(600))
                .map("after", ctx -> put(ctx, "after", System.currentTimeMillis()))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                String id = client.start(bp, Map.of());
                Check.sleep(200);
                Check.equal(client.instance(id).status(), "RUNNING", "still running mid-sleep");
                Check.equal(w.inFlight(), 0, "no worker slot held during sleep");

                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
                Map<String, Object> ctx = Json.asObject(v.context());
                long elapsed = (Long) ctx.get("after") - (Long) ctx.get("before");
                Check.isTrue(elapsed >= 550, "timer waited at least the requested time, was " + elapsed + "ms");
            }
        });
    }

    /**
     * A worker that takes a task and dies must not strand the instance: the leader
     * reclaims the expired lease and the task becomes dispatchable again.
     */
    public static void expiredLeaseIsReclaimed() throws Exception {
        Blueprint<Map<String, Object>> bp = json("orphan")
                .map("work", ctx -> put(ctx, "done", true))
                .retry(RetryPolicy.fixed(5, Duration.ofMillis(20)))
                .build();

        withServer((server, client) -> {
            client.register(bp);
            String id = client.start(bp, Map.of());

            // Simulate a worker that leases the task and then disappears without reporting.
            List<TaskActivation> leased = client.poll("doomed-worker", bp.queues(), 1, 400, 2000);
            Check.equal(leased.size(), 1, "tasks leased by the doomed worker");

            // The leader must hand the same step back out once the lease expires.
            AtomicReference<TaskActivation> reclaimed = new AtomicReference<>();
            Check.eventually("the lease to be reclaimed", 10_000, () -> {
                List<TaskActivation> again = client.poll("survivor", bp.queues(), 1, 30_000, 0);
                if (again.isEmpty()) return false;
                reclaimed.set(again.get(0));
                return true;
            });
            Check.equal(reclaimed.get().nodeId(), leased.get(0).nodeId(), "same step redelivered");
            Check.isTrue(reclaimed.get().attempt() > leased.get(0).attempt(), "attempt counter advanced");

            client.complete(reclaimed.get().taskId(), "survivor", Map.of("done", true));
            InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(20));
            Check.equal(v.status(), "COMPLETED", "status");
            Check.equal(Json.asObject(v.context()).get("done"), true, "work eventually done");
        });
    }

    /** A task may only be completed by the worker holding its lease. */
    public static void staleLeaseIsRejected() throws Exception {
        Blueprint<Map<String, Object>> bp = json("lease-guard")
                .map("work", ctx -> put(ctx, "done", true))
                .build();

        withServer((server, client) -> {
            client.register(bp);
            client.start(bp, Map.of());
            List<TaskActivation> leased = client.poll("owner", bp.queues(), 1, 30_000, 2000);
            Check.equal(leased.size(), 1, "leased tasks");

            boolean rejected = false;
            try {
                client.complete(leased.get(0).taskId(), "impostor", Map.of("done", true));
            } catch (WiggleClient.WiggleApiException e) {
                rejected = e.status() == 409;
            }
            Check.isTrue(rejected, "completion by a non-owner is rejected with 409");
        });
    }

    /** Cancelling an instance stops it and abandons its in-flight work. */
    public static void cancelStopsAnInstance() throws Exception {
        Blueprint<Map<String, Object>> bp = json("cancellable")
                .map("slow", ctx -> {
                    Check.sleep(2000);
                    return put(ctx, "done", true);
                })
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp)) {
                String id = client.start(bp, Map.of());
                Check.eventually("the task to be picked up", 5000, () -> w.inFlight() > 0);
                client.cancel(id, "operator request");
                InstanceView v = client.instance(id);
                Check.equal(v.status(), "CANCELLED", "status");
                Check.equal(v.terminationReason(), "operator request", "reason");
            }
        });
    }

    /**
     * A task that runs far longer than its lease still completes exactly once: the worker
     * heartbeats to extend the lease while the handler runs, so the leader never reclaims
     * it and redelivers it to another worker. Without the heartbeat this same task -- three
     * lease-lengths long -- would be reclaimed mid-flight and executed twice.
     */
    public static void heartbeatKeepsLongTaskAlive() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        Blueprint<Map<String, Object>> bp = json("heartbeat")
                .map("long-running", ctx -> {
                    invocations.incrementAndGet();
                    Check.sleep(900);               // three times the 300ms lease
                    return put(ctx, "done", true);
                })
                .build();

        withServer((server, client) -> {
            Worker w = new Worker(client, "hb-" + Ids.next("x"),
                    WorkerOptions.defaults()
                            .withLease(Duration.ofMillis(300))          // heartbeat fires at ~100ms
                            .withLongPollWait(Duration.ofMillis(250)))
                    .register(bp);
            try (w) {
                w.start();
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Check.equal(Json.asObject(v.context()).get("done"), true, "work completed");
                Check.equal(invocations.get(), 1, "task executed exactly once (lease never expired)");
            }
        });
    }

    /** Discovery returns every live node's dialable address, served from any node. */
    public static void discoveryReturnsLiveServers() throws Exception {
        withCluster(2, (servers, client) -> {
            Set<String> expected = new HashSet<>();
            for (WiggleServer s : servers) expected.add(s.baseUrl());

            List<String> seen = new ArrayList<>();
            Check.eventually("both nodes discoverable", 5000, () -> {
                seen.clear();
                seen.addAll(client.discover("probe", List.of()));
                return seen.size() == 2;
            });
            Check.equal(new HashSet<>(seen), expected, "discovered addresses match the live nodes");
        });
    }

    /**
     * The backpressure invariant under a cluster: a worker polling several servers
     * round-robin still never holds more in-flight tasks than its concurrency. This is
     * exactly what fanning a full poll out to every server would have broken.
     */
    public static void backpressureHoldsAcrossServers() throws Exception {
        withCluster(2, (servers, client) -> {
            int concurrency = 3;
            Blueprint<Map<String, Object>> bp = json("disco-backpressure")
                    .map("slow", ctx -> { Check.sleep(120); return put(ctx, "done", true); })
                    .build();
            client.register(bp);

            List<String> seeds = new ArrayList<>();
            for (WiggleServer s : servers) seeds.add(s.baseUrl());

            Worker w = new Worker(seeds, "w-" + Ids.next("x"), WorkerOptions.defaults()
                    .withConcurrency(concurrency)
                    .withLongPollWait(Duration.ofMillis(200))
                    .withDiscoveryInterval(Duration.ofMillis(200)))
                    .register(bp);
            AtomicInteger maxSeen = new AtomicInteger();
            AtomicBoolean sampling = new AtomicBoolean(true);
            Thread sampler = new Thread(() -> {
                while (sampling.get()) {
                    maxSeen.accumulateAndGet(w.inFlight(), Math::max);
                    try { Thread.sleep(3); } catch (InterruptedException e) { return; }
                }
            });
            try {
                w.start();
                sampler.setDaemon(true);
                sampler.start();
                List<String> ids = new ArrayList<>();
                for (int i = 0; i < 30; i++) ids.add(client.start(bp, Map.of("i", (long) i)));
                for (String id : ids) {
                    Check.equal(client.awaitCompletion(id, Duration.ofSeconds(30)).status(), "COMPLETED", "status");
                }
            } finally {
                sampling.set(false);
                w.close();
            }
            Check.isTrue(maxSeen.get() > 0, "the worker actually ran tasks");
            Check.isTrue(maxSeen.get() <= concurrency,
                    "in-flight never exceeded concurrency " + concurrency + ", peak was " + maxSeen.get());
        });
    }

    /**
     * A worker seeded with only one node still drains the whole cluster: it discovers the
     * siblings and round-robins across them, so work handed out by any node completes.
     */
    public static void workerSeededWithOneNodeDrainsCluster() throws Exception {
        withCluster(2, (servers, client) -> {
            Blueprint<Map<String, Object>> bp = json("disco-drain")
                    .map("work", ctx -> put(ctx, "done", true))
                    .build();
            client.register(bp);

            Worker w = new Worker(List.of(servers.get(0).baseUrl()), "w-" + Ids.next("x"),
                    WorkerOptions.defaults()
                            .withConcurrency(4)
                            .withLongPollWait(Duration.ofMillis(200))
                            .withDiscoveryInterval(Duration.ofMillis(200)))
                    .register(bp);
            try {
                w.start();
                List<String> ids = new ArrayList<>();
                for (int i = 0; i < 20; i++) ids.add(client.start(bp, Map.of("i", (long) i)));
                for (String id : ids) {
                    Check.equal(client.awaitCompletion(id, Duration.ofSeconds(30)).status(), "COMPLETED", "status");
                }
            } finally {
                w.close();
            }
        });
    }

    /** An unreachable seed must not stop a worker: it bootstraps from the reachable one. */
    public static void seedFailoverBootstrapsFromReachableNode() throws Exception {
        withCluster(1, (servers, client) -> {
            Blueprint<Map<String, Object>> bp = json("disco-seed-failover")
                    .map("work", ctx -> put(ctx, "done", true))
                    .build();
            client.register(bp);

            // First seed points at nothing; the worker must fall through to the live node.
            List<String> seeds = List.of("127.0.0.1:1", servers.get(0).baseUrl());
            Worker w = new Worker(seeds, "w-" + Ids.next("x"), WorkerOptions.defaults()
                    .withConcurrency(2)
                    .withLongPollWait(Duration.ofMillis(200))
                    .withDiscoveryInterval(Duration.ofMillis(200)))
                    .register(bp);
            try {
                w.start();
                String id = client.start(bp, Map.of());
                Check.equal(client.awaitCompletion(id, Duration.ofSeconds(30)).status(), "COMPLETED", "status");
            } finally {
                w.close();
            }
        });
    }

    /** The same DSL compiles to the same version; a changed topology gets a new one. */
    public static void definitionVersionIsContentAddressed() {
        Blueprint<Map<String, Object>> a = json("versioned").map("one", ctx -> ctx).build();
        Blueprint<Map<String, Object>> b = json("versioned").map("one", ctx -> ctx).build();
        Blueprint<Map<String, Object>> c = json("versioned")
                .map("one", ctx -> ctx).map("two", ctx -> ctx).build();

        Check.equal(a.version(), b.version(), "identical topologies share a version");
        Check.isTrue(a.version() != c.version(), "a changed topology gets a new version");
        Check.isTrue(a.version() > 0, "versions are positive");
    }

    /** The DSL rejects graphs that could not run, at build time rather than at runtime. */
    public static void dslRejectsInvalidGraphs() {
        boolean duplicateRejected = false;
        try {
            json("dup").map("same", ctx -> ctx).map("same", ctx -> ctx).build();
        } catch (IllegalArgumentException e) {
            duplicateRejected = true;
        }
        Check.isTrue(duplicateRejected, "duplicate step names are rejected");

        boolean emptyRejected = false;
        try {
            json("empty").build();
        } catch (IllegalStateException e) {
            emptyRejected = true;
        }
        Check.isTrue(emptyRejected, "a workflow with no steps is rejected");

        boolean strayRetryRejected = false;
        try {
            json("stray").map("a", ctx -> ctx).sleep(Duration.ofMillis(1))
                    .retry(RetryPolicy.fixed(2, Duration.ofMillis(1))).build();
        } catch (IllegalStateException e) {
            strayRetryRejected = true;
        }
        Check.isTrue(strayRetryRejected, "retry() must follow a step");
    }

    /**
     * Leader election over a shared store: exactly one leader, chosen deterministically
     * as the longest-running node, and failover when that node goes away.
     */
    public static void leaderElectionAndFailover() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            ClusterManager first = new ClusterManager(storage, "node-a", 2, 150, 3);
            first.start();
            Check.sleep(120);
            ClusterManager second = new ClusterManager(storage, "node-b", 2, 150, 3);
            second.start();

            try {
                Check.eventually("a stable leader", 5000, () -> first.isLeader() != second.isLeader());
                Check.isTrue(first.isLeader(), "the longest-running node leads");
                Check.isTrue(!second.isLeader(), "the newer node follows");
                Check.equal(first.members().size(), 2, "cluster size");

                first.close();
                Check.eventually("failover to the surviving node", 5000, second::isLeader);
            } finally {
                first.close();
                second.close();
            }
        }
    }

    /** Two workers on one server share the work rather than duplicating it. */
    public static void workDistributesAcrossWorkers() throws Exception {
        Map<String, AtomicInteger> byWorker = new ConcurrentHashMap<>();
        AtomicInteger total = new AtomicInteger();
        Blueprint<Map<String, Object>> bp = json("distributed")
                .map("work", ctx -> {
                    total.incrementAndGet();
                    Check.sleep(40);
                    return put(ctx, "done", true);
                })
                .build();

        withServer((server, client) -> {
            try (Worker a = startWorker(client, bp); Worker b = startWorker(client, bp)) {
                byWorker.put(a.workerId(), new AtomicInteger());
                byWorker.put(b.workerId(), new AtomicInteger());
                List<String> ids = new ArrayList<>();
                for (int i = 0; i < 20; i++) ids.add(client.start(bp, Map.of("i", (long) i)));
                for (String id : ids) {
                    Check.equal(client.awaitCompletion(id, Duration.ofSeconds(30)).status(),
                            "COMPLETED", "status of " + id);
                }
                Check.equal(total.get(), 20, "each task executed exactly once");
            }
        });
    }

    /** Records round-trip through JSON without losing types. */
    public static void recordCodecRoundTrip() {
        record Line(String sku, int qty) {}
        record Basket(String id, List<Line> lines, Map<String, String> tags,
                      java.math.BigDecimal total, java.time.Instant at, Optional<String> note) {}

        ContextCodec<Basket> codec = ContextCodec.records(Basket.class);
        Basket original = new Basket("b1", List.of(new Line("sku-1", 2), new Line("sku-2", 5)),
                Map.of("channel", "web"), new java.math.BigDecimal("19.99"),
                java.time.Instant.parse("2026-01-02T03:04:05Z"), Optional.of("gift"));

        Basket round = codec.decode(Json.parse(Json.write(codec.encode(original))));
        Check.equal(round, original, "record round trip");
    }

    // ------------------------------------------------------------------ main

    private record Case(String name, ThrowingRunnable body) {}

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static List<Case> all() {
        return List.of(
                new Case("recordCodecRoundTrip", Scenarios::recordCodecRoundTrip),
                new Case("definitionVersionIsContentAddressed", Scenarios::definitionVersionIsContentAddressed),
                new Case("dslRejectsInvalidGraphs", Scenarios::dslRejectsInvalidGraphs),
                new Case("sequentialPipeline", Scenarios::sequentialPipeline),
                new Case("filterShortCircuits", Scenarios::filterShortCircuits),
                new Case("forkMergesDisjointWrites", Scenarios::forkMergesDisjointWrites),
                new Case("joinRunsContinuationOnce", Scenarios::joinRunsContinuationOnce),
                new Case("nestedForks", Scenarios::nestedForks),
                new Case("filterInsideBranchDoesNotStrandSiblings", Scenarios::filterInsideBranchDoesNotStrandSiblings),
                new Case("retriesTransientFailures", Scenarios::retriesTransientFailures),
                new Case("exhaustedRetriesFailInstance", Scenarios::exhaustedRetriesFailInstance),
                new Case("permanentFailureSkipsRetries", Scenarios::permanentFailureSkipsRetries),
                new Case("sleepDefersWithoutHoldingAWorker", Scenarios::sleepDefersWithoutHoldingAWorker),
                new Case("expiredLeaseIsReclaimed", Scenarios::expiredLeaseIsReclaimed),
                new Case("staleLeaseIsRejected", Scenarios::staleLeaseIsRejected),
                new Case("cancelStopsAnInstance", Scenarios::cancelStopsAnInstance),
                new Case("heartbeatKeepsLongTaskAlive", Scenarios::heartbeatKeepsLongTaskAlive),
                new Case("discoveryReturnsLiveServers", Scenarios::discoveryReturnsLiveServers),
                new Case("backpressureHoldsAcrossServers", Scenarios::backpressureHoldsAcrossServers),
                new Case("workerSeededWithOneNodeDrainsCluster", Scenarios::workerSeededWithOneNodeDrainsCluster),
                new Case("seedFailoverBootstrapsFromReachableNode", Scenarios::seedFailoverBootstrapsFromReachableNode),
                new Case("leaderElectionAndFailover", Scenarios::leaderElectionAndFailover),
                new Case("workDistributesAcrossWorkers", Scenarios::workDistributesAcrossWorkers));
    }

    public static void main(String[] args) {
        int passed = 0;
        List<String> failures = new ArrayList<>();
        for (Case c : all()) {
            long start = System.currentTimeMillis();
            try {
                c.body().run();
                passed++;
                System.out.printf("  PASS  %-45s %5d ms%n", c.name(), System.currentTimeMillis() - start);
            } catch (Throwable t) {
                failures.add(c.name() + " -- " + t);
                System.out.printf("  FAIL  %-45s %5d ms%n        %s%n",
                        c.name(), System.currentTimeMillis() - start, t);
            }
        }
        System.out.println("\n" + passed + "/" + all().size() + " scenarios passed");
        failures.forEach(f -> System.out.println("  ! " + f));
        if (!failures.isEmpty()) System.exit(1);
    }
}
