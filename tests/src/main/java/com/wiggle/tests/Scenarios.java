package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.dsl.WorkflowStream;
import com.wiggle.client.worker.Handlers;
import com.wiggle.client.worker.PermanentActivityException;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.*;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Behavioural conformance suite for the engine. Each method is self-contained and
 * throws {@link AssertionError} on failure, so it can be driven either by
 * {@link #main} or by the JUnit wrapper in tests/src/test.
 *
 * <p>Each workflow is pure topology; its step logic lives in a {@code @Handlers} class below whose
 * method names match the steps and whose signatures define the types.
 */
public final class Scenarios {

    private Scenarios() {}

    private interface Body {
        void run(WiggleServer server, WiggleClient client) throws Exception;
    }

    private static void withServer(Body body) throws Exception {
        ServerConfig config = new ServerConfig(0, "test-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            body.run(server, client);
        }
    }

    private static Worker startWorker(WiggleClient client, Blueprint bp, Object handlers) {
        Worker w = new Worker(client, "w-" + Ids.next("x"),
                WorkerOptions.defaults().withConcurrency(4).withLongPollWait(Duration.ofMillis(250)))
                .register(bp).handlers(handlers);
        return w.start();
    }

    private static WorkflowStream json(String name) {
        return Workflow.define(name);
    }

    static Map<String, Object> put(Map<String, Object> ctx, String key, Object value) {
        Map<String, Object> next = new LinkedHashMap<>(ctx);
        next.put(key, value);
        return next;
    }


    /** A linear pipeline runs its steps in order and the context accumulates. */
    public static void sequentialPipeline() throws Exception {
        Blueprint bp = json("seq")
                .step("one")
                .step("two")
                .step("three")
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new SeqH())) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                Check.equal(ctx.get("a"), 1L, "a");
                Check.equal(ctx.get("b"), 2L, "b");
                Check.equal(ctx.get("c"), 3L, "c");
            }
        });
    }

    @Handlers("seq")
    static final class SeqH {
        public Map<String, Object> one(Map<String, Object> ctx) { return put(ctx, "a", 1L); }
        public Map<String, Object> two(Map<String, Object> ctx) { return put(ctx, "b", (Long) ctx.get("a") + 1); }
        public Map<String, Object> three(Map<String, Object> ctx) { return put(ctx, "c", (Long) ctx.get("b") + 1); }
    }

    /** A false gate ends the instance successfully and skips everything downstream. */
    public static void gateShortCircuits() throws Exception {
        AtomicInteger downstream = new AtomicInteger();
        Blueprint bp = json("gated")
                .step("seed")
                .gate("gate")
                .step("never")
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new GatedH(downstream))) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Check.equal(v.terminationReason(), "gated:gate", "termination reason");
                Check.equal(downstream.get(), 0, "downstream invocations");
            }
        });
    }

    @Handlers("gated")
    static final class GatedH {
        final AtomicInteger downstream;
        GatedH(AtomicInteger downstream) { this.downstream = downstream; }
        public Map<String, Object> seed(Map<String, Object> ctx) { return put(ctx, "keep", false); }
        public boolean gate(Map<String, Object> ctx) { return Boolean.TRUE.equals(ctx.get("keep")); }
        public Map<String, Object> never(Map<String, Object> ctx) {
            downstream.incrementAndGet();
            return put(ctx, "ran", true);
        }
    }

    /** Parallel branches merge field-by-field instead of clobbering each other. */
    public static void forkMergesDisjointWrites() throws Exception {
        Blueprint bp = json("fork-merge")
                .step("seed")
                .fork(
                        Branch.of("left", s -> s.step("slow-left")),
                        Branch.of("right", s -> s.step("fast-right")))
                .combine("merge")
                .step("after")
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new ForkMergeH())) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                Check.equal(ctx.get("left"), "L", "left branch write survived");
                Check.equal(ctx.get("right"), "R", "right branch write survived");
                Check.equal(ctx.get("joined"), true, "post-join step ran");
            }
        });
    }

    @Handlers("fork-merge")
    static final class ForkMergeH {
        public Map<String, Object> seed(Map<String, Object> ctx) { return put(ctx, "seeded", true); }
        public Map<String, Object> slowLeft(Map<String, Object> ctx) {
            Check.sleep(150);           // finishes last on purpose
            return put(ctx, "left", "L");
        }
        public Map<String, Object> fastRight(Map<String, Object> ctx) { return put(ctx, "right", "R"); }
        public Map<String, Object> after(Map<String, Object> ctx) { return put(ctx, "joined", true); }
    }

    /** The step after a fork runs exactly once, no matter how many branches there were. */
    public static void joinRunsContinuationOnce() throws Exception {
        AtomicInteger afterCount = new AtomicInteger();
        Blueprint bp = json("join-once")
                .fork(
                        Branch.of("a", s -> s.step("a1")),
                        Branch.of("b", s -> s.step("b1")),
                        Branch.of("c", s -> s.step("c1")))
                .combine("merge")
                .step("after")
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new JoinOnceH(afterCount))) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Check.sleep(300);
                Check.equal(afterCount.get(), 1, "continuation invocations");
            }
        });
    }

    @Handlers("join-once")
    static final class JoinOnceH {
        final AtomicInteger afterCount;
        JoinOnceH(AtomicInteger afterCount) { this.afterCount = afterCount; }
        public Map<String, Object> a1(Map<String, Object> ctx) { return put(ctx, "a", 1L); }
        public Map<String, Object> b1(Map<String, Object> ctx) { return put(ctx, "b", 1L); }
        public Map<String, Object> c1(Map<String, Object> ctx) { return put(ctx, "c", 1L); }
        public Map<String, Object> after(Map<String, Object> ctx) {
            afterCount.incrementAndGet();
            return ctx;
        }
    }

    /** Forks nest: the join stack pops back to the right barrier. */
    public static void nestedForks() throws Exception {
        Blueprint bp = json("nested")
                .fork(
                        Branch.of("outer-left", s -> s.fork(
                                Branch.of("inner-a", t -> t.step("ia")),
                                Branch.of("inner-b", t -> t.step("ib")))
                                .combine("inner-merge")
                                .step("inner-after")),
                        Branch.of("outer-right", s -> s.step("or")))
                .combine("outer-merge")
                .step("outer-after")
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new NestedH())) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                for (String k : List.of("ia", "ib", "innerAfter", "or", "outerAfter")) {
                    Check.equal(ctx.get(k), 1L, "context key " + k);
                }
            }
        });
    }

    @Handlers("nested")
    static final class NestedH {
        public Map<String, Object> ia(Map<String, Object> ctx) { return put(ctx, "ia", 1L); }
        public Map<String, Object> ib(Map<String, Object> ctx) { return put(ctx, "ib", 1L); }
        public Map<String, Object> innerAfter(Map<String, Object> ctx) { return put(ctx, "innerAfter", 1L); }
        public Map<String, Object> or(Map<String, Object> ctx) { return put(ctx, "or", 1L); }
        public Map<String, Object> outerAfter(Map<String, Object> ctx) { return put(ctx, "outerAfter", 1L); }
    }

    /** A gate inside a branch short-circuits that branch only; siblings still join. */
    public static void gateInsideBranchDoesNotStrandSiblings() throws Exception {
        Blueprint bp = json("branch-gate")
                .fork(
                        Branch.of("gated", s -> s
                                .gate("gate")
                                .step("skipped")),
                        Branch.of("other", s -> s.step("ran")))
                .combine("merge")
                .step("after")
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new BranchGateH())) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Map<String, Object> ctx = Json.asObject(v.context());
                Check.equal(ctx.get("skipped"), null, "gated branch stopped");
                Check.equal(ctx.get("ran"), true, "sibling branch ran");
                Check.equal(ctx.get("after"), true, "join released");
            }
        });
    }

    @Handlers("branch-gate")
    static final class BranchGateH {
        public boolean gate(Map<String, Object> ctx) { return false; }
        public Map<String, Object> skipped(Map<String, Object> ctx) { return put(ctx, "skipped", true); }
        public Map<String, Object> ran(Map<String, Object> ctx) { return put(ctx, "ran", true); }
        public Map<String, Object> after(Map<String, Object> ctx) { return put(ctx, "after", true); }
    }

    /** A transient failure is retried according to the step's policy. */
    public static void retriesTransientFailures() throws Exception {
        Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        Blueprint bp = json("retry")
                .step("flaky", RetryPolicy.fixed(5, Duration.ofMillis(50)))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new RetryH(attempts))) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Check.equal(Json.asObject(v.context()).get("attempts"), 3L, "attempts before success");
            }
        });
    }

    @Handlers("retry")
    static final class RetryH {
        final Map<String, AtomicInteger> attempts;
        RetryH(Map<String, AtomicInteger> attempts) { this.attempts = attempts; }
        public Map<String, Object> flaky(Map<String, Object> ctx) {
            int n = attempts.computeIfAbsent("flaky", k -> new AtomicInteger()).incrementAndGet();
            if (n < 3) throw new IllegalStateException("boom " + n);
            return put(ctx, "attempts", (long) n);
        }
    }

    /** Retries stop at the policy limit and the instance fails with the last error. */
    public static void exhaustedRetriesFailInstance() throws Exception {
        Blueprint bp = json("retry-exhausted")
                .step("always-fails", RetryPolicy.fixed(2, Duration.ofMillis(20)))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new ExhaustedH())) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "FAILED", "status");
                Check.contains(v.error(), "permanent trouble", "error message");
            }
        });
    }

    @Handlers("retry-exhausted")
    static final class ExhaustedH {
        public Map<String, Object> alwaysFails(Map<String, Object> ctx) {
            throw new IllegalStateException("permanent trouble");
        }
    }

    /** PermanentActivityException skips retries entirely. */
    public static void permanentFailureSkipsRetries() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        Blueprint bp = json("permanent")
                .step("fatal", RetryPolicy.fixed(5, Duration.ofMillis(20)))
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new PermanentH(calls))) {
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "FAILED", "status");
                Check.equal(calls.get(), 1, "invocation count");
            }
        });
    }

    @Handlers("permanent")
    static final class PermanentH {
        final AtomicInteger calls;
        PermanentH(AtomicInteger calls) { this.calls = calls; }
        public Map<String, Object> fatal(Map<String, Object> ctx) {
            calls.incrementAndGet();
            throw new PermanentActivityException("do not retry me");
        }
    }

    /** A sleep is a server-side timer: the instance waits without occupying a worker. */
    public static void sleepDefersWithoutHoldingAWorker() throws Exception {
        Blueprint bp = json("sleeper")
                .step("before")
                .sleep(Duration.ofMillis(600))
                .step("after")
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new SleeperH())) {
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

    @Handlers("sleeper")
    static final class SleeperH {
        public Map<String, Object> before(Map<String, Object> ctx) { return put(ctx, "before", System.currentTimeMillis()); }
        public Map<String, Object> after(Map<String, Object> ctx) { return put(ctx, "after", System.currentTimeMillis()); }
    }

    /**
     * A worker that takes a task and dies must not strand the instance: the leader
     * reclaims the expired lease and the task becomes dispatchable again.
     */
    public static void expiredLeaseIsReclaimed() throws Exception {
        Blueprint bp = json("orphan")
                .step("work", RetryPolicy.fixed(5, Duration.ofMillis(20)))
                .build();

        withServer((server, client) -> {
            client.register(bp);
            String id = client.start(bp, Map.of());

            // Simulate a worker that leases the task and then disappears without reporting.
            List<TaskActivation> leased = client.poll("doomed-worker", bp.queues(), 1, 400, 2000).tasks();
            Check.equal(leased.size(), 1, "tasks leased by the doomed worker");

            // The leader must hand the same step back out once the lease expires.
            AtomicReference<TaskActivation> reclaimed = new AtomicReference<>();
            Check.eventually("the lease to be reclaimed", 10_000, () -> {
                List<TaskActivation> again = client.poll("survivor", bp.queues(), 1, 30_000, 0).tasks();
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
        Blueprint bp = json("lease-guard")
                .step("work")
                .build();

        withServer((server, client) -> {
            client.register(bp);
            client.start(bp, Map.of());
            List<TaskActivation> leased = client.poll("owner", bp.queues(), 1, 30_000, 2000).tasks();
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
        Blueprint bp = json("cancellable")
                .step("slow")
                .build();

        withServer((server, client) -> {
            try (Worker w = startWorker(client, bp, new CancellableH())) {
                String id = client.start(bp, Map.of());
                Check.eventually("the task to be picked up", 5000, () -> w.inFlight() > 0);
                client.cancel(id, "operator request");
                InstanceView v = client.instance(id);
                Check.equal(v.status(), "CANCELLED", "status");
                Check.equal(v.terminationReason(), "operator request", "reason");
            }
        });
    }

    @Handlers("cancellable")
    static final class CancellableH {
        public Map<String, Object> slow(Map<String, Object> ctx) {
            Check.sleep(2000);
            return put(ctx, "done", true);
        }
    }

    /**
     * A task that runs far longer than its lease still completes exactly once: the worker
     * heartbeats to extend the lease while the handler runs, so the leader never reclaims
     * it and redelivers it to another worker. Without the heartbeat this same task -- three
     * lease-lengths long -- would be reclaimed mid-flight and executed twice.
     */
    public static void heartbeatKeepsLongTaskAlive() throws Exception {
        AtomicInteger invocations = new AtomicInteger();
        Blueprint bp = json("heartbeat")
                .step("long-running")
                .build();

        withServer((server, client) -> {
            Worker w = new Worker(client, "hb-" + Ids.next("x"),
                    WorkerOptions.defaults()
                            .withLease(Duration.ofMillis(300))          // heartbeat fires at ~100ms
                            .withLongPollWait(Duration.ofMillis(250)))
                    .register(bp).handlers(new HeartbeatH(invocations));
            try (w) {
                w.start();
                InstanceView v = client.awaitCompletion(client.start(bp, Map.of()), Duration.ofSeconds(20));
                Check.equal(v.status(), "COMPLETED", "status");
                Check.equal(Json.asObject(v.context()).get("done"), true, "work completed");
                Check.equal(invocations.get(), 1, "task executed exactly once (lease never expired)");
            }
        });
    }

    @Handlers("heartbeat")
    static final class HeartbeatH {
        final AtomicInteger invocations;
        HeartbeatH(AtomicInteger invocations) { this.invocations = invocations; }
        public Map<String, Object> longRunning(Map<String, Object> ctx) {
            invocations.incrementAndGet();
            Check.sleep(900);               // three times the 300ms lease
            return put(ctx, "done", true);
        }
    }

    /** The same DSL compiles to the same version; a changed topology gets a new one. */
    public static void definitionVersionIsContentAddressed() {
        Blueprint a = json("versioned").step("one").build();
        Blueprint b = json("versioned").step("one").build();
        Blueprint c = json("versioned")
                .step("one").step("two").build();

        Check.equal(a.version(), b.version(), "identical topologies share a version");
        Check.isTrue(a.version() != c.version(), "a changed topology gets a new version");
        Check.isTrue(a.version() > 0, "versions are positive");
    }

    /** The DSL rejects graphs that could not run, at build time rather than at runtime. */
    public static void dslRejectsInvalidGraphs() {
        boolean duplicateRejected = false;
        try {
            json("dup").step("same").step("same").build();
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
        AtomicInteger total = new AtomicInteger();
        Blueprint bp = json("distributed")
                .step("work")
                .build();

        withServer((server, client) -> {
            DistributedH handlers = new DistributedH(total);
            try (Worker a = startWorker(client, bp, handlers); Worker b = startWorker(client, bp, handlers)) {
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

    @Handlers("distributed")
    static final class DistributedH {
        final AtomicInteger total;
        DistributedH(AtomicInteger total) { this.total = total; }
        public Map<String, Object> work(Map<String, Object> ctx) {
            total.incrementAndGet();
            Check.sleep(40);
            return put(ctx, "done", true);
        }
    }

    /** Records round-trip through JSON without losing types. */
    public static void recordCodecRoundTrip() {
        record Line(String sku, int qty) {}
        record Basket(String id, List<Line> lines, Map<String, String> tags,
                      java.math.BigDecimal total, java.time.Instant at, Optional<String> note) {}

        Basket original = new Basket("b1", List.of(new Line("sku-1", 2), new Line("sku-2", 5)),
                Map.of("channel", "web"), new java.math.BigDecimal("19.99"),
                java.time.Instant.parse("2026-01-02T03:04:05Z"), Optional.of("gift"));

        Basket round = (Basket) RecordMapper.fromJson(
                Json.parse(Json.write(RecordMapper.toJson(original))), Basket.class);
        Check.equal(round, original, "record round trip");
    }

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
                new Case("gateShortCircuits", Scenarios::gateShortCircuits),
                new Case("forkMergesDisjointWrites", Scenarios::forkMergesDisjointWrites),
                new Case("joinRunsContinuationOnce", Scenarios::joinRunsContinuationOnce),
                new Case("nestedForks", Scenarios::nestedForks),
                new Case("gateInsideBranchDoesNotStrandSiblings", Scenarios::gateInsideBranchDoesNotStrandSiblings),
                new Case("retriesTransientFailures", Scenarios::retriesTransientFailures),
                new Case("exhaustedRetriesFailInstance", Scenarios::exhaustedRetriesFailInstance),
                new Case("permanentFailureSkipsRetries", Scenarios::permanentFailureSkipsRetries),
                new Case("sleepDefersWithoutHoldingAWorker", Scenarios::sleepDefersWithoutHoldingAWorker),
                new Case("expiredLeaseIsReclaimed", Scenarios::expiredLeaseIsReclaimed),
                new Case("staleLeaseIsRejected", Scenarios::staleLeaseIsRejected),
                new Case("cancelStopsAnInstance", Scenarios::cancelStopsAnInstance),
                new Case("heartbeatKeepsLongTaskAlive", Scenarios::heartbeatKeepsLongTaskAlive),
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
