package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Node;
import com.wiggle.core.ReportResult;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.Task_;
import com.wiggle.proto.CriteriaComparison;
import com.wiggle.proto.CriteriaPredicate;
import com.wiggle.proto.PollRequest;
import com.wiggle.proto.WiggleControlPlaneGrpc;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.testfixtures.TestPorts;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Polling with {@link QueueCriteria} end to end: claim, local chains, and the wire checks. */
class QueueCriteriaTest {

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
        Map<String, Object> d(Map<String, Object> ctx);
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "qc-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static FlowSpec flow(String name, ExecutionMode mode) {
        return FlowSpec.define(name, 1, Map.class, Steps.class, (f, s) -> Modes.in(f, mode)
                .thenApply(s::a)
                .thenApply(s::b)
                .thenApply(s::c)
                .thenApply(s::d));
    }

    @Test
    @DisplayName("WGL-QC-060: workers on one queue split its instances by correlation id, in every mode")
    void splitByCorrelation() throws Exception {
        for (ExecutionMode mode : List.of(ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC)) {
            Map<String, Set<String>> ranOn = new ConcurrentHashMap<>();
            FlowSpec spec = flow("qc-split", mode);
            QueueCriteria vip = QueueCriteria.from("qc-split").where(cb -> cb.like(Task_.CORRELATION_ID, "vip-%"));
            QueueCriteria rest = QueueCriteria.from("qc-split").where(cb -> cb.not(cb.like(Task_.CORRELATION_ID, "vip-%")));
            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker v = new Worker(client, "vip", WorkerOptions.defaults().withCriteria(vip))
                         .registerHandler("qc-split", new H("vip", ranOn));
                 Worker r = new Worker(client, "rest", WorkerOptions.defaults().withCriteria(rest))
                         .registerHandler("qc-split", new H("rest", ranOn))) {
                client.register(spec);
                v.start();
                r.start();
                String vipId = client.start("qc-split", Map.of(), spec.version(), "vip-1");
                String stdId = client.start("qc-split", Map.of(), spec.version(), "std-1");
                for (String id : List.of(vipId, stdId)) {
                    assertEquals("COMPLETED", client.awaitCompletion(id, Duration.ofSeconds(20)).status(), mode + " " + id);
                }
                assertEquals(Set.of("vip"), ranOn.get(vipId), mode + ": every step of the vip instance");
                assertEquals(Set.of("rest"), ranOn.get(stdId), mode + ": every step of the other instance");
            }
        }
    }

    @Test
    @DisplayName("WGL-QC-080: a task reaches the worker whose criteria match it on the wake, not the fallback")
    void wakeReachesTheMatchingWorker() throws Exception {
        String wf = "qc-wake";
        FlowSpec spec = flow(wf, ExecutionMode.SERVER);
        QueueCriteria vip = QueueCriteria.from(wf).where(cb -> cb.like(Task_.CORRELATION_ID, "vip-%"));
        QueueCriteria rest = QueueCriteria.from(wf).where(cb -> cb.not(cb.like(Task_.CORRELATION_ID, "vip-%")));
        // A missed wake costs the 3 s fallback; long polls outlast it, so nothing else hides one.
        System.setProperty("wiggle.fallback.pollMillis", "3000");
        ServerConfig longPolls = new ServerConfig(TestPorts.free(), "qc-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofSeconds(10), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(longPolls).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker v = new Worker(client, "vip", WorkerOptions.defaults().withConcurrency(1).withCriteria(vip))
                     .registerHandler(wf, new H("vip", new ConcurrentHashMap<>()));
             Worker r = new Worker(client, "rest", WorkerOptions.defaults().withConcurrency(1).withCriteria(rest))
                     .registerHandler(wf, new H("rest", new ConcurrentHashMap<>()))) {
            client.register(spec);
            v.start();
            r.start();
            Thread.sleep(500);
            for (int i = 0; i < 6; i++) {
                String correlation = (i / 2 % 2 == 0 ? "vip-" : "std-") + i;   // two of each in turn
                long t0 = System.nanoTime();
                String id = client.start(wf, Map.of(), spec.version(), correlation);
                assertEquals("COMPLETED", client.awaitCompletion(id, Duration.ofSeconds(20)).status());
                long ms = (System.nanoTime() - t0) / 1_000_000;
                assertTrue(ms < 1_500, correlation + " took " + ms + " ms: its worker waited for the fallback");
            }
        } finally {
            System.clearProperty("wiggle.fallback.pollMillis");
        }
    }

    @Test
    @DisplayName("WGL-QC-070: a local chain hands back at a step its criteria do not match")
    void chainHandsBackOutsideCriteria() throws Exception {
        for (ExecutionMode mode : List.of(ExecutionMode.LOCAL_SYNC, ExecutionMode.LOCAL_ASYNC)) {
            Map<String, Set<String>> ranOn = new ConcurrentHashMap<>();
            Map<String, String> byStep = new ConcurrentHashMap<>();
            FlowSpec spec = flow("qc-chain", mode);
            QueueCriteria front = QueueCriteria.from("qc-chain").where(cb -> cb.in(Task_.STEP, "a", "b", "d"));
            QueueCriteria back = QueueCriteria.from("qc-chain").where(cb -> cb.equal(Task_.STEP, "c"));
            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker f = new Worker(client, "front", WorkerOptions.defaults().withCriteria(front))
                         .registerHandler("qc-chain", new H("front", ranOn, byStep));
                 Worker b = new Worker(client, "back", WorkerOptions.defaults().withCriteria(back))
                         .registerHandler("qc-chain", new H("back", ranOn, byStep))) {
                client.register(spec);
                f.start();
                b.start();
                InstanceView done = client.awaitCompletion(client.start(spec, Map.of()), Duration.ofSeconds(20));
                assertEquals("COMPLETED", done.status(), mode + " status");
                assertEquals(Map.of("a", "front", "b", "front", "c", "back", "d", "front"), byStep, mode.name());
            }
        }
    }

    @Test
    @DisplayName("WGL-QC-010: each queue a worker serves has its own criteria, in claims and local chains")
    void criteriaPerQueue() throws Exception {
        for (ExecutionMode mode : List.of(ExecutionMode.SERVER, ExecutionMode.LOCAL_SYNC)) {
            Map<String, String> byStep = new ConcurrentHashMap<>();
            String wf = "qc-two-" + mode.name().toLowerCase(java.util.Locale.ROOT);
            String other = wf + "-other";
            FlowSpec spec = FlowSpec.define(wf, 1, Map.class, Steps.class, (f, s) -> Modes.in(f, mode)
                    .thenApply(s::a)
                    .thenApply(s::b)
                    .thenApply(s::c, other)
                    .thenApply(s::d, other));
            // front takes everything on wf, and only d on the other queue; back takes c there.
            QueueCriteria all = QueueCriteria.from(wf);
            QueueCriteria onlyD = QueueCriteria.from(other).where(cb -> cb.equal(Task_.STEP, "d"));
            QueueCriteria onlyC = QueueCriteria.from(other).where(cb -> cb.equal(Task_.STEP, "c"));
            try (WiggleServer server = new WiggleServer(config()).start();
                 WiggleClient client = new WiggleClient(server.baseUrl());
                 Worker f = new Worker(client, "front", WorkerOptions.defaults().withCriteria(all, onlyD))
                         .registerHandler(wf, new H("front", new ConcurrentHashMap<>(), byStep));
                 Worker b = new Worker(client, "back", WorkerOptions.defaults().withCriteria(onlyC))
                         .registerHandler(wf, new H("back", new ConcurrentHashMap<>(), byStep))) {
                client.register(spec);
                f.start();
                b.start();
                InstanceView done = client.awaitCompletion(client.start(spec, Map.of()), Duration.ofSeconds(20));
                assertEquals("COMPLETED", done.status(), mode + " status");
                assertEquals(Map.of("a", "front", "b", "front", "c", "back", "d", "front"), byStep, mode.name());
            }
        }
    }

    @Test
    @DisplayName("WGL-QC-070/071: the server leases a continuation only when the reported criteria match it")
    void serverChecksTheContinuation() throws Exception {
        FlowSpec spec = flow("qc-lease", ExecutionMode.LOCAL_ASYNC);
        String nodeB = nodeNamed(spec, "b");
        QueueCriteria onlyA = QueueCriteria.from("qc-lease").where(cb -> cb.equal(Task_.STEP, "a"));
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec);

            client.start(spec, Map.of());
            TaskActivation a = client.poll("w1", List.of(onlyA), Set.of(), 1, 30_000, 2_000).tasks().getFirst();
            assertEquals("a", a.stepName());
            List<WiggleClient.StepReport> twoSteps = List.of(
                    new WiggleClient.StepReport(a.nodeId(), Map.of("a", 1), null),
                    new WiggleClient.StepReport(nodeB, Map.of("b", 1), null));
            WiggleApiException refused = assertThrows(WiggleApiException.class,
                    () -> client.reportSteps(a.taskId(), a.leaseOwner(), twoSteps, false, List.of(onlyA)));
            assertEquals(400, refused.status(), "running b in a chain is outside the worker's criteria");

            ReportResult released = client.reportSteps(a.taskId(), a.leaseOwner(),
                    List.of(twoSteps.getFirst()), false, List.of(onlyA));
            assertNull(released.nextTaskId(), "b is parked for whoever matches, not leased back");

            client.start(spec, Map.of());
            TaskActivation a2 = client.poll("w2", List.of(onlyA), Set.of(), 1, 30_000, 2_000).tasks().getFirst();
            ReportResult leased = client.reportSteps(a2.taskId(), a2.leaseOwner(),
                    List.of(new WiggleClient.StepReport(a2.nodeId(), Map.of("a", 1), null)), false, null);
            assertNotNull(leased.nextTaskId(), "a worker polling by queue name is leased b as before");
        }
    }

    @Test
    @DisplayName("WGL-QC-050/051: queues and criteria together, or malformed criteria, are INVALID_ARGUMENT")
    void wireChecks() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start()) {
            ManagedChannel channel = Grpc.newChannelBuilder(server.baseUrl(), InsecureChannelCredentials.create()).build();
            try {
                var stub = WiggleControlPlaneGrpc.newBlockingStub(channel);
                com.wiggle.proto.QueueCriteria ok = com.wiggle.proto.QueueCriteria.newBuilder().setQueue("q").build();
                StatusRuntimeException both = assertThrows(StatusRuntimeException.class, () -> stub.pollTasks(
                        PollRequest.newBuilder().setWorkerId("w").addQueues("q").addCriteria(ok).build()));
                assertEquals(Status.Code.INVALID_ARGUMENT, both.getStatus().getCode());

                com.wiggle.proto.QueueCriteria unknownField = ok.toBuilder().setWhere(CriteriaPredicate.newBuilder()
                        .setCmp(CriteriaComparison.newBuilder().setField("lease_owner").setOp("IS_NULL"))).build();
                StatusRuntimeException bad = assertThrows(StatusRuntimeException.class, () -> stub.pollTasks(
                        PollRequest.newBuilder().setWorkerId("w").addCriteria(unknownField).build()));
                assertEquals(Status.Code.INVALID_ARGUMENT, bad.getStatus().getCode());
                assertTrue(bad.getStatus().getDescription().contains("lease_owner"), bad.getStatus().getDescription());

                StatusRuntimeException noQueue = assertThrows(StatusRuntimeException.class, () -> stub.pollTasks(
                        PollRequest.newBuilder().setWorkerId("w")
                                .addCriteria(com.wiggle.proto.QueueCriteria.getDefaultInstance()).build()));
                assertEquals(Status.Code.INVALID_ARGUMENT, noQueue.getStatus().getCode());

                StatusRuntimeException twice = assertThrows(StatusRuntimeException.class, () -> stub.pollTasks(
                        PollRequest.newBuilder().setWorkerId("w").addCriteria(ok).addCriteria(ok).build()));
                assertEquals(Status.Code.INVALID_ARGUMENT, twice.getStatus().getCode());
                assertTrue(twice.getStatus().getDescription().contains("more than one"), twice.getStatus().getDescription());
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test
    @DisplayName("WGL-QC-014: a worker refuses both withQueues and withCriteria")
    void workerRefusesBothForms() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "both", WorkerOptions.defaults().withQueues("q")
                     .withCriteria(QueueCriteria.from("q")))) {
            assertThrows(IllegalStateException.class, w::start);
        }
    }

    private static String nodeNamed(FlowSpec spec, String name) {
        for (Node n : spec.definition().nodes().values()) if (name.equals(n.name())) return n.id();
        throw new AssertionError("no node " + name);
    }

    static final class H {
        final String label;
        final Map<String, Set<String>> ranOn;
        final Map<String, String> byStep;

        H(String label, Map<String, Set<String>> ranOn) {
            this(label, ranOn, new ConcurrentHashMap<>());
        }

        H(String label, Map<String, Set<String>> ranOn, Map<String, String> byStep) {
            this.label = label;
            this.ranOn = ranOn;
            this.byStep = byStep;
        }

        public Map<String, Object> a(Map<String, Object> ctx) { return ran("a", ctx); }
        public Map<String, Object> b(Map<String, Object> ctx) { return ran("b", ctx); }
        public Map<String, Object> c(Map<String, Object> ctx) { return ran("c", ctx); }
        public Map<String, Object> d(Map<String, Object> ctx) { return ran("d", ctx); }

        private Map<String, Object> ran(String step, Map<String, Object> ctx) {
            ranOn.computeIfAbsent(com.wiggle.client.worker.Step.instanceId(), k -> ConcurrentHashMap.newKeySet()).add(label);
            byStep.put(step, label);
            return ctx;
        }
    }
}
