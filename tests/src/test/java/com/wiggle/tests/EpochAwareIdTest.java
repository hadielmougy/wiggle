package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.InstanceStatus;
import com.wiggle.core.TaskActivation;
import com.wiggle.placement.IdCodec;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 2 / T8: a cell with a placement namespace mints epoch-aware instance ids; a standalone cell
 * (no namespace) keeps the legacy {@code wfi_} form. End-to-end through the real start path.
 */
class EpochAwareIdTest {

    /** The step this spec names; a worker binds it by name. */
    interface OneStep {
        Map<String, Object> a(Map<String, Object> ctx);
    }

    /** A compensable first step and a second that fails, so the reverse pass takes the instance. */
    interface SagaSteps {
        com.wiggle.client.worker.CompensableActivity<Map<String, Object>, Map<String, Object>> one();
        Map<String, Object> two(Map<String, Object> ctx);
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "id-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static FlowSpec workflow() {
        return FlowSpec.define("wf", 1, Map.class, OneStep.class, (f, s) -> f.thenApply(s::a));
    }

    private static FlowSpec sagaWorkflow() {
        return FlowSpec.define("saga-wf", 1, Map.class, SagaSteps.class, (f, s) -> f
                .thenApplyCompensable(s::one)
                .thenApply(s::two));
    }

    @Test @DisplayName("a namespace-configured cell mints ns.e0.s0.<ulid> ids")
    void namespacedCellMintsEpochAwareIds() throws Exception {
        try (WiggleServer server = new WiggleServer(config().withNamespace("acme")).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(workflow());
            String id = client.start("wf", Map.of());
            IdCodec.Placement p = IdCodec.parse(id)
                    .orElseThrow(() -> new AssertionError("expected an epoch-aware id, got: " + id));
            assertEquals("acme", p.namespace());
            assertEquals(0, p.epoch());
            assertEquals(0, p.shard());
        }
    }

    @Test @DisplayName("the coordinator-supplied placement steers the epoch and shard a cell mints")
    void placementSteersMintedIds() throws Exception {
        try (WiggleServer server = new WiggleServer(config().withNamespace("acme")).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(workflow());
            // simulate the coordinator re-pointing this node (an epoch bump onto shards it owns)
            server.placement().set(3, new int[]{7});

            String id = client.start("wf", Map.of());
            IdCodec.Placement p = IdCodec.parse(id)
                    .orElseThrow(() -> new AssertionError("expected an epoch-aware id, got: " + id));
            assertEquals("acme", p.namespace());
            assertEquals(3, p.epoch(), "mints into the placement's epoch");
            assertEquals(7, p.shard(), "stamps a shard the cell owns");
        }
    }

    @Test @DisplayName("liveCountByEpoch groups running instances by the epoch in their id")
    void liveCensusGroupsByEpoch() throws Exception {
        try (WiggleServer server = new WiggleServer(config().withNamespace("acme")).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(workflow());
            client.start("wf", Map.of());              // epoch 0
            client.start("wf", Map.of());              // epoch 0
            server.placement().set(1, new int[]{0});   // coordinator bumps the epoch
            client.start("wf", Map.of());              // epoch 1

            Map<Long, Integer> live = server.engine().liveCountByEpoch();
            assertEquals(2, live.get(0L), "two running instances in epoch 0");
            assertEquals(1, live.get(1L), "one running instance in epoch 1");
        }
    }

    /**
     * A compensating instance is still in flight: its undo tokens are dispatched against the
     * epoch's ring. Counting only RUNNING made a draining epoch holding one look empty, and the
     * coordinator retires an epoch the census reports at zero -- stranding the reverse pass.
     */
    @Test @DisplayName("the census counts every live status, so a compensating instance holds its epoch")
    void liveCensusCountsCompensating() throws Exception {
        try (WiggleServer server = new WiggleServer(config().withNamespace("acme")).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            FlowSpec saga = sagaWorkflow();
            client.register(saga);
            Set<String> queues = saga.definition().queues();
            String id = client.start("saga-wf", Map.of());
            long epoch = IdCodec.parse(id).orElseThrow().epoch();

            TaskActivation one = claim(server, "w1", queues);
            server.engine().report(new WorkflowEngine.Run(one.taskId(), one.leaseOwner(),
                    List.of(new WorkflowEngine.StepInput(one.nodeId(), Map.of(), null)), true));
            TaskActivation two = claim(server, "w1", queues);
            server.engine().fail(two.taskId(), two.leaseOwner(), "boom", false);
            assertEquals("COMPENSATING", server.engine().instance(id).orElseThrow().status(),
                    "the reverse pass should own the instance now");

            assertEquals(1, server.engine().liveCountByEpoch().get(epoch),
                    "a compensating instance still holds its epoch against retirement");
        }
    }

    /** The census set must be exactly the enum's live set, whatever statuses exist. */
    @Test @DisplayName("the census counts a status if and only if the enum calls it live")
    void censusFollowsTheEnum() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            for (InstanceStatus s : InstanceStatus.values()) {
                storage.inTxVoid(tx -> {
                    Rows.Instance i = new Rows.Instance();
                    i.id = "wfi_" + s.name();
                    i.workflow = "w";
                    i.version = 1;
                    i.status = s;
                    tx.insertInstance(i);
                });
            }
            long counted = engine.liveCountByEpoch().values().stream().mapToInt(Integer::intValue).sum();
            long live = Arrays.stream(InstanceStatus.values()).filter(InstanceStatus::live).count();
            assertEquals(live, counted, "the census and InstanceStatus.live() disagree");
        }
    }

    private static TaskActivation claim(WiggleServer server, String worker, Set<String> queues) {
        List<TaskActivation> claimed = server.engine().poll(worker, queues, 1, 30_000L);
        assertEquals(1, claimed.size(), "expected exactly one dispatchable task");
        return claimed.getFirst();
    }

    @Test @DisplayName("a standalone cell (no namespace) keeps legacy wfi_ ids")
    void standaloneMintsLegacyIds() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(workflow());
            String id = client.start("wf", Map.of());
            assertTrue(IdCodec.isLegacy(id), "standalone id should be legacy: " + id);
            assertTrue(id.startsWith("wfi_"), "legacy id keeps the wfi_ prefix: " + id);
        }
    }
}
