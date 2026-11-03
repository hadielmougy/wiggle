package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.TaskActivation;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A worker that stops polling ends its polls in progress: they return at once and claim nothing more. */
class StopPollingTest {

    interface OneStep {
        Map<String, Object> work(Map<String, Object> ctx);
    }

    @SuppressWarnings("unchecked")
    private static final FlowSpec FLOW = FlowSpec.define("stop-polling", 1, Map.class, OneStep.class,
            (f, s) -> f.thenApply(s::work));

    /** {@code workerId}'s long poll on the flow's queues, waiting up to {@code waitMillis}, in a thread of its own. */
    private static CompletableFuture<List<TaskActivation>> longPoll(WorkflowEngine engine, String workerId, long waitMillis) {
        return CompletableFuture.supplyAsync(() -> engine.poll(workerId, FLOW.definition().queues(), 10, null,
                System.currentTimeMillis() + waitMillis));
    }

    /** Waits until {@code workerId} has a poll in progress, ending it as the check does. */
    private static void awaitParkedThenStop(WorkflowEngine engine, String workerId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (engine.stopPolling(workerId) == 0) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError(workerId + " never polled");
            Thread.sleep(5);
        }
    }

    @Test @DisplayName("a parked poll returns empty at once, and the work started after goes to a poll still open")
    void stoppedPollClaimsNothing() throws Exception {
        try (Storage storage = PostgresStorageFactory.inMemory()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            registry.register(FLOW.definition());

            CompletableFuture<List<TaskActivation>> stopped = longPoll(engine, "leaving", 10_000);
            long stoppedAt = System.currentTimeMillis();
            awaitParkedThenStop(engine, "leaving");
            assertEquals(List.of(), stopped.get(2, TimeUnit.SECONDS), "a stopped poll claims nothing");
            assertTrue(System.currentTimeMillis() - stoppedAt < 2_000, "and does not wait out its deadline");

            CompletableFuture<List<TaskActivation>> staying = longPoll(engine, "staying", 10_000);
            String id = engine.start(FLOW.name(), FLOW.version(), Map.of(), null);
            List<TaskActivation> claimed = staying.get(5, TimeUnit.SECONDS);
            assertEquals(1, claimed.size(), "the work went to the poll that stayed");
            assertEquals(id, claimed.getFirst().instanceId());
        }
    }

    @Test @DisplayName("stopping one worker leaves another worker's poll waiting")
    void otherWorkersKeepPolling() throws Exception {
        try (Storage storage = PostgresStorageFactory.inMemory()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            registry.register(FLOW.definition());

            CompletableFuture<List<TaskActivation>> other = longPoll(engine, "other", 10_000);
            CompletableFuture<List<TaskActivation>> leaving = longPoll(engine, "leaving", 10_000);
            awaitParkedThenStop(engine, "leaving");
            assertEquals(List.of(), leaving.get(2, TimeUnit.SECONDS));
            assertTrue(!other.isDone(), "the other worker's poll is still waiting");

            engine.start(FLOW.name(), FLOW.version(), Map.of(), null);
            assertEquals(1, other.get(5, TimeUnit.SECONDS).size());
        }
    }
}
