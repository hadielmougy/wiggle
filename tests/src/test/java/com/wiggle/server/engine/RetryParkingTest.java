package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Ids;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.TokenStatus;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A retry backing off at least {@code wiggle.retry.timerMinMillis} waits WAITING, out of the dispatch
 * index, until the leader's sweep promotes it; a shorter one waits READY. The threshold is lowered
 * here so the backoffs are milliseconds.
 */
class RetryParkingTest {

    interface OneStep {
        Map<String, Object> work(Map<String, Object> ctx);
    }

    private Storage storage;
    private WorkflowEngine engine;

    @BeforeEach
    void open() {
        System.setProperty("wiggle.retry.timerMinMillis", "100");
        storage = new InMemoryStorage();
        storage.migrate();
        engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
    }

    @AfterEach
    void close() {
        System.clearProperty("wiggle.retry.timerMinMillis");
        storage.close();
    }

    private FlowSpec oneStep(Duration backoff) {
        return FlowSpec.define("retry-" + Ids.next("wf"), 1, Map.class, OneStep.class,
                (f, s) -> f.thenApply(s::work, RetryPolicy.fixed(3, backoff)));
    }

    /** Starts one instance, claims its step and fails it once, retryably. */
    private Token failOnce(FlowSpec bp) {
        engine.register(bp.definition());
        String id = engine.start(bp.name(), bp.version(), Map.of(), null);
        List<TaskActivation> got = engine.poll("w", bp.definition().workerQueues(), 1, null);
        assertEquals(1, got.size());
        engine.fail(got.getFirst().taskId(), "w", "boom", true);
        return engine.tokens(id).stream().filter(t -> t.id.equals(got.getFirst().taskId())).findFirst().orElseThrow();
    }

    @Test @DisplayName("a long backoff parks the retry WAITING; the sweep makes it dispatchable once due")
    void longBackoffParksUntilPromoted() throws Exception {
        FlowSpec bp = oneStep(Duration.ofMillis(300));
        Token parked = failOnce(bp);
        assertEquals(TokenStatus.WAITING, parked.status, "off the dispatch index while it backs off");
        assertTrue(engine.poll("w", bp.definition().workerQueues(), 1, null).isEmpty(), "not claimable");
        assertEquals(0, engine.promoteDueRetries(10), "not due yet");

        Thread.sleep(350);
        assertEquals(1, engine.promoteDueRetries(10));
        List<TaskActivation> retry = engine.poll("w", bp.definition().workerQueues(), 1, null);
        assertEquals(1, retry.size(), "dispatchable again");
        assertEquals(parked.id, retry.getFirst().taskId(), "the same task");
        assertEquals(2, retry.getFirst().attempt(), "its second attempt");
    }

    @Test @DisplayName("a short backoff waits READY, as before")
    void shortBackoffStaysReady() throws Exception {
        FlowSpec bp = oneStep(Duration.ofMillis(20));
        Token waiting = failOnce(bp);
        assertEquals(TokenStatus.READY, waiting.status);
        Thread.sleep(40);
        assertEquals(0, engine.promoteDueRetries(10), "nothing parked to promote");
        assertEquals(1, engine.poll("w", bp.definition().workerQueues(), 1, null).size());
    }

    @Test @DisplayName("a parked retry of a cancelled instance is not brought back")
    void cancelledInstanceIsNotPromoted() throws Exception {
        FlowSpec bp = oneStep(Duration.ofMillis(150));
        Token parked = failOnce(bp);
        engine.cancel(parked.instanceId, "stop");
        Thread.sleep(200);
        assertEquals(0, engine.promoteDueRetries(10));
        Token after = engine.tokens(parked.instanceId).stream().filter(t -> t.id.equals(parked.id)).findFirst().orElseThrow();
        assertEquals(TokenStatus.CANCELLED, after.status);
    }
}
