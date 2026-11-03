package com.wiggle.tests;

import com.wiggle.boot.test.WiggleBootTest;
import com.wiggle.boot.test.WiggleTestNode;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A worker that closes while its long poll is parked leaves nothing claimed on the server: work started
 * the moment after goes to a worker still polling, not to the closed one's poll for a lease's length.
 */
@WiggleBootTest
@TestPropertySource(properties = "WIGGLE_LONGPOLL_MAX_MILLIS=10000")
class WorkerCloseTest {

    @Autowired WiggleTestNode node;

    public interface Steps {
        Map<String, Object> work(Map<String, Object> ctx);
    }

    public static final class Handlers implements Steps {
        @Override public Map<String, Object> work(Map<String, Object> ctx) {
            Map<String, Object> next = new HashMap<>(ctx);
            next.put("done", true);
            return next;
        }
    }

    private void awaitPolling(String workerId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (node.server().engine().livePollers().stream().noneMatch(p -> p.workerId().equals(workerId))) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError(workerId + " never polled");
            Thread.sleep(10);
        }
        Thread.sleep(50);
    }

    @Test @DisplayName("work started right after a worker closes mid-poll goes to the worker still polling")
    void closedWorkerStrandsNothing() throws Exception {
        @SuppressWarnings("unchecked")
        FlowSpec flow = FlowSpec.define("worker-close", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
        try (WiggleClient client = node.client()) {
            client.register(flow);
            Worker leaving = new Worker(client, "leaving").registerHandler("worker-close", new Handlers());
            leaving.start();
            awaitPolling("leaving");   // its poll is on the server, parked for up to 10 s
            long closing = System.currentTimeMillis();
            leaving.close();
            assertTrue(System.currentTimeMillis() - closing < 3_000, "closing does not wait out the long poll");

            String id = client.start(flow, Map.of());
            Thread.sleep(500);   // a poll the closed worker left on the server would have claimed it by now
            try (Worker staying = new Worker(client, "staying").registerHandler("worker-close", new Handlers())) {
                staying.start();
                assertEquals("COMPLETED", client.awaitCompletion(id, Duration.ofSeconds(5)).status(),
                        "not left to the closed worker's poll until its 30 s lease ran out");
            }
        }
    }
}
