package com.wiggle.tests;

import com.wiggle.boot.test.WiggleTestNode;
import com.wiggle.boot.test.WiggleTestNodes;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.Json;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Nodes on one database form one cluster: one leader among them, and work started on one is run through another. */
class ClusterNodesTest {

    public interface Steps {
        Map<String, Object> mark(Map<String, Object> ctx);
    }

    public static final class Handlers implements Steps {
        @Override public Map<String, Object> mark(Map<String, Object> ctx) {
            Map<String, Object> next = new LinkedHashMap<>(ctx);
            next.put("marked", true);
            return next;
        }
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) throw new AssertionError("timed out waiting until " + what);
            Thread.sleep(50);
        }
    }

    private static long leaders(WiggleTestNodes nodes, int... running) {
        long n = 0;
        for (int i : running) if (nodes.node(i).server().cluster().isLeader()) n++;
        return n;
    }

    @Test @DisplayName("two nodes elect one leader, and the other takes over when it stops")
    void oneLeaderAndFailover() throws Exception {
        try (WiggleTestNodes nodes = WiggleTestNodes.start(2, Map.of())) {
            await(() -> leaders(nodes, 0, 1) == 1, "one of the two leads");
            int leader = nodes.node(0).server().cluster().isLeader() ? 0 : 1;
            int other = 1 - leader;
            nodes.stop(leader);
            await(() -> nodes.node(other).server().cluster().isLeader(), "node " + other + " takes over");
        }
    }

    @Test @DisplayName("an instance started through one node is run by a worker polling the other")
    void workCrossesNodes() throws Exception {
        @SuppressWarnings("unchecked")
        FlowSpec flow = FlowSpec.define("cluster-nodes", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::mark));
        try (WiggleTestNodes nodes = WiggleTestNodes.start(2, Map.of())) {
            WiggleTestNode first = nodes.node(0);
            WiggleTestNode second = nodes.node(1);
            try (WiggleClient starter = first.client();
                 WiggleClient polling = second.client();
                 Worker worker = new Worker(polling, "cluster-worker").registerHandler("cluster-nodes", new Handlers())) {
                starter.register(flow);
                worker.start();
                String id = starter.start(flow, Map.of());
                assertEquals("COMPLETED", starter.awaitCompletion(id, Duration.ofSeconds(15)).status());
                assertEquals(true, Json.asObject(polling.instance(id).context()).get("marked"));
            }
        }
    }
}
