package com.wiggle.tests;

import com.wiggle.core.Tls;
import com.wiggle.dist.coord.CoordinatorLink;
import com.wiggle.dist.coord.HttpCoordinatorLink;
import com.wiggle.proto.CoordinatorHeartbeatResponse;
import com.wiggle.proto.NodeConfig;
import com.wiggle.proto.RegisterResponse;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.proto.RingSlot;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordinatorApi;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 1 / T7: node lifecycle. Coordinator-side register/heartbeat/deregister/fetchConfig logic, plus
 * a real gRPC round-trip driving the node-side {@link HttpCoordinatorLink} against an in-process
 * {@link CoordinatorApi}.
 */
class NodeLifecycleTest {

    @Test @DisplayName("coordinator-side register/heartbeat/deregister/fetchConfig logic")
    void coordinatorSideLogic() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        try (CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED)) {
            RegisterResponse reg = api.doRegister("acme", RegisteredNode.newBuilder()
                    .setName("node-a").setEndpoint("grpc://h:1").setEngineVersion("2.1.5").build());
            assertFalse(reg.getNodeId().isBlank());
            assertTrue(reg.getHeartbeatIntervalSeconds() > 0);
            assertEquals(1, store.nodes("acme").size());

            // heartbeat touches liveness and returns the namespace's generation (policy revision)
            api.doOpenEpoch("acme", List.of(RingSlot.newBuilder().setShard(0).setCellId("cell-3").build()));
            CoordinatorHeartbeatResponse hb = api.doHeartbeat(reg.getNodeId(), 0);
            assertTrue(hb.getOk());
            assertEquals(1, hb.getConfigGeneration(), "generation follows the policy revision");

            // an unknown node is told it is not registered
            assertFalse(api.doHeartbeat("no-such-node", 0).getOk());

            // fetchConfig reports the same generation
            NodeConfig cfg = api.doFetchConfig("acme");
            assertEquals(1, cfg.getGeneration());

            api.doDeregister(reg.getNodeId());
            assertEquals(0, store.nodes("acme").size());
        }
    }

    @Test @DisplayName("node-side link registers, heartbeats, and deregisters over real gRPC")
    void nodeToCoordinatorRoundTrip() throws Exception {
        InMemoryCoordinatorStore store = new InMemoryCoordinatorStore();
        CoordinatorApi api = new CoordinatorApi(store, 0, Tls.Options.DISABLED);
        api.start();
        try {
            String url = "127.0.0.1:" + api.port();
            try (HttpCoordinatorLink link = new HttpCoordinatorLink(url)) {
                link.register(new CoordinatorLink.NodeInfo(
                        "node-a", "acme", "", "127.0.0.1:9999", "2.1.5"), (CoordinatorLink.CellRuntime) null);

                List<CoordNode> roster = store.nodes("acme");
                assertEquals(1, roster.size(), "register landed in the coordinator roster");
                assertEquals("127.0.0.1:9999", roster.get(0).endpoint());
                long hb0 = roster.get(0).lastHeartbeat();

                Thread.sleep(5);
                link.heartbeat();
                assertTrue(store.nodes("acme").get(0).lastHeartbeat() >= hb0, "heartbeat advanced liveness");
            }
            // close() deregistered the node
            assertEquals(0, store.nodes("acme").size(), "close() deregistered the node");
        } finally {
            api.close();
        }
    }
}
