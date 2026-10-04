package com.wiggle.server.cluster;

import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.ReplicatedStorage;
import com.wiggle.server.store.ReplicatedStorage.Fallback;
import com.wiggle.server.store.ReplicatedStorage.Named;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** The monitor ties the leader's heartbeat to every node's probe, which is what puts a replica in service. */
class ReplicaMonitorTest {

    @Test @DisplayName("once the leader stamps the heartbeat, the probe finds the replica within its lag")
    void aReplicaComesIntoService() throws Exception {
        InMemoryStorage primary = new InMemoryStorage();
        // a "replica" that is the primary itself: zero lag, so it serves as soon as a heartbeat exists
        ReplicatedStorage storage = new ReplicatedStorage(0, primary, List.of(new Named("#1", primary)),
                2_000, Fallback.PRIMARY);
        storage.migrate();
        try (ClusterManager cluster = new ClusterManager(storage, "monitor-node", 1, 50, 3);
             ReplicaMonitor monitor = new ReplicaMonitor(storage, cluster, 50)) {
            cluster.start();
            monitor.start();
            long deadline = System.currentTimeMillis() + 5_000;
            while (!storage.replicaStatus().getFirst().healthy() && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
            }
            assertTrue(storage.replicaStatus().getFirst().healthy(), storage.replicaStatus().toString());
        }
    }
}
