package com.wiggle.coordinator.ratis;

import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.CoordinatorStoreProvider;
import org.apache.ratis.client.RaftClient;

/**
 * DESIGN SKETCH — not wired into the build. The {@link CoordinatorStoreProvider} seam for the embedded
 * Ratis+RocksDB backend, resolved by {@code dist} the same way JDBC/Cassandra/etcd are. Boots (or joins)
 * the coordinator's Ratis group + RocksDB-backed {@link CoordStateMachine} and hands back a client-side
 * {@link RatisCoordinatorStore}. A single-member group is the zero-dependency dev mode.
 */
public final class RatisCoordinatorStoreProvider implements CoordinatorStoreProvider {

    private final RaftClient client;   // built from parsed peers; the RaftServer+RocksDB boot is omitted here

    /** e.g. from {@code WIGGLE_COORD_STORE=ratis:///var/lib/wiggle/coord?peers=host1,host2,host3}. */
    public RatisCoordinatorStoreProvider(String uri) {
        // TODO: parse uri -> (dataDir, groupId, peers); start RaftServer with CoordStateMachine over
        //       RocksDB at dataDir if this node is a member; build a RaftClient to the group.
        this.client = null;
    }

    @Override public CoordinatorStore coordinatorStore() {
        return new RatisCoordinatorStore(client);
    }
}
