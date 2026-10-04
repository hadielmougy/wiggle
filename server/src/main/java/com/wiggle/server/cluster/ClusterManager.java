package com.wiggle.server.cluster;

import com.wiggle.core.Ids;
import com.wiggle.election.ElectionRule;
import com.wiggle.election.ElectionStore;
import com.wiggle.election.LeaderElection;
import com.wiggle.election.Member;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.server.store.Storage;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Cell membership and leader election over the engine's node table.
 *
 * <p>The election itself is {@link LeaderElection} in {@code :election}, shared with the control
 * plane so both run the same rule: announce and heartbeat, longest-running alive process leads, a
 * process with a stale heartbeat stands down. What stays here is the part that is specific to a
 * cell -- the {@code wf_node} row, which also carries the worker count and the leader flag the
 * dashboard reads.
 */
public final class ClusterManager implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ClusterManager.class.getName());

    private final Storage storage;
    private final ServerNode self = new ServerNode();
    private final LeaderElection election;
    private final LongSupplier generationInForce;
    /** "node id:generation" pairs already warned about, so each lagging node is named once. */
    private final Set<String> warned = ConcurrentHashMap.newKeySet();

    public ClusterManager(Storage storage, String name, int workers,
                          long heartbeatIntervalMillis, int missedHeartbeatsBeforeDead) {
        this(storage, name, workers, heartbeatIntervalMillis, missedHeartbeatsBeforeDead, 0, () -> 0);
    }

    /**
     * @param loadedGeneration  the newest storage-topology generation this node has loaded, published in
     *                          its node row
     * @param generationInForce the generation in force now; while leader, this node warns about every
     *                          live node that has not loaded it
     */
    public ClusterManager(Storage storage, String name, int workers, long heartbeatIntervalMillis,
                          int missedHeartbeatsBeforeDead, long loadedGeneration, LongSupplier generationInForce) {
        this.storage = storage;
        this.generationInForce = generationInForce;
        self.topologyGeneration = loadedGeneration;
        long now = System.currentTimeMillis();
        self.id = Ids.next("node");
        self.name = name;
        self.firstHeartbeat = now;
        self.lastHeartbeat = now;
        self.workers = workers;
        this.election = new LeaderElection(new NodeTableStore(),
                new Member(self.id, name, now, now), "node",
                heartbeatIntervalMillis, missedHeartbeatsBeforeDead);
    }

    public String nodeId() { return self.id; }

    public boolean isLeader() { return election.isLeader(); }

    public long deadAfterMillis() { return election.deadAfterMillis(); }

    public void start() { election.start(); }

    /** The full node rows -- worker counts and leader flag included -- for the dashboard. */
    public List<ServerNode> members() {
        return storage.inHome(tx -> tx.nodes());
    }

    @Override public void close() { election.close(); }

    /** The election's view of {@code wf_node}: one transaction per step, as the contract requires. */
    private final class NodeTableStore implements ElectionStore {

        @Override
        public List<Member> step(Member me, long pruneBefore, ElectionRule elect) {
            self.lastHeartbeat = me.lastHeartbeat();
            return storage.inHome(tx -> {
                tx.upsertNode(self);
                tx.deleteNodesOlderThan(pruneBefore);
                List<ServerNode> nodes = tx.nodes();
                List<Member> roster = nodes.stream().map(ClusterManager::member).toList();
                String leaderId = elect.leaderOf(roster);
                tx.setLeader(self.id, self.id.equals(leaderId));
                if (self.id.equals(leaderId)) warnLagging(nodes);
                return roster;
            });
        }

        /** Names, once each, the live nodes that have not loaded the topology generation in force. */
        private void warnLagging(List<ServerNode> nodes) {
            long required = generationInForce.getAsLong();
            for (ServerNode n : nodes) {
                if (n.topologyGeneration >= required || !warned.add(n.id + ":" + required)) continue;
                LOG.log(System.Logger.Level.WARNING, () -> "node '" + n.name + "' (" + n.id + ") runs storage "
                        + "topology generation " + n.topologyGeneration + ", but generation " + required
                        + " is in force; it may mint on the old placement and cannot route to new shards");
            }
        }

        @Override public void standDown(Member me) {
            // Backdate our heartbeat so the rest of the cell re-elects immediately rather than
            // waiting out the whole timeout window.
            self.lastHeartbeat = 0;
            storage.inHome(tx -> {
                tx.upsertNode(self);
                tx.setLeader(self.id, false);
                return null;
            });
        }

        @Override public List<Member> members() {
            return storage.inHome(tx -> tx.nodes().stream().map(ClusterManager::member).toList());
        }
    }

    private static Member member(ServerNode n) {
        return new Member(n.id, n.name, n.firstHeartbeat, n.lastHeartbeat);
    }
}
