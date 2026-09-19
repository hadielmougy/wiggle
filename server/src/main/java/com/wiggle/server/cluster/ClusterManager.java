package com.wiggle.server.cluster;

import com.wiggle.core.Ids;
import com.wiggle.election.ElectionRule;
import com.wiggle.election.ElectionStore;
import com.wiggle.election.LeaderElection;
import com.wiggle.election.Member;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.server.store.Storage;

import java.util.List;

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

    private final Storage storage;
    private final ServerNode self = new ServerNode();
    private final LeaderElection election;

    public ClusterManager(Storage storage, String name, int workers,
                          long heartbeatIntervalMillis, int missedHeartbeatsBeforeDead) {
        this.storage = storage;
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
        return storage.inTx(tx -> tx.nodes());
    }

    @Override public void close() { election.close(); }

    /** The election's view of {@code wf_node}: one transaction per step, as the contract requires. */
    private final class NodeTableStore implements ElectionStore {

        @Override
        public List<Member> step(Member me, long pruneBefore, ElectionRule elect) {
            self.lastHeartbeat = me.lastHeartbeat();
            return storage.inTx(tx -> {
                tx.upsertNode(self);
                tx.deleteNodesOlderThan(pruneBefore);
                List<Member> roster = tx.nodes().stream().map(ClusterManager::member).toList();
                String leaderId = elect.leaderOf(roster);
                tx.setLeader(self.id, self.id.equals(leaderId));
                return roster;
            });
        }

        @Override public void standDown(Member me) {
            // Backdate our heartbeat so the rest of the cell re-elects immediately rather than
            // waiting out the whole timeout window.
            self.lastHeartbeat = 0;
            storage.inTxVoid(tx -> {
                tx.upsertNode(self);
                tx.setLeader(self.id, false);
            });
        }

        @Override public List<Member> members() {
            return storage.inTx(tx -> tx.nodes().stream().map(ClusterManager::member).toList());
        }
    }

    private static Member member(ServerNode n) {
        return new Member(n.id, n.name, n.firstHeartbeat, n.lastHeartbeat);
    }
}
