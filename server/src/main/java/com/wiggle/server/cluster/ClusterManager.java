package com.wiggle.server.cluster;

import com.wiggle.core.Ids;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cluster membership and leader election, modelled on background job server
 * announce/heartbeat scheme:
 *
 *  - every node announces itself once and then heartbeats on a fixed interval;
 *  - a node is considered alive while its last heartbeat is within the timeout window;
 *  - the leader is the alive node with the earliest first heartbeat (longest-running),
 *    ties broken by id, so every node computes the same answer from the same table;
 *  - a node that finds its own heartbeat stale steps down before doing leader work.
 *
 * Election needs no consensus protocol because the shared database is the only source
 * of truth, and leader-only duties are all idempotent and re-entrant: a brief overlap
 * during failover duplicates work but never corrupts state.
 */
public final class ClusterManager implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ClusterManager.class.getName());

    private final Storage storage;
    private final ServerNode self = new ServerNode();
    private final long heartbeatIntervalMillis;
    private final int missedHeartbeatsBeforeDead;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-cluster");
                t.setDaemon(true);
                return t;
            });
    private final AtomicBoolean leader = new AtomicBoolean(false);
    private volatile long lastSuccessfulHeartbeat;
    /** The roster this node last saw while it was leader, to log joins/leaves once. */
    private final Set<String> knownMembers = ConcurrentHashMap.newKeySet();

    public ClusterManager(Storage storage, String name, int workers,
                          long heartbeatIntervalMillis, int missedHeartbeatsBeforeDead) {
        this.storage = storage;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.missedHeartbeatsBeforeDead = missedHeartbeatsBeforeDead;
        long now = System.currentTimeMillis();
        self.id = Ids.next("node");
        self.name = name;
        self.firstHeartbeat = now;
        self.lastHeartbeat = now;
        self.workers = workers;
    }

    public String nodeId() { return self.id; }

    public boolean isLeader() {
        // Fencing: a node whose own heartbeat has gone stale must not act as leader,
        // because the rest of the cluster has probably already elected someone else.
        return leader.get() && System.currentTimeMillis() - lastSuccessfulHeartbeat < deadAfterMillis();
    }

    public long deadAfterMillis() {
        return heartbeatIntervalMillis * missedHeartbeatsBeforeDead;
    }

    public void start() {
        beat();
        scheduler.scheduleAtFixedRate(this::safeBeat, heartbeatIntervalMillis,
                heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void safeBeat() {
        try {
            beat();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "heartbeat failed: " + e);
        }
    }

    /** The outcome of one heartbeat: who leads, who is alive, and whether that is us. */
    private record Election(String leaderId, Set<String> aliveIds, boolean self) {}

    /** Longest-running node wins; ties broken by id so every node computes the same answer. */
    private static final Comparator<ServerNode> BY_SENIORITY =
            Comparator.comparingLong((ServerNode n) -> n.firstHeartbeat).thenComparing(n -> n.id);

    private void beat() {
        long now = System.currentTimeMillis();
        self.lastHeartbeat = now;
        Election election = storage.inTx(tx -> heartbeatAndElect(tx, now));
        lastSuccessfulHeartbeat = now;
        boolean was = leader.getAndSet(election.self());
        LOG.log(System.Logger.Level.DEBUG, () -> "heartbeat: node " + self.id + " at " + now
                + ", " + election.aliveIds().size() + " alive node(s), elected leader=" + election.leaderId()
                + ", self leader=" + election.self());
        if (was != election.self()) {
            LOG.log(System.Logger.Level.INFO, () -> election.self()
                    ? "node " + self.id + " became leader"
                    : "node " + self.id + " stepped down");
        }
        logMembershipChanges(election.self(), election.aliveIds());
    }

    private Election heartbeatAndElect(Tx tx, long now) {
        tx.upsertNode(self);
        tx.deleteNodesOlderThan(now - deadAfterMillis() * 4);
        List<ServerNode> alive = tx.nodes().stream()
                .filter(n -> now - n.lastHeartbeat < deadAfterMillis())
                .toList();
        Optional<ServerNode> elected = alive.stream().min(BY_SENIORITY);
        boolean me = elected.map(n -> n.id.equals(self.id)).orElse(false);
        tx.setLeader(self.id, me);
        Set<String> aliveIds = new LinkedHashSet<>();
        alive.forEach(n -> aliveIds.add(n.id));
        return new Election(elected.map(n -> n.id).orElse(null), aliveIds, me);
    }

    /** The leader logs roster changes once, so a cluster's log shows joins/leaves without N-way duplication. */
    private void logMembershipChanges(boolean nowLeader, Set<String> aliveIds) {
        if (!nowLeader) {
            knownMembers.clear();   // re-sync from scratch if we regain leadership later
            return;
        }
        for (String id : aliveIds) {
            if (knownMembers.add(id)) {
                LOG.log(System.Logger.Level.INFO, () -> "cluster: node " + id + " joined (" + aliveIds.size() + " alive)");
            }
        }
        knownMembers.removeIf(id -> {
            if (!aliveIds.contains(id)) {
                LOG.log(System.Logger.Level.INFO, () -> "cluster: node " + id + " left (" + aliveIds.size() + " alive)");
                return true;
            }
            return false;
        });
    }

    public List<ServerNode> members() {
        return storage.inTx(tx -> tx.nodes());
    }

    @Override public void close() {
        scheduler.shutdownNow();
        leader.set(false);
        LOG.log(System.Logger.Level.DEBUG, () -> "node " + self.id + " closing, backdating heartbeat for immediate re-election");
        try {
            // Backdate our own heartbeat so the rest of the cluster re-elects immediately
            // instead of waiting out the full timeout window.
            self.lastHeartbeat = 0;
            storage.inTxVoid(tx -> {
                tx.upsertNode(self);
                tx.setLeader(self.id, false);
            });
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, () -> "node " + self.id + " best-effort shutdown heartbeat failed: " + e);
        }
    }
}
