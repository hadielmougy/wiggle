package dev.wiggle.server.cluster;

import dev.wiggle.core.Ids;
import dev.wiggle.server.store.Rows.ServerNode;
import dev.wiggle.server.store.Storage;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cluster membership and leader election, modelled on JobRunr's background job server
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
    /** Alive members as of the last heartbeat, so worker discovery never hits the DB. */
    private volatile List<ServerNode> aliveSnapshot = List.of();

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

    /**
     * The host:port workers should dial to reach this node. Set once before {@link #start()}
     * (the API must be bound first so the real port is known); every heartbeat persists it.
     */
    public void setAdvertisedAddress(String address) { self.advertisedAddress = address; }

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

    private void beat() {
        long now = System.currentTimeMillis();
        self.lastHeartbeat = now;
        boolean nowLeader = storage.inTx(tx -> {
            tx.upsertNode(self);
            tx.deleteNodesOlderThan(now - deadAfterMillis() * 4);
            List<ServerNode> alive = tx.nodes().stream()
                    .filter(n -> now - n.lastHeartbeat < deadAfterMillis())
                    .toList();
            this.aliveSnapshot = alive;
            Optional<ServerNode> elected = alive.stream()
                    .min((a, b) -> a.firstHeartbeat != b.firstHeartbeat
                            ? Long.compare(a.firstHeartbeat, b.firstHeartbeat)
                            : a.id.compareTo(b.id));
            boolean me = elected.map(n -> n.id.equals(self.id)).orElse(false);
            tx.setLeader(self.id, me);
            return me;
        });
        lastSuccessfulHeartbeat = now;
        boolean was = leader.getAndSet(nowLeader);
        if (was != nowLeader) {
            LOG.log(System.Logger.Level.INFO, () -> nowLeader
                    ? "node " + self.id + " became leader"
                    : "node " + self.id + " stepped down");
        }
    }

    public List<ServerNode> members() {
        return storage.inTx(tx -> tx.nodes());
    }

    /**
     * Alive members from the last heartbeat cache -- served to workers on discovery so
     * per-worker discovery load never touches the database.
     */
    public List<ServerNode> aliveMembers() {
        return aliveSnapshot;
    }

    @Override public void close() {
        scheduler.shutdownNow();
        leader.set(false);
        try {
            // Backdate our own heartbeat so the rest of the cluster re-elects immediately
            // instead of waiting out the full timeout window.
            self.lastHeartbeat = 0;
            storage.inTxVoid(tx -> {
                tx.upsertNode(self);
                tx.setLeader(self.id, false);
            });
        } catch (RuntimeException ignored) {
            // best effort on shutdown
        }
    }
}
