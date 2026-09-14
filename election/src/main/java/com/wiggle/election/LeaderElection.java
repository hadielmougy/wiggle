package com.wiggle.election;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Leader election by announce-and-heartbeat over a shared table:
 *
 * <ul>
 *   <li>every process announces itself once, then heartbeats on a fixed interval;</li>
 *   <li>a process is alive while its last heartbeat is inside the timeout window;</li>
 *   <li>the leader is the alive process with the earliest first heartbeat -- the longest-running --
 *       with ties broken by id, so every process computes the same answer from the same table;</li>
 *   <li>a process whose own heartbeat has gone stale stands down before doing any leader work.</li>
 * </ul>
 *
 * <p>No consensus protocol, because there is nothing to agree on: the shared table is the only
 * source of truth, and the rule over it is a pure function every process evaluates identically.
 * What makes that safe is that leader-only duties are idempotent and re-entrant -- a brief overlap
 * during a failover duplicates work but cannot corrupt state. An election that had to hand out
 * exclusive access would need real consensus; this one does not.
 *
 * <p>Both the cell engine and the control plane run this, each over its own {@link ElectionStore}.
 */
public final class LeaderElection implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(LeaderElection.class.getName());

    /** Longest-running wins; ties broken by id so every process computes the same answer. */
    private static final Comparator<Member> BY_SENIORITY =
            Comparator.comparingLong(Member::firstHeartbeat).thenComparing(Member::id);

    private final ElectionStore store;
    private final long heartbeatIntervalMillis;
    private final int missedHeartbeatsBeforeDead;
    private final String role;

    private volatile Member self;
    private final AtomicBoolean leader = new AtomicBoolean(false);
    private volatile long lastSuccessfulHeartbeat;
    /** The roster last seen while leader, so joins and leaves are logged once rather than N times. */
    private final Set<String> knownMembers = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService scheduler;

    /**
     * @param role              short label for thread name and logs, e.g. {@code "node"} or {@code "coordinator"}
     * @param heartbeatIntervalMillis how often to check in
     * @param missedHeartbeatsBeforeDead how many missed beats before a process counts as dead
     */
    public LeaderElection(ElectionStore store, Member self, String role,
                          long heartbeatIntervalMillis, int missedHeartbeatsBeforeDead) {
        this.store = store;
        this.self = self;
        this.role = role;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.missedHeartbeatsBeforeDead = missedHeartbeatsBeforeDead;
        this.lastSuccessfulHeartbeat = self.firstHeartbeat();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wiggle-" + role + "-election");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Who leads among {@code roster} at {@code now}, or null if nobody is alive. Pure, and public
     * because it is the whole rule: worth being able to read and test on its own.
     */
    public static String electedLeader(Collection<Member> roster, long now, long deadAfterMillis) {
        return roster.stream()
                .filter(m -> now - m.lastHeartbeat() < deadAfterMillis)
                .min(BY_SENIORITY)
                .map(Member::id)
                .orElse(null);
    }

    public String nodeId() {
        return self.id();
    }

    public long deadAfterMillis() {
        return heartbeatIntervalMillis * missedHeartbeatsBeforeDead;
    }

    /**
     * Whether this process may act as leader. Fenced on its own heartbeat: one that has gone stale
     * must not act, because the rest of the cluster has most likely elected someone else already.
     */
    public boolean isLeader() {
        return leader.get() && System.currentTimeMillis() - lastSuccessfulHeartbeat < deadAfterMillis();
    }

    public List<Member> members() {
        return store.members();
    }

    /** Beats once inline -- so {@link #isLeader()} is meaningful on return -- then on a timer. */
    public LeaderElection start() {
        beat();
        scheduler.scheduleAtFixedRate(this::safeBeat, heartbeatIntervalMillis,
                heartbeatIntervalMillis, TimeUnit.MILLISECONDS);
        return this;
    }

    private void safeBeat() {
        try {
            beat();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, role + " heartbeat failed: " + e);
        }
    }

    private void beat() {
        long now = System.currentTimeMillis();
        synchronized (this) {
            self = self.withHeartbeat(now);
        }
        Set<String> aliveIds = new LinkedHashSet<>();
        boolean[] won = {false};
        store.step(self, now - deadAfterMillis() * 4, roster -> {
            for (Member m : roster) {
                if (now - m.lastHeartbeat() < deadAfterMillis()) aliveIds.add(m.id());
            }
            String elected = electedLeader(roster, now, deadAfterMillis());
            won[0] = self.id().equals(elected);
            return elected;
        });

        lastSuccessfulHeartbeat = now;
        boolean was = leader.getAndSet(won[0]);
        if (was != won[0]) {
            LOG.log(System.Logger.Level.INFO, () -> role + " " + self.id()
                    + (won[0] ? " became leader" : " stepped down"));
        }
        logMembershipChanges(won[0], aliveIds);
    }

    /** The leader logs roster changes, so the log shows joins and leaves without N-way duplication. */
    private void logMembershipChanges(boolean nowLeader, Set<String> aliveIds) {
        if (!nowLeader) {
            knownMembers.clear();   // re-sync from scratch if leadership comes back
            return;
        }
        for (String id : aliveIds) {
            if (knownMembers.add(id)) {
                LOG.log(System.Logger.Level.INFO,
                        () -> role + ": " + id + " joined (" + aliveIds.size() + " alive)");
            }
        }
        knownMembers.removeIf(id -> {
            if (!aliveIds.contains(id)) {
                LOG.log(System.Logger.Level.INFO,
                        () -> role + ": " + id + " left (" + aliveIds.size() + " alive)");
                return true;
            }
            return false;
        });
    }

    @Override public void close() {
        scheduler.shutdownNow();
        leader.set(false);
        try {
            store.standDown(self);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG,
                    () -> role + " " + self.id() + " best-effort stand-down failed: " + e);
        }
    }

    /** The current leader as this process last computed it, or empty if it does not know. */
    public Optional<String> leaderId() {
        long now = System.currentTimeMillis();
        return Optional.ofNullable(electedLeader(store.members(), now, deadAfterMillis()));
    }
}
