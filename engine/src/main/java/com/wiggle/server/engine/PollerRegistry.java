package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Who is currently polling, and for what. Every {@code PollTasks} call says which queues a worker
 * serves and — for a version-scoped worker — which (workflow, version) pairs it will claim; this
 * remembers the most recent of those per worker, for as long as the worker keeps polling.
 *
 * <p>It answers one question: <b>is there anything READY that no running worker can take?</b> A
 * queue nobody polls, or a version every worker has scoped itself out of, leaves tokens dispatchable
 * forever -- not failed, not retried, simply never claimed.
 *
 * <p>In memory and not durable: it is updated on the hottest path, so it costs a map write and
 * nothing else. It therefore knows only this node's pollers, so coverage is reported per node and a
 * console aggregates. An entry expires after {@link #ttlMillis} without a poll.
 */
public final class PollerRegistry {

    /** What one worker last told us it serves. Empty {@code versions} means every version. */
    public record Poller(String workerId, Set<String> queues, Set<WorkflowVersion> versions, long lastSeen) {}

    private final Map<String, Poller> pollers = new ConcurrentHashMap<>();
    private final long ttlMillis;

    public PollerRegistry(long ttlMillis) {
        this.ttlMillis = ttlMillis <= 0 ? 60_000 : ttlMillis;
    }

    public long ttlMillis() {
        return ttlMillis;
    }

    /** Records a poll. Called once per {@code PollTasks}, so it does no more than a map write. */
    public void seen(String workerId, Set<String> queues, Set<WorkflowVersion> versions, long now) {
        if (workerId == null || workerId.isBlank()) return;
        pollers.put(workerId, new Poller(workerId,
                queues == null ? Set.of() : Set.copyOf(queues),
                versions == null ? Set.of() : Set.copyOf(versions),
                now));
    }

    /** The workers that have polled within the TTL, dropping the ones that have not. */
    public Set<Poller> live(long now) {
        pollers.values().removeIf(p -> now - p.lastSeen() > ttlMillis);
        return new LinkedHashSet<>(pollers.values());
    }

    /**
     * Whether some live worker would claim a token on {@code queue} of this (workflow, version).
     *
     * <p>Both halves must hold: the worker polls that queue, and either it is unscoped (serves every
     * version) or it named this one. That mirrors the claim exactly — a version-scoped worker filters
     * its claim by the same pairs — so this answers what the dispatcher would actually do.
     */
    public boolean covers(String workflow, int version, String queue, long now) {
        WorkflowVersion wv = new WorkflowVersion(workflow, version);
        for (Poller p : live(now)) {
            boolean servesQueue = p.queues().isEmpty() || p.queues().contains(queue);
            boolean servesVersion = p.versions().isEmpty() || p.versions().contains(wv);
            if (servesQueue && servesVersion) return true;
        }
        return false;
    }
}
