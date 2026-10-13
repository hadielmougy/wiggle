package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.Criteria;
import com.wiggle.core.criteria.Predicate;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.server.store.ClaimFilter;

import java.util.LinkedHashSet;
import java.util.List;
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

    /** What one worker last told us it serves. Empty {@code versions} means every version; each of
     *  {@code queues} carries its criteria, and no queues means every queue. */
    public record Poller(String workerId, ClaimFilter filter, long lastSeen) {}

    private final Map<String, Poller> pollers = new ConcurrentHashMap<>();
    private final long ttlMillis;

    public PollerRegistry(long ttlMillis) {
        this.ttlMillis = ttlMillis <= 0 ? 60_000 : ttlMillis;
    }

    public long ttlMillis() {
        return ttlMillis;
    }

    /** Records a poll. Called once per {@code PollTasks}, so it does no more than a map write. */
    public void seen(String workerId, ClaimFilter filter, long now) {
        if (workerId == null || workerId.isBlank()) return;
        pollers.put(workerId, new Poller(workerId, filter, now));
    }

    /** The workers that have polled within the TTL, dropping the ones that have not. */
    public Set<Poller> live(long now) {
        pollers.values().removeIf(p -> now - p.lastSeen() > ttlMillis);
        return new LinkedHashSet<>(pollers.values());
    }

    public Coverage coverage(WorkflowVersion wv, String queue, long now) {
        Set<Predicate> conditions = new LinkedHashSet<>();
        for (Poller p : live(now)) {
            List<QueueCriteria> queues = p.filter().queues();
            Set<WorkflowVersion> versions = p.filter().versions();
            QueueCriteria forQueue = QueueCriteria.forQueue(queues, queue);
            boolean servesQueue = queues.isEmpty() || forQueue != null;
            boolean servesVersion = versions.isEmpty() || versions.contains(wv);
            if (!servesQueue || !servesVersion) continue;
            Predicate where = forQueue == null ? null : forQueue.where();
            switch (onSlice(where, wv)) {
                case TRUE -> { return new Coverage(Coverage.State.COVERED, List.of()); }
                case UNKNOWN -> conditions.add(where);
                case FALSE -> { }
            }
        }
        return conditions.isEmpty() ? new Coverage(Coverage.State.UNCOVERED, List.of())
                : new Coverage(Coverage.State.CONDITIONAL, List.copyOf(conditions));
    }

    /** A slice's coverage; {@code conditions} are the criteria of a CONDITIONAL one, else empty. */
    public record Coverage(State state, List<Predicate> conditions) {
        public enum State { COVERED, CONDITIONAL, UNCOVERED }
    }

    /** {@code where}, the criteria of the slice's queue, on what the slice fixes: TRUE when null. */
    private static Criteria.Truth onSlice(Predicate where, WorkflowVersion wv) {
        if (where == null) return Criteria.Truth.TRUE;
        return Criteria.evaluate(where, (f, attribute) -> switch (f) {
            case WORKFLOW -> wv.workflow();
            case VERSION -> (long) wv.version();
            default -> Criteria.UNKNOWN;
        });
    }
}
