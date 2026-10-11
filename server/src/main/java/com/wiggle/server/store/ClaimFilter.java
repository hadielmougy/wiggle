package com.wiggle.server.store;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.Criteria;
import com.wiggle.core.criteria.Predicate;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.TaskFacts;
import com.wiggle.server.store.Rows.Token;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Which ready tasks a claim may take: those on {@code queues} (every queue when empty), of
 * {@code versions} (every version when empty), for which the predicate of their queue in
 * {@code where} holds (all of a queue's tasks when it has none).
 */
public record ClaimFilter(Set<String> queues, Set<WorkflowVersion> versions, Map<String, Predicate> where) {

    public ClaimFilter {
        queues = queues == null ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(queues));
        versions = versions == null ? Set.of() : Set.copyOf(versions);
        where = where == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(where));
    }

    public static ClaimFilter of(Set<String> queues, Set<WorkflowVersion> versions) {
        return new ClaimFilter(queues, versions, Map.of());
    }

    /** A poll's criteria, one per queue: those queues, each narrowed by its own predicate. */
    public static ClaimFilter of(Collection<QueueCriteria> criteria, Set<WorkflowVersion> versions) {
        Set<String> queues = new LinkedHashSet<>();
        Map<String, Predicate> where = new LinkedHashMap<>();
        for (QueueCriteria c : criteria) {
            queues.add(c.queue());
            if (c.where() != null) where.put(c.queue(), c.where());
        }
        return new ClaimFilter(queues, versions, where);
    }

    /** Whether {@code t} passes the filter; the in-memory claim's test, and the reference for SQL. */
    public boolean admits(Token t) {
        if (!queues.isEmpty() && !queues.contains(t.queue)) return false;
        if (!versions.isEmpty() && !versions.contains(new WorkflowVersion(t.workflow, t.version))) return false;
        Predicate p = where.get(t.queue);
        return p == null || Criteria.matches(p, facts(t));
    }

    public static TaskFacts facts(Token t) {
        return new TaskFacts(t.workflow, t.version, t.queue, TaskFacts.stepOf(t.workflow, t.activity), t.nodeId,
                t.kind == null ? null : t.kind.name(), t.nextAttempt(), t.correlationId, t.instCreatedAt, t.attributes);
    }
}
