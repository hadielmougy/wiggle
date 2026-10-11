package com.wiggle.server.store;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.Criteria;
import com.wiggle.core.criteria.Predicate;
import com.wiggle.core.criteria.TaskFacts;
import com.wiggle.server.store.Rows.Token;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Which ready tasks a claim may take: those on {@code queues} (every queue when empty), of
 * {@code versions} (every version when empty), for which {@code where} holds (all when null).
 */
public record ClaimFilter(Set<String> queues, Set<WorkflowVersion> versions, Predicate where) {

    public ClaimFilter {
        queues = queues == null ? Set.of() : Collections.unmodifiableSet(new LinkedHashSet<>(queues));
        versions = versions == null ? Set.of() : Set.copyOf(versions);
    }

    /** Whether {@code t} passes the filter; the in-memory claim's test, and the reference for SQL. */
    public boolean admits(Token t) {
        if (!queues.isEmpty() && !queues.contains(t.queue)) return false;
        if (!versions.isEmpty() && !versions.contains(new WorkflowVersion(t.workflow, t.version))) return false;
        return where == null || Criteria.matches(where, facts(t));
    }

    public static TaskFacts facts(Token t) {
        return new TaskFacts(t.workflow, t.version, t.queue, TaskFacts.stepOf(t.workflow, t.activity), t.nodeId,
                t.kind == null ? null : t.kind.name(), t.nextAttempt(), t.correlationId, t.instCreatedAt, t.attributes);
    }
}
