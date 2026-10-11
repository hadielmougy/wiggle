package com.wiggle.server.store;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.core.criteria.Criteria;
import com.wiggle.core.criteria.QueueCriteria;
import com.wiggle.core.criteria.TaskFacts;
import com.wiggle.server.store.Rows.Token;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Which ready tasks a claim may take: those on one of {@code queues} that its criteria admit (every
 * queue when the list is empty), of {@code versions} (every version when empty). A poll by queue
 * names is one {@link QueueCriteria} per name, without a predicate.
 */
public record ClaimFilter(List<QueueCriteria> queues, Set<WorkflowVersion> versions) {

    public ClaimFilter {
        queues = queues == null || queues.isEmpty() ? List.of() : QueueCriteria.forPoll(queues);
        versions = versions == null ? Set.of() : Set.copyOf(versions);
    }

    /** A poll by queue names: every task on each of {@code names}; every queue when empty. */
    public static ClaimFilter byName(Collection<String> names, Set<WorkflowVersion> versions) {
        return new ClaimFilter(names == null ? List.of() : names.stream().map(QueueCriteria::from).toList(), versions);
    }

    /** The queues named, in order; empty for every queue. */
    public Set<String> queueNames() {
        Set<String> out = new LinkedHashSet<>();
        for (QueueCriteria c : queues) out.add(c.queue());
        return out;
    }

    /** Whether {@code t} passes the filter; the in-memory claim's test, and the reference for SQL. */
    public boolean admits(Token t) {
        if (!versions.isEmpty() && !versions.contains(new WorkflowVersion(t.workflow, t.version))) return false;
        if (queues.isEmpty()) return true;
        QueueCriteria c = QueueCriteria.forQueue(queues, t.queue);
        return c != null && (c.where() == null || Criteria.matches(c.where(), facts(t)));
    }

    public static TaskFacts facts(Token t) {
        return new TaskFacts(t.workflow, t.version, t.queue, TaskFacts.stepOf(t.workflow, t.activity), t.nodeId,
                t.kind == null ? null : t.kind.name(), t.nextAttempt(), t.correlationId, t.instCreatedAt);
    }
}
