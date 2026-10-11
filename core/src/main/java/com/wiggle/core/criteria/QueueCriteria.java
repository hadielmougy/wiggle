package com.wiggle.core.criteria;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Function;

/**
 * What a poll may claim: ready tasks on one of {@code queues} for which {@code where} holds (every
 * task on them when {@code where} is null). Immutable, with value equality; validated on creation
 * against the limits in {@link Criteria}.
 *
 * <pre>{@code
 * QueueCriteria.from("payments")
 *         .where(cb -> cb.and(
 *                 cb.equal(Task_.WORKFLOW, "orders"),
 *                 cb.lessThan(Task_.ATTEMPT, 3)));
 * }</pre>
 */
public record QueueCriteria(List<String> queues, Predicate where) {

    public QueueCriteria {
        queues = queues == null ? List.of() : List.copyOf(new LinkedHashSet<>(queues));
        Criteria.validate(queues, where);
    }

    public static QueueCriteria from(String... queues) {
        return new QueueCriteria(List.of(queues), null);
    }

    public QueueCriteria where(Function<CriteriaBuilder, Predicate> build) {
        return where(build.apply(CriteriaBuilder.INSTANCE));
    }

    public QueueCriteria where(Predicate predicate) {
        return new QueueCriteria(queues, predicate);
    }

    /** Whether a task with these facts would be claimed. */
    public boolean matches(TaskFacts task) {
        return queues.contains(task.queue()) && (where == null || Criteria.matches(where, task));
    }
}
