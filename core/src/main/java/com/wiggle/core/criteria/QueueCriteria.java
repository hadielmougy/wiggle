package com.wiggle.core.criteria;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * What a poll may claim from one queue: its ready tasks for which {@code where} holds (all of them
 * when {@code where} is null). A worker serving several queues holds one per queue. Immutable, with
 * value equality; validated on creation against the limits in {@link Criteria}.
 *
 * <pre>{@code
 * QueueCriteria.from("payments")
 *         .where(cb -> cb.and(
 *                 cb.equal(Task_.WORKFLOW, "orders"),
 *                 cb.lessThan(Task_.ATTEMPT, 3)));
 * }</pre>
 */
public record QueueCriteria(String queue, Predicate where) {

    public QueueCriteria {
        Criteria.validate(queue, where);
    }

    public static QueueCriteria from(String queue) {
        return new QueueCriteria(queue, null);
    }

    public QueueCriteria where(Function<CriteriaBuilder, Predicate> build) {
        return where(build.apply(CriteriaBuilder.INSTANCE));
    }

    public QueueCriteria where(Predicate predicate) {
        return new QueueCriteria(queue, predicate);
    }

    /** Whether a task with these facts would be claimed. */
    public boolean matches(TaskFacts task) {
        return queue.equals(task.queue()) && (where == null || Criteria.matches(where, task));
    }

    /** {@code all} as one poll's criteria. @throws IllegalArgumentException when empty or a queue repeats */
    public static List<QueueCriteria> forPoll(Collection<QueueCriteria> all) {
        if (all == null || all.isEmpty()) throw new IllegalArgumentException("criteria need at least one queue");
        Set<String> queues = new HashSet<>();
        for (QueueCriteria c : all) {
            if (!queues.add(c.queue())) {
                throw new IllegalArgumentException("queue '" + c.queue() + "' has more than one criteria");
            }
        }
        return List.copyOf(all);
    }

    /** The criteria for {@code queue} among {@code all}, or null when none names it. */
    public static QueueCriteria forQueue(Collection<QueueCriteria> all, String queue) {
        for (QueueCriteria c : all) if (c.queue().equals(queue)) return c;
        return null;
    }
}
