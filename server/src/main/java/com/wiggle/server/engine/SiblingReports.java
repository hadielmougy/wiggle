package com.wiggle.server.engine;

import com.wiggle.server.engine.WorkflowEngine.Run;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reports for one instance that arrive together -- the arms of a fork finishing at once -- applied
 * in one transaction under one instance lock instead of each waiting for the other's commit.
 *
 * <p>The first report of an instance to arrive leads a group; a report of the same instance that
 * arrives while the leader is still taking its lock joins the group and is applied by the leader
 * after its own run. Once the leader holds the lock the group is sealed, and anything later reports
 * on its own. A run is only grouped when this node leased its task, which is what tells it the
 * task's instance without a read.
 */
final class SiblingReports {

    /** Upper bound on remembered leases; past it the map is cleared and grouping restarts empty. */
    private static final int MAX_LEASES = 200_000;

    private final ConcurrentHashMap<String, String> instanceOfTask = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Group> open = new ConcurrentHashMap<>();

    /** Records that {@code taskId}, of {@code instanceId}, is leased to a worker of this node. */
    void leased(String taskId, String instanceId) {
        if (instanceOfTask.size() >= MAX_LEASES) instanceOfTask.clear();
        instanceOfTask.put(taskId, instanceId);
    }

    /** The instance of a task this node leased, forgetting it; null when it was not leased here. */
    String reported(String taskId) {
        return instanceOfTask.remove(taskId);
    }

    /** Opens a group for {@code instanceId} and leads it, or joins the one already open. */
    Joined join(String instanceId, Run run) {
        Group fresh = new Group(instanceId);
        Group group = open.putIfAbsent(instanceId, fresh);
        if (group == null) return new Joined(fresh, null);
        Follower follower = new Follower(run, new CompletableFuture<>());
        return new Joined(null, group.offer(follower) ? follower : Follower.alone(run));
    }

    /** Closes the leader's group; any follower it never took reports alone. */
    void close(Group group) {
        open.remove(group.instanceId, group);
        for (Follower f : group.seal()) f.result.complete(Outcome.ALONE);
    }

    /** Exactly one of: the group this report leads, or its place as a follower. */
    record Joined(Group led, Follower follower) {}

    /** What a follower's run came to: applied with the leader, or left to report on its own. */
    record Outcome(ReportOutcome applied, RuntimeException failure) {
        static final Outcome ALONE = new Outcome(null, null);

        boolean alone() {
            return applied == null && failure == null;
        }
    }

    record Follower(Run run, CompletableFuture<Outcome> result) {
        static Follower alone(Run run) {
            return new Follower(run, CompletableFuture.completedFuture(Outcome.ALONE));
        }
    }

    static final class Group {
        private final String instanceId;
        private final List<Follower> waiting = new ArrayList<>();
        private boolean sealed;

        private Group(String instanceId) {
            this.instanceId = instanceId;
        }

        String instanceId() {
            return instanceId;
        }

        synchronized boolean offer(Follower f) {
            if (sealed) return false;
            waiting.add(f);
            return true;
        }

        /** Takes every follower that joined so far; nothing joins after. */
        synchronized List<Follower> seal() {
            sealed = true;
            List<Follower> taken = List.copyOf(waiting);
            waiting.clear();
            return taken;
        }
    }
}
