package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.server.store.Rows.Token;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The mutable runtime state of one transaction: instances, tokens, leases, schedules, and cluster
 * nodes. It also extends {@link GraphStore} -- the immutable definition/graph reference data -- so a
 * single transaction can read the graph and mutate runtime state atomically. Every engine mutation
 * runs inside {@link Storage#inTx}.
 */
public interface Tx extends GraphStore {

    void insertInstance(Instance instance);
    /** Acquires the instance write-lock for the remainder of this transaction. */
    Optional<Instance> lockInstance(String id);
    Optional<Instance> findInstance(String id);
    void updateInstance(Instance instance);
    List<Instance> listInstances(String workflow, InstanceStatus status, int limit);
    int countInstances(InstanceStatus status);

    void insertToken(Token token);
    Optional<Token> findToken(String id);
    List<Token> tokensOf(String instanceId);
    void updateToken(Token token);

    /**
     * Atomically leases up to {@code max} dispatchable tokens. Implementations must
     * guarantee a token is handed to exactly one worker.
     */
    List<Token> claimTasks(String workerId, Set<String> queues, int max, long now, long leaseUntil);

    /** WAITING timer tokens whose fire time has passed. */
    List<Token> dueTimers(long now, int max);

    /** AWAITING signal tokens, oldest first -- what the pending-signals list shows. */
    List<Token> pendingSignals(int max);

    /** AWAITING signal tokens with a deadline (availableAt > 0) that has passed. */
    List<Token> dueSignals(long now, int max);

    /** Instances whose parent token belongs to {@code parentInstanceId} -- its sub-workflows. */
    List<String> childInstanceIds(String parentInstanceId);

    void putSchedule(Rows.Schedule schedule);
    void deleteSchedule(String id);
    List<Rows.Schedule> schedules();
    /** The schedule for a workflow, if one exists -- workflow is a unique key for schedules. */
    java.util.Optional<Rows.Schedule> scheduleByWorkflow(String workflow);
    /** Schedules whose fire time has passed. */
    List<Rows.Schedule> dueSchedules(long now, int max);
    /**
     * Advances a schedule's fire time iff it still reads {@code expectedFireAt} -- the
     * compare-and-set that keeps overlapping leaders from double-firing.
     */
    boolean claimSchedule(String id, long expectedFireAt, long nextFireAt);

    /** RUNNING tokens whose lease has expired (worker died or partitioned away). */
    List<Token> expiredLeases(long now, int max);

    /** Snapshot of the dispatchable backlog, for lag monitoring. */
    Rows.QueueDepth queueDepth(long now);

    /**
     * Worker-dispatched tokens (TASK/PREDICATE) that finished (DONE) since {@code since} --
     * the throughput signal for lag monitoring. DB-driven rather than an in-process counter,
     * so it reflects consumption across every node in the cluster, not just this one.
     */
    int countProcessedSince(long since);

    void upsertNode(ServerNode node);
    List<ServerNode> nodes();
    void deleteNodesOlderThan(long lastHeartbeatBefore);
    void setLeader(String nodeId, boolean leader);

    int deleteTerminalInstancesBefore(long updatedBefore, int limit);
}
