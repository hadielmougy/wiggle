package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.core.WorkflowVersion;
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

    /**
     * Whether a throw rolls this transaction's writes back. The in-memory store answers false:
     * it applies writes directly and cannot undo them. Write-buffering keys off this -- deferring
     * writes that a rollback cannot reclaim would let a mid-transaction throw discard the buffer
     * while already-issued writes stand, leaving a state no execution could have produced.
     */
    default boolean transactional() { return true; }

    void insertInstance(Instance instance);
    /** Acquires the instance write-lock for the remainder of this transaction. */
    Optional<Instance> lockInstance(String id);

    /**
     * {@code lockInstance} for a set of rows, {@code ids} already sorted ascending. The default
     * loops -- exactly today's one-lock-per-statement, in the caller's order. A JDBC backend
     * overrides it with one {@code WHERE id IN (...) ORDER BY id FOR UPDATE}: on a btree primary
     * key the scan yields ascending ids with no separate sort node, so rows lock in id order, and
     * two overlapping statements of this same shape acquire in the same global order -- no cycle.
     * Were a planner ever to reorder, the deadlock is detected by the database and surfaces as
     * the throw the batch's replay path already handles. Missing ids are simply absent.
     */
    default List<Instance> lockInstances(List<String> ids) {
        List<Instance> out = new java.util.ArrayList<>(ids.size());
        for (String id : ids) lockInstance(id).ifPresent(out::add);
        return out;
    }
    Optional<Instance> findInstance(String id);
    void updateInstance(Instance instance);

    /** {@code updateInstance} for a set of rows; same contract as {@link #insertTokens}. */
    default void updateInstances(List<Instance> instances) {
        for (Instance i : instances) updateInstance(i);
    }
    List<Instance> listInstances(String workflow, InstanceStatus status, int limit);
    /** Instances started with {@code correlationId} (a business key), newest first. */
    List<Instance> findByCorrelation(String correlationId, int limit);
    int countInstances(InstanceStatus status);

    void insertToken(Token token);

    /** {@code insertToken} for a set of rows. The default loops; a JDBC backend overrides it with
     *  one {@code executeBatch}, which is where a cross-instance batch actually saves round-trips. */
    default void insertTokens(List<Token> tokens) {
        for (Token t : tokens) insertToken(t);
    }
    Optional<Token> findToken(String id);

    /** {@code findToken} for a set of ids: any order, missing ids absent. The default loops; a
     *  JDBC backend overrides it with one {@code WHERE id IN} read. */
    default List<Token> findTokens(List<String> ids) {
        List<Token> out = new java.util.ArrayList<>(ids.size());
        for (String id : ids) findToken(id).ifPresent(out::add);
        return out;
    }
    List<Token> tokensOf(String instanceId);
    void updateToken(Token token);

    /** {@code updateToken} for a set of rows; same contract as {@link #insertTokens}. */
    default void updateTokens(List<Token> tokens) {
        for (Token t : tokens) updateToken(t);
    }

    List<String> joinStacksAt(String instanceId, String nodeId);

    boolean hasActiveTokens(String instanceId);

    List<Token> claimTasks(String workerId, Set<String> queues, Set<WorkflowVersion> versions,
                           int max, long now, long leaseUntil);

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
     * The dispatchable backlog split by (workflow, version, queue) -- everything that decides which
     * workers may claim a token. Read-only and console-facing, so it is not on the hot path.
     */
    List<Rows.BacklogSlice> backlogByVersion(long now, int max);

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

    void appendCompensation(Rows.CompLog entry);
    /** The instance's compensation log, ordered by seq ascending. */
    java.util.List<Rows.CompLog> compensationLog(String instanceId);
    void markCompensated(String instanceId, long seq);

    /** Cancels every active token of an instance, stamping {@code now} as their update time. */
    void cancelActiveTokens(String instanceId, long now);

    void insertAnomaly(Rows.Anomaly anomaly);

    /** Anomalies newest first, narrowed by workflow and/or instance when either is non-null. */
    List<Rows.Anomaly> anomalies(String workflow, String instanceId, int limit);

    /**
     * The durations of the newest {@code max} settled, timed steps of one workflow version that
     * finished after {@code since}. Bounded so the percentiles are computed over a sample the
     * server can hold, not a table scan the console waits on.
     */
    List<Rows.StepDuration> stepDurations(String workflow, int version, long since, int max);
}
