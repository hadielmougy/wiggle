package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows.Token;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The mutable runtime state of one transaction: instances, tokens, leases, schedules, and cluster
 * nodes. It extends {@link ReadTx} with the writes and the locking reads, and {@link GraphStore} -- the
 * immutable definition/graph reference data -- so a single transaction can read the graph and mutate
 * runtime state atomically. Every engine mutation runs inside a {@link Storage} transaction.
 */
public interface Tx extends ReadTx, GraphStore {

    /**
     * Whether a throw rolls this transaction's writes back. The in-memory store answers false:
     * it applies writes directly and cannot undo them. Write-buffering keys off this -- deferring
     * writes that a rollback cannot reclaim would let a mid-transaction throw discard the buffer
     * while already-issued writes stand, leaving a state no execution could have produced.
     */
    default boolean transactional() { return true; }

    /** Claims this database for {@code shardId} when nothing has claimed it; leaves an existing claim
     *  alone, so the caller compares {@link #shardIdentity} afterwards. */
    void claimShardIdentity(int shardId);

    /** Stamps the replica-lag heartbeat with {@code now}. Needs a claimed shard identity. */
    void writeShardBeat(long now);

    /** Moves a consumer's event position on {@code shard}, a shard other than home, to
     *  {@code ackedSeq}, never backwards. Held on the home shard. */
    void advanceEventPosition(String consumer, int shard, long ackedSeq);

    /** Writes a shard's registry row, replacing any it had. */
    void putShardRecord(Rows.ShardRecord record);

    /** Writes a portal account, replacing any of that name. Held on the auth shard. */
    void putAuthUser(Rows.AuthUser user);

    /** Deletes a portal account with its role grants and sessions; false when there was none. */
    boolean deleteAuthUser(String name);

    /** Replaces the roles {@code user} holds. */
    void setAuthRolesOf(String user, List<String> roles);

    /** Writes a role, replacing any of that name. Held on the auth shard. */
    void putAuthRole(Rows.AuthRole role);

    /** Deletes a role and every grant of it; false when there was none. */
    boolean deleteAuthRole(String name);

    /** Adds a machine credential; its id, key hash and subject are each unique. */
    void insertAuthCredential(Rows.AuthCredential credential);

    /** Deletes a machine credential; false when there was none. */
    boolean deleteAuthCredential(String id);

    void insertAuthSession(Rows.AuthSession session);

    void deleteAuthSession(String idHash);

    /** Deletes every session of {@code user} except {@code keepIdHash} (null keeps none); returns how many. */
    int deleteAuthSessionsOf(String user, String keepIdHash);

    /** Deletes up to {@code max} sessions that expired before {@code now}; returns how many. */
    int deleteExpiredAuthSessions(long now, int max);

    /** Appends to the auth audit and returns the seq the store assigned. */
    long appendAuthAudit(Rows.AuthAudit entry);

    /**
     * Writes a search document unless one for the same instance is newer; returns whether it wrote.
     * Held on a search shard.
     */
    boolean upsertSearchDoc(Rows.SearchDoc doc);

    /** Deletes the document and its vectors. */
    void deleteSearchDoc(String instanceId);

    /** Writes each vector unless the one held for that instance and model is newer. */
    void upsertSearchVectors(List<Rows.SearchVector> vectors);

    /** Deletes up to {@code max} vectors of {@code model}; returns how many. */
    int deleteSearchVectors(String model, int max);

    /** Prepares this database to search {@code model}'s vectors fast, where it can; idempotent. */
    default void ensureVectorIndex(String model, int dimension) { }

    /** Writes a model's registry row, replacing any it had. Held on the home shard. */
    void putSearchModel(Rows.SearchModel model);

    /** Deletes up to {@code max} documents, with their vectors, whose instance last changed before {@code updatedBefore}. */
    int deleteSearchDocsBefore(long updatedBefore, int max);

    void insertInstance(Instance instance);

    /** Acquires the instance write-lock for the remainder of this transaction. */
    Optional<Instance> lockInstance(String id);

    /** {@code lockInstance} on the instance that owns token {@code tokenId}; empty when either the
     *  token or its instance is missing. */
    default Optional<Instance> lockInstanceOf(String tokenId) {
        return findToken(tokenId).flatMap(t -> lockInstance(t.instanceId));
    }

    /** {@code lockInstanceOf}, and token {@code tokenId} read under that lock; empty when either the
     *  token or its instance is missing. */
    default Optional<Rows.LockedTask> lockTask(String tokenId) {
        return lockInstanceOf(tokenId).flatMap(i -> findToken(tokenId).map(t -> new Rows.LockedTask(i, t)));
    }

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

    /**
     * Writes back the fields of an instance that change as it runs: status, termination reason,
     * error and context, stamped with {@code updatedAt}.
     *
     * <p>It writes nothing else. An instance's identity and provenance -- {@code workflow},
     * {@code version}, {@code correlationId}, {@code parentTokenId}, {@code createdAt} -- are
     * settled when the row is inserted and are not reachable from here, so a body that hands back
     * a row carrying a changed one cannot rewrite history with it. Nothing in the engine assigns
     * them after birth; this is what keeps that true of the store as well.
     *
     * <p>The revision is advanced by the store, from the value the row holds rather than from the
     * caller's: it counts the writes the row has taken. A caller working from a stale copy is
     * therefore not able to walk the revision backwards, and its own copy is advanced from where
     * it was so the engine can write the row on again.
     */
    void updateInstance(Instance instance);

    /** {@code updateInstance} for a set of rows; same contract as {@link #insertTokens}. */
    default void updateInstances(List<Instance> instances) {
        for (Instance i : instances) updateInstance(i);
    }

    void insertToken(Token token);

    /** {@code insertToken} for a set of rows. The default loops; a JDBC backend overrides it with
     *  one {@code executeBatch}, which is where a cross-instance batch actually saves round-trips. */
    default void insertTokens(List<Token> tokens) {
        for (Token t : tokens) insertToken(t);
    }

    void updateToken(Token token);

    /** {@code updateToken} for a set of rows; same contract as {@link #insertTokens}. */
    default void updateTokens(List<Token> tokens) {
        for (Token t : tokens) updateToken(t);
    }

    /**
     * Moves the lease expiry of a RUNNING token to {@code until}, touching nothing else of the
     * row. A null {@code leaseOwner} matches any holder. Returns false, having written nothing,
     * when the token is missing, not RUNNING, or leased by someone else.
     */
    default boolean renewLease(String taskId, String leaseOwner, long until, long now) {
        Token t = findToken(taskId).orElse(null);
        if (t == null || t.status != TokenStatus.RUNNING) return false;
        if (leaseOwner != null && !leaseOwner.equals(t.leaseOwner)) return false;
        t.leaseExpiresAt = until;
        t.updatedAt = now;
        updateToken(t);
        return true;
    }

    List<String> joinStacksAt(String instanceId, String nodeId);

    /** The instance's JOINED tokens parked at {@code nodeId}, by id. */
    default List<Token> joinedAt(String instanceId, String nodeId) {
        return tokensOf(instanceId).stream()
                .filter(t -> t.status == TokenStatus.JOINED && nodeId.equals(t.nodeId))
                .toList();
    }

    /** The instance's token AWAITING signal {@code name}, the lowest id if several. */
    default Optional<Token> awaitingSignal(String instanceId, String name) {
        return tokensOf(instanceId).stream()
                .filter(t -> t.status == TokenStatus.AWAITING && t.kind == NodeKind.SIGNAL)
                .filter(t -> name.equals(t.activity))
                .findFirst();
    }

    boolean hasActiveTokens(String instanceId);

    List<Token> claimTasks(String workerId, Set<String> queues, Set<WorkflowVersion> versions,
                           int max, long now, long leaseUntil);

    /** WAITING timer tokens whose fire time has passed. */
    List<Token> dueTimers(long now, int max);

    /** WAITING task and predicate tokens -- retries parked for their backoff -- whose backoff has run
     *  out, earliest first. */
    List<Token> dueRetries(long now, int max);

    /** AWAITING signal tokens with a deadline (availableAt > 0) that has passed. */
    List<Token> dueSignals(long now, int max);

    void putSchedule(Rows.Schedule schedule);

    void deleteSchedule(String id);

    /** Schedules whose fire time has passed. */
    List<Rows.Schedule> dueSchedules(long now, int max);

    /**
     * Advances a schedule's fire time iff it still reads {@code expectedFireAt} -- the
     * compare-and-set that keeps overlapping leaders from double-firing.
     */
    boolean claimSchedule(String id, long expectedFireAt, long nextFireAt);

    /** RUNNING tokens whose lease has expired (worker died or partitioned away). */
    List<Token> expiredLeases(long now, int max);

    void upsertNode(ServerNode node);

    void deleteNodesOlderThan(long lastHeartbeatBefore);

    void setLeader(String nodeId, boolean leader);

    int deleteTerminalInstancesBefore(long updatedBefore, int limit);

    void appendCompensation(Rows.CompLog entry);

    void markCompensated(String instanceId, long seq);

    /** Cancels every active token of an instance, stamping {@code now} as their update time. */
    void cancelActiveTokens(String instanceId, long now);

    /** Appends to the event log and returns the seq the store assigned; visible with the transaction. */
    long appendEvent(Rows.Event event);

    /** Registers {@code cursor} if that consumer has none; leaves an existing one untouched. */
    void createEventCursorIfAbsent(Rows.EventCursor cursor);

    /** Moves the consumer's cursor to {@code ackedSeq}, never backwards, and stamps {@code now}. */
    void advanceEventCursor(String consumer, long ackedSeq, long now);

    /**
     * Deletes up to {@code max} of the oldest events created before {@code createdBefore}; when
     * {@code upToSeq} is non-null, only events with seq at or below it, so an unacknowledged
     * event outlives the age cap for as long as a consumer is still on its way to it.
     */
    int deleteEvents(long createdBefore, Long upToSeq, int max);
}
