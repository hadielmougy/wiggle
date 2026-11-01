package com.wiggle.server.store;

import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.Rows.Token;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Tokens -- the units of execution -- with their leases and the work queue: the writes, and the
 * reads that must see the primary. Held on an instance shard.
 */
public interface TokenRepository extends TokenReads {

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

    /** Leases up to {@code max} due ready tasks that {@code filter} admits, oldest instance first. */
    List<Token> claimTasks(String workerId, ClaimFilter filter, int max, long now, long leaseUntil);

    default List<Token> claimTasks(String workerId, Set<String> queues, Set<WorkflowVersion> versions,
                                   int max, long now, long leaseUntil) {
        return claimTasks(workerId, ClaimFilter.byName(queues, versions), max, now, leaseUntil);
    }

    /** WAITING timer tokens whose fire time has passed. */
    List<Token> dueTimers(long now, int max);

    /** WAITING task and predicate tokens -- retries parked for their backoff -- whose backoff has run
     *  out, earliest first. */
    List<Token> dueRetries(long now, int max);

    /** AWAITING signal tokens with a deadline (availableAt > 0) that has passed. */
    List<Token> dueSignals(long now, int max);

    /** RUNNING tokens whose lease has expired (worker died or partitioned away). */
    List<Token> expiredLeases(long now, int max);

    /** Cancels every active token of an instance, stamping {@code now} as their update time. */
    void cancelActiveTokens(String instanceId, long now);
}
