package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Token;

import java.util.List;
import java.util.Optional;

/**
 * Tokens -- the units of execution -- with their leases and the work queue: the reads, which a
 * read-only connection such as a read replica can serve. Held on an instance shard.
 */
public interface TokenReads {

    Optional<Token> findToken(String id);

    /** {@code findToken} for a set of ids: any order, missing ids absent. The default loops; a
     *  JDBC backend overrides it with one {@code WHERE id IN} read. */
    default List<Token> findTokens(List<String> ids) {
        List<Token> out = new java.util.ArrayList<>(ids.size());
        for (String id : ids) findToken(id).ifPresent(out::add);
        return out;
    }

    List<Token> tokensOf(String instanceId);

    /** AWAITING signal tokens, oldest first -- what the pending-signals list shows. */
    List<Token> pendingSignals(int max);

    /** Snapshot of the dispatchable backlog, for lag monitoring. */
    Rows.QueueDepth queueDepth(long now);

    /**
     * The dispatchable backlog split by (workflow, version, queue) -- everything that decides which
     * workers may claim a token. Read-only and console-facing, so it is not on the hot path.
     */
    List<Rows.BacklogSlice> backlogByVersion(long now, int max);

    /**
     * How many due READY tasks of one backlog slice {@code where} admits, counting at most {@code cap}.
     * Console-facing, like {@link #backlogByVersion}.
     */
    int countReady(String workflow, int version, String queue, com.wiggle.core.criteria.Predicate where,
                   long now, int cap);

    /**
     * The durations of the newest {@code max} settled, timed steps of one workflow version that
     * finished after {@code since}. Bounded so the percentiles are computed over a sample the
     * server can hold, not a table scan the console waits on.
     */
    List<Rows.StepDuration> stepDurations(String workflow, int version, long since, int max);
}
