package com.wiggle.server.store;

import com.wiggle.core.InstanceStatus;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.ServerNode;
import com.wiggle.server.store.Rows.Token;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The reads of one transaction: everything a caller may do on a read-only connection, such as a
 * read replica. {@link Tx} extends it with the writes and the locking reads, so a body typed as
 * {@code ReadTx} cannot write.
 */
public interface ReadTx extends GraphReads {

    /** The shard this database was claimed for, or empty when no shard has claimed it. */
    OptionalInt shardIdentity();

    /** Every shard the cluster has used, by id. Held on the home shard. */
    List<Rows.ShardRecord> shardRegistry();

    Optional<Instance> findInstance(String id);

    List<Instance> listInstances(String workflow, InstanceStatus status, int limit);

    /** Instances started with {@code correlationId} (a business key), newest first. */
    List<Instance> findByCorrelation(String correlationId, int limit);

    int countInstances(InstanceStatus status);

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

    /** Instances whose parent token belongs to {@code parentInstanceId} -- its sub-workflows. */
    List<String> childInstanceIds(String parentInstanceId);

    List<Rows.Schedule> schedules();

    /** The schedule for a workflow, if one exists -- workflow is a unique key for schedules. */
    java.util.Optional<Rows.Schedule> scheduleByWorkflow(String workflow);

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

    List<ServerNode> nodes();

    /** The instance's compensation log, ordered by seq ascending. */
    java.util.List<Rows.CompLog> compensationLog(String instanceId);

    /** Anomalies newest first, narrowed by workflow and/or instance when either is non-null. */
    List<Rows.Anomaly> anomalies(String workflow, String instanceId, int limit);

    /** Up to {@code max} events with seq greater than {@code afterSeq}, ascending. */
    default List<Rows.Event> eventsAfter(long afterSeq, int max) {
        return eventsAfter(afterSeq, Long.MAX_VALUE, max);
    }

    /**
     * {@link #eventsAfter(long, int)} restricted to events appended before {@code createdBefore}.
     * The feed holds this line back from now: seq is store-assigned, so an uncommitted append may
     * still hold a seq below one already visible, and a consumer that read past it would never be
     * offered it. Waiting out the window costs latency, not correctness.
     */
    List<Rows.Event> eventsAfter(long afterSeq, long createdBefore, int max);

    /** The highest seq the log has assigned, or 0 when it is empty. */
    long latestEventSeq();

    /** One consumer's cursor, or null when it has never polled. */
    Rows.EventCursor eventCursor(String consumer);

    /** The lowest seq any consumer cursor has acknowledged, or null when no cursor exists. */
    Long oldestAckedSeq();

    /**
     * The durations of the newest {@code max} settled, timed steps of one workflow version that
     * finished after {@code since}. Bounded so the percentiles are computed over a sample the
     * server can hold, not a table scan the console waits on.
     */
    List<Rows.StepDuration> stepDurations(String workflow, int version, long since, int max);
}
