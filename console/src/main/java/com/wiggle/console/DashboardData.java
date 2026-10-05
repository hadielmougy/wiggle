package com.wiggle.console;

import com.wiggle.core.InstanceView;
import com.wiggle.core.NodeStats;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * The read/ops surface the portal serves, in neutral types so the SPA's JSON does not follow engine
 * or storage changes. {@link EngineDashboardData} backs it with the server process it runs in.
 */
public interface DashboardData {

    List<String> workflowNames();

    /** The latest compiled graph for {@code name} as parsed JSON, or empty if unknown. */
    Optional<Object> workflowGraph(String name);

    List<InstanceView> listInstances(String workflow, String status, int limit);

    /** Instances started with {@code correlationId} (a business key), newest first, capped at {@code limit}. */
    List<InstanceView> findByCorrelation(String correlationId, int limit);

    Optional<InstanceDetail> instance(String id);

    void cancel(String id, String reason);

    void signal(String id, String name, Object payload);

    /** Signal waits pending external delivery, oldest first. */
    List<SignalView> pendingSignals(int limit);

    /**
     * Dispatchable work split by (workflow, version, queue), each flagged with whether any worker
     * polling the server would claim it. An uncovered slice is work nothing can pick up -- a queue
     * nobody polls, or a version every worker has scoped itself out of. Neither shows up anywhere
     * else: the instance reads RUNNING and the token reads READY, which is what healthy looks like.
     */
    List<BacklogView> backlogCoverage(int limit);

    /**
     * Per-node duration statistics for a workflow (null or zero version = latest) over its newest
     * {@code sample} timed steps finished after {@code since}; slowest p95 first. Steps are timed
     * where they ran, so this covers worker-dispatched and locally-chained runs alike.
     */
    List<NodeStats> stepStats(String workflow, Integer version, long since, int sample);

    List<ScheduleView> schedules();

    String createSchedule(String workflow, Duration every, Object context);

    String createCronSchedule(String workflow, String cron, Object context);

    void deleteSchedule(String id);

    List<TriggerView> triggers();

    /** Upserts on (workflow, source); returns the trigger id. */
    String createTrigger(String workflow, String source, List<String> eventTypes, boolean includeContext);

    void deleteTrigger(String id);

    ClusterView cluster();

    /**
     * Full-text search over instances, or empty when search is not enabled. {@code readable} is the
     * set of workflows the caller may read, null for every one.
     */
    /** Whether {@link #search} answers. */
    boolean searchEnabled();

    /** Whether {@link #search} answers a semantic search. */
    boolean semanticEnabled();

    /** {@code semantic} ranks by closeness in meaning instead of by the words. */
    Optional<SearchView> search(String text, String workflow, String status, int limit, boolean partialOk,
                                boolean semantic, java.util.Set<String> readable);

    record SearchView(List<SearchHitView> hits, boolean partial) {}

    record SearchHitView(String instanceId, String workflow, int version, String status, String correlationId,
                         long updatedAt, double score, boolean purged) {}

    record InstanceDetail(InstanceView instance, List<TokenView> tokens) {}

    record TokenView(String id, String nodeId, String kind, String status, String activity,
                     String queue, int attempt, long availableAt, String leaseOwner,
                     long leaseExpiresAt, String lastError, long updatedAt,
                     Long startedAt, Long finishedAt, long createdAt, Object input, Object output) {}

    record SignalView(String instanceId, String workflow, String signal, long deadline, long createdAt) {}

    record BacklogView(String workflow, int version, String queue, int readyCount,
                       long oldestAvailableAt, boolean covered, int livePollers) {}

    record ScheduleView(String id, String workflow, long everyMillis, String cron,
                        long nextFireAt, long createdAt) {}

    record TriggerView(String id, String workflow, String source, List<String> eventTypes,
                       boolean includeContext, long createdAt) {}

    record ClusterView(String nodeId, boolean leader, List<MemberView> members) {}

    record MemberView(String id, String name, int workers, boolean leader, boolean alive, long lastHeartbeat) {}
}
