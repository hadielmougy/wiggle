package com.wiggle.server.http;

import com.wiggle.core.InstanceView;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * The read/ops surface the {@link HttpDashboard} needs, decoupled from any particular source. The
 * embedded dashboard is backed by the in-process engine ({@code EngineDashboardData}); the standalone
 * ops console backs the same dashboard with a gRPC client (single cell, or fanned out across a
 * namespace's cells under a coordinator). Everything here is neutral (no engine or storage types), so
 * both backends produce identical JSON to the SPA.
 */
public interface DashboardData {

    List<String> workflowNames();

    /** The latest compiled graph for {@code name} as parsed JSON, or empty if unknown. */
    Optional<Object> workflowGraph(String name);

    List<InstanceView> listInstances(String workflow, String status, int limit);

    Optional<InstanceDetail> instance(String id);

    void cancel(String id, String reason);

    void signal(String id, String name, Object payload);

    /** Signal waits pending external delivery; may be empty where the backend can't enumerate them. */
    List<SignalView> pendingSignals(int limit);

    List<ScheduleView> schedules();

    String createSchedule(String workflow, Duration every, Object context);

    String createCronSchedule(String workflow, String cron, Object context);

    void deleteSchedule(String id);

    ClusterView cluster();

    record InstanceDetail(InstanceView instance, List<TokenView> tokens) {}

    record TokenView(String id, String nodeId, String kind, String status, String activity,
                     String queue, int attempt, long availableAt, String leaseOwner,
                     long leaseExpiresAt, String lastError, long updatedAt) {}

    record SignalView(String instanceId, String workflow, String signal, long deadline, long createdAt) {}

    record ScheduleView(String id, String workflow, long everyMillis, String cron,
                        long nextFireAt, long createdAt) {}

    record ClusterView(String nodeId, boolean leader, List<MemberView> members) {}

    record MemberView(String id, String name, int workers, boolean leader, boolean alive, long lastHeartbeat) {}
}
