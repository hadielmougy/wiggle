package com.wiggle.core.wire;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The control plane's methods over HTTP. Every method is a {@code POST /v1/<Method>} whose body is the
 * request {@link Msg} and whose {@code 200} answer is the response {@link Msg}, both
 * {@code application/json}. A failed call answers a non-2xx status with a
 * {@code {"code": ..., "message": ...}} body. Chapter 70 of the spec gives every message's fields.
 */
public final class ControlPlane {
    private ControlPlane() {}

    public static final String PREFIX = "/v1/";
    public static final String JSON = "application/json";

    /** One method, by its name. */
    public record Method(String name) {
        public String path() { return PREFIX + name; }
    }

    public static final Method HEALTH_CHECK = new Method("HealthCheck");
    public static final Method GET_CLUSTER = new Method("GetCluster");
    public static final Method LIST_WORKFLOWS = new Method("ListWorkflows");
    public static final Method GET_BACKLOG_COVERAGE = new Method("GetBacklogCoverage");
    public static final Method REGISTER_WORKFLOW = new Method("RegisterWorkflow");
    public static final Method GET_WORKFLOW = new Method("GetWorkflow");
    public static final Method START_INSTANCE = new Method("StartInstance");
    public static final Method LIST_INSTANCES = new Method("ListInstances");
    public static final Method GET_INSTANCE = new Method("GetInstance");
    public static final Method CANCEL_INSTANCE = new Method("CancelInstance");
    public static final Method SIGNAL_INSTANCE = new Method("SignalInstance");
    public static final Method CREATE_SCHEDULE = new Method("CreateSchedule");
    public static final Method LIST_SCHEDULES = new Method("ListSchedules");
    public static final Method DELETE_SCHEDULE = new Method("DeleteSchedule");
    public static final Method POLL_TASKS = new Method("PollTasks");
    public static final Method REPORT_STEPS = new Method("ReportSteps");
    public static final Method FAIL_TASK = new Method("FailTask");
    public static final Method HEARTBEAT_TASK = new Method("HeartbeatTask");
    public static final Method STOP_POLLING = new Method("StopPolling");
    public static final Method POLL_EVENTS = new Method("PollEvents");
    public static final Method ACK_EVENTS = new Method("AckEvents");
    public static final Method GET_STEP_STATS = new Method("GetStepStats");
    public static final Method SEARCH_INSTANCES = new Method("SearchInstances");

    /** Every method, in the order the spec lists them. */
    public static final List<Method> METHODS = List.of(
            HEALTH_CHECK, GET_CLUSTER, LIST_WORKFLOWS, GET_BACKLOG_COVERAGE, REGISTER_WORKFLOW, GET_WORKFLOW,
            START_INSTANCE, LIST_INSTANCES, GET_INSTANCE, CANCEL_INSTANCE, SIGNAL_INSTANCE,
            CREATE_SCHEDULE, LIST_SCHEDULES, DELETE_SCHEDULE,
            POLL_TASKS, REPORT_STEPS, FAIL_TASK, HEARTBEAT_TASK, STOP_POLLING,
            POLL_EVENTS, ACK_EVENTS, GET_STEP_STATS, SEARCH_INSTANCES);

    public static final Map<String, Method> BY_NAME =
            METHODS.stream().collect(Collectors.toUnmodifiableMap(Method::name, Function.identity()));
}
