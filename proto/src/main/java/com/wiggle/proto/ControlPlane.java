package com.wiggle.proto;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The control plane's HTTP binding of the {@code WiggleControlPlane} service in {@code wiggle.proto}.
 * Every method is a unary {@code POST /v1/<Method>}; the body is the request message and the answer
 * is the response message, both as {@code application/x-protobuf} or both as {@code application/json}
 * (protobuf's canonical JSON mapping). A failed call answers a non-2xx status with a JSON
 * {@code {"code": ..., "message": ...}} body.
 */
public final class ControlPlane {
    private ControlPlane() {}

    public static final String PREFIX = "/v1/";
    public static final String PROTOBUF = "application/x-protobuf";
    public static final String JSON = "application/json";

    /** One RPC: its name, and how to parse its request and response. */
    public record Method<Q extends Message, R extends Message>(String name, Parser<Q> request, Parser<R> response) {
        public String path() { return PREFIX + name; }
    }

    public static final Method<Empty, HealthStatus> HEALTH_CHECK =
            new Method<>("HealthCheck", Empty.parser(), HealthStatus.parser());
    public static final Method<Empty, ClusterView> GET_CLUSTER =
            new Method<>("GetCluster", Empty.parser(), ClusterView.parser());
    public static final Method<Empty, WorkflowNames> LIST_WORKFLOWS =
            new Method<>("ListWorkflows", Empty.parser(), WorkflowNames.parser());
    public static final Method<BacklogCoverageRequest, BacklogCoverage> GET_BACKLOG_COVERAGE =
            new Method<>("GetBacklogCoverage", BacklogCoverageRequest.parser(), BacklogCoverage.parser());
    public static final Method<WorkflowDefinition, RegisterWorkflowResult> REGISTER_WORKFLOW =
            new Method<>("RegisterWorkflow", WorkflowDefinition.parser(), RegisterWorkflowResult.parser());
    public static final Method<GetWorkflowRequest, WorkflowDefinition> GET_WORKFLOW =
            new Method<>("GetWorkflow", GetWorkflowRequest.parser(), WorkflowDefinition.parser());
    public static final Method<StartInstanceRequest, StartInstanceResult> START_INSTANCE =
            new Method<>("StartInstance", StartInstanceRequest.parser(), StartInstanceResult.parser());
    public static final Method<ListInstancesRequest, InstanceList> LIST_INSTANCES =
            new Method<>("ListInstances", ListInstancesRequest.parser(), InstanceList.parser());
    public static final Method<InstanceIdRequest, InstanceDetail> GET_INSTANCE =
            new Method<>("GetInstance", InstanceIdRequest.parser(), InstanceDetail.parser());
    public static final Method<CancelInstanceRequest, CancelInstanceResult> CANCEL_INSTANCE =
            new Method<>("CancelInstance", CancelInstanceRequest.parser(), CancelInstanceResult.parser());
    public static final Method<SignalRequest, Ack> SIGNAL_INSTANCE =
            new Method<>("SignalInstance", SignalRequest.parser(), Ack.parser());
    public static final Method<CreateScheduleRequest, ScheduleView> CREATE_SCHEDULE =
            new Method<>("CreateSchedule", CreateScheduleRequest.parser(), ScheduleView.parser());
    public static final Method<Empty, ScheduleList> LIST_SCHEDULES =
            new Method<>("ListSchedules", Empty.parser(), ScheduleList.parser());
    public static final Method<ScheduleIdRequest, Ack> DELETE_SCHEDULE =
            new Method<>("DeleteSchedule", ScheduleIdRequest.parser(), Ack.parser());
    public static final Method<PollRequest, TaskList> POLL_TASKS =
            new Method<>("PollTasks", PollRequest.parser(), TaskList.parser());
    public static final Method<ReportStepsRequest, ReportStepsResult> REPORT_STEPS =
            new Method<>("ReportSteps", ReportStepsRequest.parser(), ReportStepsResult.parser());
    public static final Method<TaskFailureRequest, Ack> FAIL_TASK =
            new Method<>("FailTask", TaskFailureRequest.parser(), Ack.parser());
    public static final Method<HeartbeatRequest, HeartbeatResult> HEARTBEAT_TASK =
            new Method<>("HeartbeatTask", HeartbeatRequest.parser(), HeartbeatResult.parser());
    public static final Method<PollEventsRequest, EventList> POLL_EVENTS =
            new Method<>("PollEvents", PollEventsRequest.parser(), EventList.parser());
    public static final Method<AckEventsRequest, Ack> ACK_EVENTS =
            new Method<>("AckEvents", AckEventsRequest.parser(), Ack.parser());
    public static final Method<StepStatsRequest, StepStats> GET_STEP_STATS =
            new Method<>("GetStepStats", StepStatsRequest.parser(), StepStats.parser());
    public static final Method<SearchRequest, SearchResult> SEARCH_INSTANCES =
            new Method<>("SearchInstances", SearchRequest.parser(), SearchResult.parser());

    /** Every method of the service, in the order {@code wiggle.proto} declares them. */
    public static final List<Method<?, ?>> METHODS = List.of(
            HEALTH_CHECK, GET_CLUSTER, LIST_WORKFLOWS, GET_BACKLOG_COVERAGE, REGISTER_WORKFLOW, GET_WORKFLOW,
            START_INSTANCE, LIST_INSTANCES, GET_INSTANCE, CANCEL_INSTANCE, SIGNAL_INSTANCE,
            CREATE_SCHEDULE, LIST_SCHEDULES, DELETE_SCHEDULE,
            POLL_TASKS, REPORT_STEPS, FAIL_TASK, HEARTBEAT_TASK,
            POLL_EVENTS, ACK_EVENTS, GET_STEP_STATS, SEARCH_INSTANCES);

    public static final Map<String, Method<?, ?>> BY_NAME =
            METHODS.stream().collect(Collectors.toUnmodifiableMap(Method::name, Function.identity()));
}
