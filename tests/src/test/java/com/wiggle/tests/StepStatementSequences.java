package com.wiggle.tests;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.core.Ids;
import com.wiggle.core.TaskActivation;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.jdbc.StatementLog;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.wiggle.jdbc.StatementLog.COMMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The exact statements, in order, that starting an instance, claiming its tasks and reporting its
 * steps issue to the database, as {@link StatementLog} summarises them. A change to what a step
 * costs the database fails here and updates the expected sequence in the same change. The
 * dialects differ only in how a task is locked and how tasks are claimed: {@link #lockTask} and
 * {@link #claim}.
 */
public abstract class StepStatementSequences {

    private static final String WORKER = "seq-w1";

    /** A fresh, migrated store of the dialect under test. */
    protected abstract JdbcStorage storage();

    /** The statements that lock a reported task with its instance. */
    protected abstract List<String> lockTask();

    /** The statements that hand {@code tasks} ready tasks to a worker, before reading their instances. */
    protected abstract List<String> claim(int tasks);

    private JdbcStorage storage;
    private final StatementLog log = new StatementLog();
    private JdbcStorage.Observation observation;
    private WorkflowEngine registering;
    private WorkflowEngine other;

    @BeforeEach void open() {
        storage = storage();
        registering = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
        other = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
        observation = storage.observe(log);
    }

    @AfterEach void close() {
        observation.close();
        storage.close();
    }

    /** Three steps in a line; a worker binds them by name. */
    interface ThreeSteps {
        Map<String, Object> a(Map<String, Object> ctx);
        Map<String, Object> b(Map<String, Object> ctx);
        Map<String, Object> c(Map<String, Object> ctx);
    }

    /** Two arms of two steps each, combined, then a last step: the shape of the order flow. */
    interface ForkSteps {
        Map<String, Object> a1(Map<String, Object> ctx);
        Map<String, Object> a2(Map<String, Object> ctx);
        Map<String, Object> b1(Map<String, Object> ctx);
        Map<String, Object> b2(Map<String, Object> ctx);
        Map<String, Object> merge(Map<String, Object> a, Map<String, Object> b);
        Map<String, Object> last(Map<String, Object> ctx);
    }

    private FlowSpec linear() {
        FlowSpec flow = FlowSpec.define("seq-" + Ids.next("wf"), 1, Map.class, ThreeSteps.class,
                (f, s) -> f.thenApply(s::a).thenApply(s::b).thenApply(s::c));
        registering.register(flow.definition());
        log.take();
        return flow;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private FlowSpec forked() {
        FlowSpec flow = FlowSpec.define("seq-fork-" + Ids.next("wf"), 1, Map.class, ForkSteps.class, (f, s) -> {
            var a = f.thenApply(s::a1).thenApply(s::a2);
            var b = f.thenApply(s::b1).thenApply(s::b2);
            return Wiggle.allOf(a, b).combine(s::merge).thenApply(s::last);
        });
        registering.register(flow.definition());
        log.take();
        return flow;
    }

    private static final List<String> START = List.of("INSERT wf_instance", "INSERT wf_event", "INSERT wf_token", COMMIT);
    private static final List<String> GRAPH = List.of("SELECT wf_graph_node", "SELECT wf_definition");
    private static final List<String> FINISH = List.of("SELECT wf_token", "UPDATE wf_instance", "INSERT wf_event");

    /** A report that completes its task and readies the next one. */
    private List<String> step(List<String> then) {
        return concat(lockTask(), List.of("INSERT wf_token", "UPDATE wf_token", "UPDATE wf_instance"), then, List.of(COMMIT));
    }

    @Test @DisplayName("a linear flow on the node that registered it: start, claim and report each step")
    void linearOnRegisteringNode() {
        runLinear(registering, START, List.of());
    }

    @Test @DisplayName("a linear flow on another node: the graph is read once, at its first start and first claim")
    void linearOnOtherNode() {
        runLinear(other, concat(GRAPH, START), List.of("SELECT wf_definition"));
    }

    private void runLinear(WorkflowEngine engine, List<String> start, List<String> firstClaimExtra) {
        FlowSpec flow = linear();
        engine.start(flow.name(), flow.version(), Map.of(), null);
        assertEquals(start, log.take(), "start");

        for (String name : List.of("a", "b", "c")) {
            TaskActivation task = single(engine.poll(WORKER, flow.definition().workerQueues(), 10, null));
            assertEquals(concat(claim(1), List.of("SELECT wf_instance"), name.equals("a") ? firstClaimExtra : List.of(),
                    List.of(COMMIT)), log.take(), "claim " + name);
            report(engine, task);
            assertEquals(step(name.equals("c") ? FINISH : List.of()), log.take(), "report " + name);
        }
        assertEquals(List.of(), engine.poll(WORKER, flow.definition().workerQueues(), 10, null));
        assertEquals(concat(claim(0), List.of(COMMIT)), log.take(), "an empty claim");
    }

    @Test @DisplayName("a claim of several tasks reads each task's instance once")
    void batchClaim() {
        FlowSpec flow = linear();
        for (int i = 0; i < 3; i++) registering.start(flow.name(), flow.version(), Map.of(), null);
        log.take();
        assertEquals(3, registering.poll(WORKER, flow.definition().workerQueues(), 10, null).size());
        assertEquals(concat(claim(3), List.of("SELECT wf_instance", "SELECT wf_instance", "SELECT wf_instance", COMMIT)),
                log.take());
    }

    @Test @DisplayName("each report of a fork/combine flow: the first arm to reach the join, the second that completes it")
    void forkJoin() {
        FlowSpec flow = forked();
        registering.start(flow.name(), flow.version(), Map.of(), null);
        log.take();

        Map<String, List<String>> reports = new LinkedHashMap<>();
        for (List<TaskActivation> batch = registering.poll(WORKER, flow.definition().workerQueues(), 10, null);
             !batch.isEmpty(); batch = registering.poll(WORKER, flow.definition().workerQueues(), 10, null)) {
            log.take();
            for (TaskActivation task : batch.stream().sorted(Comparator.comparing(TaskActivation::stepName)).toList()) {
                report(registering, task);
                reports.put(task.stepName(), log.take());
            }
        }

        Map<String, List<String>> expected = new LinkedHashMap<>();
        expected.put("a1", step(List.of()));
        expected.put("b1", step(List.of()));
        expected.put("a2", step(List.of("SELECT wf_token")));
        expected.put("b2", step(List.of("SELECT wf_token", "SELECT wf_token", "UPDATE wf_token",
                "SELECT wf_token", "INSERT wf_token")));
        expected.put("merge", step(List.of()));
        expected.put("last", step(FINISH));
        assertEquals(expected, reports);
    }

    private static void report(WorkflowEngine engine, TaskActivation task) {
        engine.report(new Run(task.taskId(), WORKER, List.of(new StepInput(task.nodeId(), Map.of(), null)), false));
    }

    private static TaskActivation single(List<TaskActivation> batch) {
        assertEquals(1, batch.size(), "one task ready");
        return batch.get(0);
    }

    @SafeVarargs
    protected static List<String> concat(List<String>... parts) {
        List<String> out = new ArrayList<>();
        Stream.of(parts).forEach(out::addAll);
        return out;
    }
}
