package com.wiggle.server.engine;

import com.wiggle.tests.Reports;
import com.wiggle.core.*;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The invariant the two lifecycles exist to keep, driven rather than asserted in prose: when an
 * instance reaches a terminal state, none of its tokens is still active. A leaked READY token
 * would be dispatched forever against a dead instance; a leaked RUNNING one would hold a lease
 * nothing will ever release.
 *
 * <p>Each case walks a real workflow to one terminal state through the engine's own entry points.
 * The states are taken from {@link StateChart}, so a new terminal state that nothing here reaches
 * fails {@link #everyTerminalStateIsCovered()} rather than going quietly untested.
 *
 * <p>The last case covers the other half of the coupling: what a worker is told when the instance
 * stopped running underneath it.
 */
class TerminalInvariantTest {

    private static final String QUEUE = "q";

    /** Reached by the cases below; checked against the chart's terminal set. */
    private static final Set<String> COVERED = new java.util.LinkedHashSet<>();

    @Test @DisplayName("a workflow that runs to a successful END leaves no active token")
    void completed() {
        drive(linear(), engine -> {
            String id = engine.start("linear", 1, Map.of(), null);
            report(engine, "one");
            report(engine, "two");
            assertTerminalAndQuiet(engine, id, InstanceStatus.COMPLETED);
        });
    }

    @Test @DisplayName("a task that exhausts its retries leaves no active token")
    void failed() {
        drive(linear(), engine -> {
            String id = engine.start("linear", 1, Map.of(), null);
            TaskActivation t = claim(engine);
            engine.fail(t.taskId(), t.leaseOwner(), "boom", false);
            assertTerminalAndQuiet(engine, id, InstanceStatus.FAILED);
        });
    }

    @Test @DisplayName("an END node marked unsuccessful leaves no active token")
    void failedAtUnsuccessfulEnd() {
        drive(unsuccessfulEnd(), engine -> {
            String id = engine.start("dead-end", 1, Map.of(), null);
            report(engine, "one");
            assertTerminalAndQuiet(engine, id, InstanceStatus.FAILED);
        });
    }

    @Test @DisplayName("cancelling a running instance leaves no active token, including a leased one")
    void cancelled() {
        drive(linear(), engine -> {
            String id = engine.start("linear", 1, Map.of(), null);
            claim(engine);   // leave a token RUNNING under a lease
            engine.cancel(id, "by hand");
            assertTerminalAndQuiet(engine, id, InstanceStatus.CANCELLED);
        });
    }

    @Test @DisplayName("a fork whose branch fails leaves no sibling active")
    void cancelledSiblingsOfAFailedBranch() {
        drive(forkJoin(), engine -> {
            String id = engine.start("forked", 1, Map.of(), null);
            TaskActivation left = claim(engine);
            engine.fail(left.taskId(), left.leaseOwner(), "boom", false);
            assertTerminalAndQuiet(engine, id, InstanceStatus.FAILED);
        });
    }

    @Test @DisplayName("a saga that undoes everything leaves no active token")
    void compensated() {
        drive(saga(), engine -> {
            String id = engine.start("saga", 1, Map.of(), null);
            report(engine, "one");                       // compensable, now in the comp-log
            TaskActivation two = claim(engine);
            engine.fail(two.taskId(), two.leaseOwner(), "boom", false);
            assertEquals(InstanceStatus.COMPENSATING.name(), status(engine, id),
                    "the reverse pass should have taken the instance over");
            assertEquals(1, active(engine, id).size(), "exactly one undo task is in flight");
            TaskActivation undo = claim(engine);           // the compensator
            Reports.one(engine, undo, null);
            assertTerminalAndQuiet(engine, id, InstanceStatus.COMPENSATED);
        });
    }

    @Test @DisplayName("a compensator that exhausts its retries leaves no active token")
    void compensationFailed() {
        drive(saga(), engine -> {
            String id = engine.start("saga", 1, Map.of(), null);
            report(engine, "one");
            TaskActivation two = claim(engine);
            engine.fail(two.taskId(), two.leaseOwner(), "boom", false);
            TaskActivation undo = claim(engine);
            engine.fail(undo.taskId(), undo.leaseOwner(), "undo broke", false);
            assertTerminalAndQuiet(engine, id, InstanceStatus.COMPENSATION_FAILED);
        });
    }

    /**
     * ReportStepsResult.instance_status is not limited to the terminal four: a concurrent failure
     * can leave the instance COMPENSATING between a worker's local steps, and the worker is told
     * so. This is what {@code proto/src/main/proto/wiggle.proto} documents on that field.
     */
    @Test @DisplayName("a report answers COMPENSATING when the saga took over mid-run")
    void reportAnswersANonTerminalNonRunningState() {
        drive(sagaFork(), engine -> {
            engine.start("saga-fork", 1, Map.of(), null);
            report(engine, "one");                       // compensable, now in the comp-log
            TaskActivation left = claim(engine);           // leased, still in flight
            TaskActivation right = claim(engine);
            engine.fail(right.taskId(), right.leaseOwner(), "boom", false);

            ReportOutcome out = engine.report(new WorkflowEngine.Run(left.taskId(), left.leaseOwner(),
                    List.of(new WorkflowEngine.StepInput("left", null, null)), false));
            assertEquals(InstanceStatus.COMPENSATING.name(), out.instanceStatus(),
                    "a worker mid-run must be told the saga took the instance over");
        });
    }

    /** Runs once, after every case above has had its chance to reach a state. */
    @AfterAll
    static void everyTerminalStateIsCovered() {
        List<String> uncovered = StateChart.instances().states().stream()
                .filter(s -> s.kind().equals("terminal")).map(StateChart.State::name)
                .filter(s -> !COVERED.contains(s)).toList();
        assertTrue(uncovered.isEmpty(), "terminal states with no case asserting the invariant: "
                + uncovered + " (reached here: " + COVERED + ")");
    }

    /** The invariant itself: terminal instance, and not one token left active. */
    private static void assertTerminalAndQuiet(WorkflowEngine engine, String id, InstanceStatus expected) {
        assertEquals(expected.name(), status(engine, id));
        COVERED.add(expected.name());
        List<Token> stillActive = active(engine, id);
        assertTrue(stillActive.isEmpty(), "instance is " + expected + " but these tokens are still active: "
                + stillActive.stream().map(t -> t.nodeId + "=" + t.status).toList());
        for (Token t : engine.tokens(id)) {
            assertTrue(t.leaseOwner == null && t.leaseExpiresAt == 0,
                    "token " + t.nodeId + " (" + t.status + ") of a terminal instance still holds a lease");
        }
    }

    private static List<Token> active(WorkflowEngine engine, String id) {
        return engine.tokens(id).stream().filter(Token::isActive).toList();
    }

    private static String status(WorkflowEngine engine, String id) {
        return engine.instance(id).orElseThrow().status();
    }

    private static TaskActivation claim(WorkflowEngine engine) {
        List<TaskActivation> claimed = engine.poll("w1", Set.of(QUEUE), 1, 60_000L);
        assertEquals(1, claimed.size(), "expected exactly one dispatchable task");
        return claimed.getFirst();
    }

    private static void report(WorkflowEngine engine, String expectedNode) {
        TaskActivation t = claim(engine);
        assertEquals(expectedNode, t.nodeId(), "dispatched the wrong node");
        Reports.one(engine, t, null);
    }

    private static void drive(WorkflowDefinition def, Consumer<WorkflowEngine> body) {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            registry.register(def);
            body.accept(new WorkflowEngine(storage, registry, 60_000));
        }
    }

    private static WorkflowDefinition linear() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("one", task("one").withNext("two"));
        n.put("two", task("two").withNext("end"));
        n.put("end", Node.end("end", true, null));
        return new WorkflowDefinition("linear", 1, "one", n, Set.of(QUEUE));
    }

    private static WorkflowDefinition unsuccessfulEnd() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("one", task("one").withNext("end"));
        n.put("end", Node.end("end", false, "gave up"));
        return new WorkflowDefinition("dead-end", 1, "one", n, Set.of(QUEUE));
    }

    private static WorkflowDefinition forkJoin() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("fork", Node.fork("fork", "fork").withBranches(List.of("left", "right")).withNext("join"));
        n.put("left", task("left").withNext("join"));
        n.put("right", task("right").withNext("join"));
        n.put("join", Node.join("join", "join", 2).withNext("end"));
        n.put("end", Node.end("end", true, null));
        return new WorkflowDefinition("forked", 1, "fork", n, Set.of(QUEUE));
    }

    private static WorkflowDefinition saga() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("one", task("one").withCompensable().withNext("two"));
        n.put("two", task("two").withNext("end"));
        n.put("end", Node.end("end", true, null));
        return new WorkflowDefinition("saga", 1, "one", n, Set.of(QUEUE));
    }

    private static WorkflowDefinition sagaFork() {
        Map<String, Node> n = new LinkedHashMap<>();
        n.put("one", task("one").withCompensable().withNext("fork"));
        n.put("fork", Node.fork("fork", "fork").withBranches(List.of("left", "right")).withNext("join"));
        n.put("left", task("left").withNext("join"));
        n.put("right", task("right").withNext("join"));
        n.put("join", Node.join("join", "join", 2).withNext("end"));
        n.put("end", Node.end("end", true, null));
        return new WorkflowDefinition("saga-fork", 1, "one", n, Set.of(QUEUE), ExecutionMode.LOCAL_SYNC);
    }

    /** One attempt, so a reported failure is terminal for the token immediately. */
    private static Node task(String id) {
        return Node.task(id, id, id, QUEUE, RetryPolicy.none());
    }
}
