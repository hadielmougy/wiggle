package com.wiggle.server.engine;

import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.TaskActivation;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What {@code wiggle-engine} is on its own: a workflow state machine you can run inside your own
 * JVM with nothing else on the classpath -- no server, no gRPC, no database, no worker runtime.
 *
 * <p>This test is the proof rather than a demonstration: it lives in the engine module, whose
 * compile and test classpath is {@code wiggle-core} plus {@code wiggle-placement} and nothing more,
 * so it cannot accidentally lean on anything the published artifact does not carry. It builds a
 * graph from the core model (no authoring DSL, which ships in {@code wiggle-client}), drives it
 * with the same poll/complete loop a worker would, and checks the result -- which is exactly the
 * shape an embedder builds their own runner around.
 */
class EmbeddedEngineTest {

    private static final String QUEUE = "embedded";

    @Test @DisplayName("an embedder can register, start and drive a workflow with only the engine on the classpath")
    void runsAWorkflowInProcess() {
        // docs:begin embed
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            engine.register(greetThenShout());

            String id = engine.start("greet", null, Map.of("name", "ada"), "corr-1");

            // Your runner: claim what is ready, run it, report the result.
            long deadline = System.currentTimeMillis() + 10_000;
            while (isRunning(engine, id) && System.currentTimeMillis() < deadline) {
                for (TaskActivation task : engine.poll("embedder", Set.of(QUEUE), 10, null)) {
                    engine.complete(task.taskId(), "embedder", handle(task));
                }
            }
            // docs:skip
            InstanceView done = engine.instance(id).orElseThrow();
            assertEquals("COMPLETED", done.status());
            assertEquals("HELLO ADA", Json.asObject(done.context()).get("greeting"));
            // docs:resume
        }
        // docs:end embed
    }

    @Test @DisplayName("the in-memory store keeps the queries an embedder's own console would need")
    void exposesQueries() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            engine.register(greetThenShout());
            String id = engine.start("greet", null, Map.of("name", "grace"), "corr-2");

            assertEquals(List.of("greet"), engine.workflowNames());
            assertEquals(1, engine.list("greet", "RUNNING", 10).size());
            assertEquals(id, engine.findByCorrelation("corr-2", 10).getFirst().id());
            assertTrue(engine.queueDepth().readyCount() > 0, "the started instance left work queued");

            engine.cancel(id, "done experimenting");
            assertEquals("CANCELLED", engine.instance(id).orElseThrow().status());
        }
    }

    @Test @DisplayName("leader duties are the embedder's job: a due timer advances only when something runs them")
    void leaderDutiesAdvanceTheClock() throws Exception {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            engine.register(greetThenSleepThenShout());
            String id = engine.start("napping", null, Map.of("name", "ada"), null);

            for (TaskActivation task : engine.poll("embedder", Set.of(QUEUE), 10, null)) {
                engine.complete(task.taskId(), "embedder", handle(task));   // parks on the sleep
            }
            Thread.sleep(80);   // the timer is now due, and nothing has run the duties

            assertTrue(engine.poll("embedder", Set.of(QUEUE), 10, null).isEmpty(),
                    "a due timer is invisible to a poll until the duties promote it");
            assertEquals("RUNNING", engine.instance(id).orElseThrow().status());

            // The scheduler tick an embedder owns. In a server this is the leader's housekeeping.
            // docs:begin housekeeping
            engine.fireDueTimers(100);
            engine.fireDueSignalDeadlines(100);
            engine.reclaimExpiredLeases(100);
            engine.fireDueSchedules(100);
            // docs:end housekeeping

            List<TaskActivation> afterTick = engine.poll("embedder", Set.of(QUEUE), 10, null);
            assertEquals(1, afterTick.size(), "the timer fired and its continuation is claimable");
            engine.complete(afterTick.getFirst().taskId(), "embedder", handle(afterTick.getFirst()));
            assertEquals("COMPLETED", engine.instance(id).orElseThrow().status());
        }
    }

    private static boolean isRunning(WorkflowEngine engine, String id) {
        return engine.instance(id).orElseThrow().status().equals("RUNNING");
    }

    /** Stands in for the handler an embedder would bind: greet, then upper-case the greeting. */
    // docs:begin handler
    private static Object handle(TaskActivation task) {
        Map<String, Object> ctx = new LinkedHashMap<>(Json.asObject(task.context()));
        switch (task.activity()) {
            case "greet" -> ctx.put("greeting", "hello " + ctx.get("name"));
            case "shout" -> ctx.put("greeting", String.valueOf(ctx.get("greeting")).toUpperCase());
            default -> throw new AssertionError("unexpected activity " + task.activity());
        }
        return ctx;
    }
    // docs:end handler

    /** greet -> sleep -> shout -> end: the timer is what makes the duties observable. */
    private static WorkflowDefinition greetThenSleepThenShout() {
        Node greet = Node.task("n1", "greet", "greet", QUEUE, RetryPolicy.forever()).withNext("n2");
        Node nap = Node.sleep("n2", "nap", 50).withNext("n3");
        Node shout = Node.task("n3", "shout", "shout", QUEUE, RetryPolicy.forever()).withNext("n4");
        Node end = Node.end("n4", true, null);
        Map<String, Node> nodes = new LinkedHashMap<>();
        for (Node n : List.of(greet, nap, shout, end)) nodes.put(n.id(), n);
        int version = WorkflowDefinition.contentVersion("napping", "n1", nodes.values(), null, Set.of());
        return new WorkflowDefinition("napping", version, "n1", nodes, Set.of(QUEUE));
    }

    /** greet -> shout -> end, assembled straight from the core model. */
    private static WorkflowDefinition greetThenShout() {
        Node greet = Node.task("n1", "greet", "greet", QUEUE, RetryPolicy.forever()).withNext("n2");
        Node shout = Node.task("n2", "shout", "shout", QUEUE, RetryPolicy.forever()).withNext("n3");
        Node end = Node.end("n3", true, null);
        Map<String, Node> nodes = new LinkedHashMap<>();
        for (Node n : List.of(greet, shout, end)) nodes.put(n.id(), n);
        int version = WorkflowDefinition.contentVersion("greet", "n1", nodes.values(), null, Set.of());
        return new WorkflowDefinition("greet", version, "n1", nodes, Set.of(QUEUE));
    }
}
