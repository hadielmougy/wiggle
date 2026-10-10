package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.CreatedBranch;
import com.wiggle.core.CreatedBranch.BranchStep;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.TaskActivation;
import com.wiggle.server.engine.WorkflowEngine.Run;
import com.wiggle.server.engine.WorkflowEngine.StepInput;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The engine's side of created branches ({@link Spawns}): what it refuses from a reporter that did
 * not check, and how a created step is dispatched. The worker's side runs in DynamicFlowTest.
 */
class SpawnsTest {

    interface Steps {
        Map<String, Object> fulfil(Map<String, Object> ctx);
        Map<String, Object> summarise(List<Map<String, Object>> results);
        Map<String, Object> plain(Map<String, Object> ctx);
    }

    private static FlowSpec spawning() {
        return FlowSpec.define("sp-flow", 1, Map.class, Steps.class, (f, s) -> f
                .executeInLocalSync()
                .thenApply(s::fulfil, RetryPolicy.fixed(7, java.time.Duration.ofSeconds(1)), "fulfilment")
                .combine(s::summarise));
    }

    private static FlowSpec plain() {
        return FlowSpec.define("sp-plain", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::plain));
    }

    private static CreatedBranch branch(String key, BranchStep... steps) {
        return new CreatedBranch(Map.of("item", "x"), key, List.of(steps));
    }

    private static BranchStep task(String name) {
        return BranchStep.step(name, NodeKind.TASK, false, null, null);
    }

    /** Starts one instance and reports its first step as having created {@code branches}. */
    private static String reportFirst(WorkflowEngine engine, FlowSpec spec, List<CreatedBranch> branches) {
        String id = engine.start(spec.name(), spec.version(), Map.of(), null);
        TaskActivation first = engine.poll("w", spec.definition().queues(), 1, null).getFirst();
        engine.report(new Run(first.taskId(), "w", List.of(new StepInput(first.nodeId(), Map.of("base", true),
                null, null, null, List.of(), branches)), true));
        return id;
    }

    private static void withEngine(FlowSpec spec, java.util.function.Consumer<WorkflowEngine> body) {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            registry.register(spec.definition());
            body.accept(new WorkflowEngine(storage, registry, 30_000));
        }
    }

    @Test @DisplayName("a step with no combine after it has its branches refused, failing the instance")
    void refusesAStepThatMayNotCreate() {
        withEngine(plain(), engine -> {
            String id = reportFirst(engine, plain(), List.of(branch(null, task("plain"))));
            var v = engine.instance(id).orElseThrow();
            assertEquals("FAILED", v.status());
            assertTrue(v.error().contains("only a step followed directly by a combine"), v.error());
        });
    }

    @Test @DisplayName("keys are all-or-none across one report")
    void refusesPartlyKeyedBranches() {
        withEngine(spawning(), engine -> {
            String id = reportFirst(engine, spawning(), List.of(branch("a", task("x")), branch(null, task("y"))));
            var v = engine.instance(id).orElseThrow();
            assertEquals("FAILED", v.status());
            assertTrue(v.error().contains("either all do or none"), v.error());
        });
    }

    @Test @DisplayName("a step kind the server does not know is refused, not dispatched")
    void refusesAnUnknownKind() {
        withEngine(spawning(), engine -> {
            String id = reportFirst(engine, spawning(),
                    List.of(branch(null, new BranchStep("x", null, false, null, null, 0))));
            assertTrue(engine.instance(id).orElseThrow().error().contains("unknown step kind"));
        });
    }

    @Test @DisplayName("only a task in a branch may create branches of its own")
    void refusesAGateThatCreates() {
        withEngine(spawning(), engine -> {
            String id = reportFirst(engine, spawning(), List.of(branch(null,
                    BranchStep.step("check", NodeKind.PREDICATE, false, null, null).withCombine("merge"))));
            assertTrue(engine.instance(id).orElseThrow().error().contains("only a task can create branches"));
        });
    }

    @Test @DisplayName("a created step runs one step at a time, on the creating step's queue and policy")
    void createdStepsDispatchServerSide() {
        withEngine(spawning(), engine -> {
            reportFirst(engine, spawning(), List.of(branch(null, task("pack")), branch(null,
                    BranchStep.step("ship", NodeKind.TASK, false, "shipping", RetryPolicy.none()))));
            List<TaskActivation> onCreatorQueue = engine.poll("w", java.util.Set.of("fulfilment"), 10, null);
            assertEquals(1, onCreatorQueue.size(), "a step without a queue takes the creating step's");
            TaskActivation pack = onCreatorQueue.getFirst();
            assertEquals("sp-flow#pack", pack.activity());
            assertTrue(CreatedBranch.isCreatedNode(pack.nodeId()), pack.nodeId());
            assertEquals(ExecutionMode.SERVER, pack.executionMode(), "no worker graph holds a created node");
            assertEquals(Map.of("base", true), pack.baseContext(), "the base is what the creating step returned");
            assertEquals(1, engine.poll("w", java.util.Set.of("shipping"), 10, null).size(),
                    "a step's own queue wins");
        });
    }

    @Test @DisplayName("a report without branches from a spawning step goes straight to the combine")
    void noBranchesGoesToTheCombine() {
        withEngine(spawning(), engine -> {
            reportFirst(engine, spawning(), List.of());
            TaskActivation combine = engine.poll("w", spawning().definition().queues(), 1, null).getFirst();
            assertEquals("summarise", combine.stepName());
            assertNull(combine.itemMapKey());
        });
    }
}
