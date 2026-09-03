package com.wiggle.client.dsl;

import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Pipeline}, the package-private graph builder behind the DSL. These
 * drive its API directly (rather than through {@link WorkflowStream}) so each responsibility --
 * id assignment, name uniqueness, edge wiring, queue tracking, and the validation/content-versioning
 * done by {@link Pipeline#build()} -- is checked in isolation. The pipeline builds pure topology now:
 * it declares named nodes (task/guard/combine/...) with retry and queue, but no step logic.
 */
class PipelineTest {

    private static Pipeline pipeline() {
        return new Pipeline("wf", null);
    }

    /** Builds a valid two-node graph: one task wired to a terminal end. */
    private static WorkflowDefinition linearTaskGraph(Pipeline p, String stepName) {
        String task = p.addTask(stepName, null, null);
        p.startAt(task);
        p.wireNext(task, p.addEnd(null));
        return p.build().definition();
    }

    // -------------------------------------------------------------------- node ids

    @Nested
    @DisplayName("node ids")
    class Ids {

        @Test
        @DisplayName("each kind gets its own prefix, numbered by a shared counter")
        void prefixesAndCounter() {
            Pipeline p = pipeline();
            assertEquals("n1", p.addTask("a", null, null));
            assertEquals("n2", p.addGuard("g", null, null));
            assertEquals("n3", p.addSleep("s", 10));
            assertEquals("n4", p.addSignal("sig", 0));
            assertEquals("fork5", p.addFork());
            assertEquals("join6", p.addJoin(2));
            assertEquals("dynfork7", p.addDynFork("each", "items", "item"));
            assertEquals("end8", p.addEnd(null));
        }

        @Test
        @DisplayName("ids are unique even when step names are not addressable (sleep)")
        void sleepIdsAreDistinct() {
            Pipeline p = pipeline();
            assertNotEquals(p.addSleep("s", 1), p.addSleep("s", 1));
        }
    }

    @Nested
    @DisplayName("name uniqueness")
    class Names {

        @Test
        @DisplayName("a duplicate step name is rejected")
        void duplicateStep() {
            Pipeline p = pipeline();
            p.addTask("dup", null, null);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> p.addTask("dup", null, null));
            assertTrue(e.getMessage().contains("duplicate step name 'dup'"), e.getMessage());
        }

        @Test
        @DisplayName("names are shared across step, guard, signal, sub-workflow and forkEach")
        void uniquenessSpansKinds() {
            assertThrows(IllegalArgumentException.class, () -> {
                Pipeline p = pipeline();
                p.addTask("x", null, null);
                p.addGuard("x", null, null);        // collides with the step
            });
            assertThrows(IllegalArgumentException.class, () -> {
                Pipeline p = pipeline();
                p.addTask("x", null, null);
                p.addSignal("x", 0);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                Pipeline p = pipeline();
                p.addTask("x", null, null);
                p.addSubWorkflow("x", "child");
            });
            assertThrows(IllegalArgumentException.class, () -> {
                Pipeline p = pipeline();
                p.addTask("x", null, null);
                p.addDynFork("x", "items", "item");
            });
        }

        @Test
        @DisplayName("sleep names are not reserved, so they never collide")
        void sleepNamesAreNotReserved() {
            Pipeline p = pipeline();
            p.addTask("s", null, null);
            p.addSleep("s", 1);   // shares the name with the step -- allowed
            p.addSleep("s", 2);   // and with another sleep -- allowed
            // no exception
        }

        @Test
        @DisplayName("a blank name is rejected")
        void blankName() {
            Pipeline p = pipeline();
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> p.addTask("  ", null, null));
            assertTrue(e.getMessage().contains("step name is required"), e.getMessage());
        }
    }

    @Nested
    @DisplayName("combine")
    class Combine {

        @Test
        @DisplayName("a combine is a task node that carries its fork arm names on itemsKey")
        void combineCarriesArmNames() {
            Pipeline p = pipeline();
            String fork = p.addFork();
            String b1 = p.addTask("b1", null, null);
            String b2 = p.addTask("b2", null, null);
            String join = p.addJoin(2);
            String merge = p.addCombine("merge", List.of("air", "hotel"), null, null);
            p.startAt(fork);
            p.setBranches(fork, List.of(b1, b2));
            p.wireNext(b1, join);
            p.wireNext(b2, join);
            p.wireNext(join, merge);
            p.wireNext(merge, p.addEnd(null));
            WorkflowDefinition def = p.build().definition();

            assertEquals(NodeKind.TASK, def.node(merge).kind());
            assertEquals("[\"air\",\"hotel\"]", def.node(merge).itemsKey(),
                    "the arm names ride on the combine node as a JSON array, in fork order");
        }
    }

    @Nested
    @DisplayName("queues")
    class Queues {

        @Test
        @DisplayName("steps use the workflow name as the default queue")
        void defaultQueueIsWorkflowName() {
            Pipeline p = pipeline();
            WorkflowDefinition def = linearTaskGraph(p, "a");
            assertEquals("wf", def.node(def.startNode()).queue());
            assertTrue(def.queues().contains("wf"));
        }

        @Test
        @DisplayName("defaultQueue changes the queue of subsequently added steps")
        void changingDefaultQueue() {
            Pipeline p = pipeline();
            String a = p.addTask("a", null, null);   // on "wf"
            p.defaultQueue("batch");
            String b = p.addTask("b", null, null);   // on "batch"
            p.startAt(a);
            p.wireNext(a, b);
            p.wireNext(b, p.addEnd(null));
            WorkflowDefinition def = p.build().definition();

            assertEquals("wf", def.node(a).queue());
            assertEquals("batch", def.node(b).queue());
            assertTrue(def.queues().containsAll(List.of("wf", "batch")));
        }

        @Test
        @DisplayName("a per-step queue routes a single node and records the new queue")
        void perStepQueueRoutesOneNode() {
            Pipeline p = pipeline();
            String a = p.addTask("a", null, "gpu");
            p.startAt(a);
            p.wireNext(a, p.addEnd(null));
            WorkflowDefinition def = p.build().definition();

            assertEquals("gpu", def.node(a).queue());
            assertTrue(def.queues().contains("gpu"));
        }
    }

    @Nested
    @DisplayName("workflow settings")
    class Settings {

        @Test
        @DisplayName("execution mode defaults to DEFAULT and is carried onto the definition")
        void executionMode() {
            Pipeline p1 = pipeline();
            assertEquals(ExecutionMode.DEFAULT, linearTaskGraph(p1, "a").executionMode());

            Pipeline p2 = pipeline();
            p2.executionMode(ExecutionMode.LOCAL_ASYNC);
            assertEquals(ExecutionMode.LOCAL_ASYNC, linearTaskGraph(p2, "a").executionMode());
        }

        @Test
        @DisplayName("markCheckpoint records the node id on the definition")
        void checkpoints() {
            Pipeline p = pipeline();
            String a = p.addTask("a", null, null);
            p.markCheckpoint(a);
            p.startAt(a);
            p.wireNext(a, p.addEnd(null));
            assertEquals(java.util.Set.of(a), p.build().definition().checkpoints());
        }
    }

    @Nested
    @DisplayName("edge wiring")
    class Wiring {

        @Test
        @DisplayName("wireNext and wireAlt set the two outgoing edges independently")
        void nextAndAlt() {
            Pipeline p = pipeline();
            String g = p.addGuard("g", null, null);
            String pass = p.addEnd(null);
            String fail = p.addEnd("gated:g");
            p.startAt(g);
            p.wireNext(g, pass);
            p.wireAlt(g, fail);
            WorkflowDefinition def = p.build().definition();

            assertEquals(pass, def.node(g).next());
            assertEquals(fail, def.node(g).altNext());
        }

        @Test
        @DisplayName("setBranches records a fork's branch starts")
        void forkBranches() {
            Pipeline p = pipeline();
            String fork = p.addFork();
            String join = p.addJoin(2);
            String b1 = p.addTask("b1", null, null);
            String b2 = p.addTask("b2", null, null);
            p.startAt(fork);
            p.setBranches(fork, List.of(b1, b2));
            p.wireNext(b1, join);
            p.wireNext(b2, join);
            p.wireNext(join, p.addEnd(null));
            WorkflowDefinition def = p.build().definition();

            assertEquals(List.of(b1, b2), def.node(fork).branches());
            assertEquals(NodeKind.JOIN, def.node(join).kind());
            assertEquals(2, def.node(join).expected());
        }
    }

    @Nested
    @DisplayName("build validation")
    class Validation {

        @Test
        @DisplayName("building without a start node fails")
        void noStart() {
            Pipeline p = pipeline();
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("defines no steps"), e.getMessage());
        }

        @Test
        @DisplayName("a task with no successor fails validation")
        void taskWithoutSuccessor() {
            Pipeline p = pipeline();
            String a = p.addTask("a", null, null);
            p.startAt(a);   // never wired onward
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("has no successor"), e.getMessage());
        }

        @Test
        @DisplayName("a predicate with no false branch fails validation")
        void predicateWithoutFalseBranch() {
            Pipeline p = pipeline();
            String g = p.addGuard("g", null, null);
            p.startAt(g);
            p.wireNext(g, p.addEnd(null));   // true edge only; altNext left null
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("no false branch"), e.getMessage());
        }

        @Test
        @DisplayName("a fork with fewer than two branches fails validation")
        void forkTooFewBranches() {
            Pipeline p = pipeline();
            String fork = p.addFork();
            String only = p.addTask("only", null, null);
            String join = p.addJoin(1);
            p.startAt(fork);
            p.setBranches(fork, List.of(only));
            p.wireNext(only, join);
            p.wireNext(join, p.addEnd(null));
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("fewer than two branches"), e.getMessage());
        }

        @Test
        @DisplayName("an edge pointing at an unknown node fails validation")
        void danglingEdge() {
            Pipeline p = pipeline();
            String a = p.addTask("a", null, null);
            p.startAt(a);
            p.wireNext(a, "ghost");   // no such node
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("unknown node"), e.getMessage());
        }

        @Test
        @DisplayName("a valid linear graph builds")
        void validGraphBuilds() {
            Pipeline p = pipeline();
            WorkflowDefinition def = linearTaskGraph(p, "a");
            assertEquals("wf", def.name());
            assertEquals(NodeKind.TASK, def.node(def.startNode()).kind());
        }
    }

    @Nested
    @DisplayName("content versioning")
    class Versioning {

        @Test
        @DisplayName("identical graphs hash to the same non-zero version")
        void deterministic() {
            int v1 = linearTaskGraph(pipeline(), "a").version();
            int v2 = linearTaskGraph(pipeline(), "a").version();
            assertEquals(v1, v2);
            assertNotEquals(0, v1);
        }

        @Test
        @DisplayName("a different step name yields a different version")
        void sensitiveToShape() {
            assertNotEquals(linearTaskGraph(pipeline(), "a").version(),
                    linearTaskGraph(pipeline(), "b").version());
        }

        @Test
        @DisplayName("the execution mode is part of the content hash")
        void sensitiveToExecutionMode() {
            Pipeline async = pipeline();
            async.executionMode(ExecutionMode.LOCAL_ASYNC);
            assertNotEquals(linearTaskGraph(pipeline(), "a").version(),
                    linearTaskGraph(async, "a").version());
        }
    }

    @Test
    @DisplayName("a null retry policy falls back to the workflow default")
    void retryDefaulting() {
        RetryPolicy custom = RetryPolicy.exponential(7, Duration.ofMillis(250));
        Pipeline p = new Pipeline("wf", custom);
        String a = p.addTask("a", null, null);            // inherits the default
        String b = p.addTask("b", RetryPolicy.none(), null);   // explicit override
        p.startAt(a);
        p.wireNext(a, b);
        p.wireNext(b, p.addEnd(null));
        WorkflowDefinition def = p.build().definition();

        assertEquals(7, def.node(a).retry().maxAttempts());
        assertEquals(1, def.node(b).retry().maxAttempts());
        assertFalse(def.node(a).id().isBlank());
    }

    @Nested
    @DisplayName("name-only overloads")
    class NameOnlyOverloads {

        private Node byActivity(WorkflowDefinition def, String activity) {
            return def.nodes().values().stream()
                    .filter(n -> activity.equals(n.activity()))
                    .findFirst().orElseThrow(() -> new AssertionError("no node for activity " + activity));
        }

        @Test
        @DisplayName("gate(name, queue) honours the queue and declares a PREDICATE (bound by name)")
        void gateNameOnlyQueue() {
            Blueprint bp = Workflow.define("wf")
                    .gate("check", "gpu")
                    .step("run")
                    .build();
            WorkflowDefinition def = bp.definition();

            assertEquals("gpu", byActivity(def, "wf#check").queue(), "queue must be honoured, not dropped");
            assertEquals(NodeKind.PREDICATE, byActivity(def, "wf#check").kind());
            assertTrue(def.queues().contains("gpu"));
        }

        @Test
        @DisplayName("gate(name, retry, queue) honours both the retry policy and the queue")
        void gateNameOnlyRetryAndQueue() {
            Blueprint bp = Workflow.define("wf")
                    .gate("check", RetryPolicy.exponential(7, Duration.ofMillis(50)), "gpu")
                    .step("run")
                    .build();
            Node gate = byActivity(bp.definition(), "wf#check");
            assertEquals("gpu", gate.queue());
            assertEquals(7, gate.retry().maxAttempts());
        }

        @Test
        @DisplayName("step(name, queue) / effect(name) route correctly")
        void stepAndEffectNameOnly() {
            Blueprint bp = Workflow.define("wf")
                    .step("ingest", "gpu")
                    .effect("notify")
                    .build();
            WorkflowDefinition def = bp.definition();

            assertEquals("gpu", byActivity(def, "wf#ingest").queue());
            assertEquals("wf", byActivity(def, "wf#notify").queue(), "no queue -> the workflow-name default");
        }

        @Test
        @DisplayName("a name-only workflow declares topology only (the worker binds handlers by name)")
        void nameOnlyDeclaresTopologyOnly() {
            // The DSL declares topology only; the worker binds the handler by name. The blueprint
            // therefore carries just the graph -- there is no baked step logic to collide with.
            Blueprint bp = Workflow.define("wf").step("check").build();
            assertEquals(NodeKind.TASK, byActivity(bp.definition(), "wf#check").kind());
        }
    }
}
