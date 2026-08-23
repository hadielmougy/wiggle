package dev.wiggle.client.dsl;

import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.ExecutionMode;
import dev.wiggle.core.Node;
import dev.wiggle.core.NodeKind;
import dev.wiggle.core.RetryPolicy;
import dev.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link Pipeline}, the package-private graph builder behind the DSL. These
 * drive its API directly (rather than through {@link WorkflowStream}) so each responsibility --
 * id assignment, name uniqueness, handler registration, edge wiring, queue tracking, and the
 * validation/content-versioning done by {@link Pipeline#build()} -- is checked in isolation.
 */
class PipelineTest {

    private static Pipeline<Map<String, Object>> pipeline() {
        return new Pipeline<>("wf", ContextCodec.jsonMap(), null);
    }

    /** A copy of {@code ctx} with one key set -- steps must return the whole context. */
    private static Map<String, Object> with(Map<String, Object> ctx, String key, Object value) {
        Map<String, Object> m = new LinkedHashMap<>(ctx);
        m.put(key, value);
        return m;
    }

    /** Builds a valid two-node graph: one task wired to a terminal end. */
    private static WorkflowDefinition linearTaskGraph(Pipeline<Map<String, Object>> p, String stepName) {
        String task = p.addStep(stepName, ctx -> ctx, null);
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
            Pipeline<Map<String, Object>> p = pipeline();
            assertEquals("n1", p.addStep("a", ctx -> ctx, null));
            assertEquals("n2", p.addGuard("g", ctx -> true, null));
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
            Pipeline<Map<String, Object>> p = pipeline();
            assertNotEquals(p.addSleep("s", 1), p.addSleep("s", 1));
        }
    }

    @Nested
    @DisplayName("name uniqueness")
    class Names {

        @Test
        @DisplayName("a duplicate step name is rejected")
        void duplicateStep() {
            Pipeline<Map<String, Object>> p = pipeline();
            p.addStep("dup", ctx -> ctx, null);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> p.addStep("dup", ctx -> ctx, null));
            assertTrue(e.getMessage().contains("duplicate step name 'dup'"), e.getMessage());
        }

        @Test
        @DisplayName("names are shared across step, guard, signal, sub-workflow and forkEach")
        void uniquenessSpansKinds() {
            assertThrows(IllegalArgumentException.class, () -> {
                Pipeline<Map<String, Object>> p = pipeline();
                p.addStep("x", ctx -> ctx, null);
                p.addGuard("x", ctx -> true, null);        // collides with the step
            });
            assertThrows(IllegalArgumentException.class, () -> {
                Pipeline<Map<String, Object>> p = pipeline();
                p.addStep("x", ctx -> ctx, null);
                p.addSignal("x", 0);
            });
            assertThrows(IllegalArgumentException.class, () -> {
                Pipeline<Map<String, Object>> p = pipeline();
                p.addStep("x", ctx -> ctx, null);
                p.addSubWorkflow("x", "child");
            });
            assertThrows(IllegalArgumentException.class, () -> {
                Pipeline<Map<String, Object>> p = pipeline();
                p.addStep("x", ctx -> ctx, null);
                p.addDynFork("x", "items", "item");
            });
        }

        @Test
        @DisplayName("sleep names are not reserved, so they never collide")
        void sleepNamesAreNotReserved() {
            Pipeline<Map<String, Object>> p = pipeline();
            p.addStep("s", ctx -> ctx, null);
            p.addSleep("s", 1);   // shares the name with the step -- allowed
            p.addSleep("s", 2);   // and with another sleep -- allowed
            // no exception
        }

        @Test
        @DisplayName("a blank name is rejected")
        void blankName() {
            Pipeline<Map<String, Object>> p = pipeline();
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> p.addStep("  ", ctx -> ctx, null));
            assertTrue(e.getMessage().contains("step name is required"), e.getMessage());
        }
    }

    @Nested
    @DisplayName("activity handlers")
    class Handlers {

        @Test
        @DisplayName("a step's handler is keyed by 'workflow#name' and returns the context diff")
        void stepHandlerReturnsDiff() throws Exception {
            Pipeline<Map<String, Object>> p = pipeline();
            String id = p.addStep("greet", ctx -> with(ctx, "greeting", "hi"), null);
            p.startAt(id);
            p.wireNext(id, p.addEnd(null));
            Blueprint<Map<String, Object>> bp = p.build();

            ActivityHandler h = bp.handlers().get("wf#greet");
            assertEquals(Map.of("greeting", "hi"), h.invoke(Map.of("name", "ada")));
        }

        @Test
        @DisplayName("an effect's handler returns null (context unchanged)")
        void effectHandlerReturnsNull() throws Exception {
            Pipeline<Map<String, Object>> p = pipeline();
            String id = p.addEffect("audit", ctx -> { /* observe only */ }, null);
            p.startAt(id);
            p.wireNext(id, p.addEnd(null));
            Blueprint<Map<String, Object>> bp = p.build();

            assertNull(bp.handlers().get("wf#audit").invoke(Map.of("a", 1L)));
        }

        @Test
        @DisplayName("a guard's handler returns a Boolean")
        void guardHandlerReturnsBoolean() throws Exception {
            Pipeline<Map<String, Object>> p = pipeline();
            String g = p.addGuard("positive", ctx -> ((Number) ctx.get("n")).intValue() > 0, null);
            p.startAt(g);
            p.wireNext(g, p.addEnd(null));
            p.wireAlt(g, p.addEnd("gated:positive"));
            Blueprint<Map<String, Object>> bp = p.build();

            ActivityHandler h = bp.handlers().get("wf#positive");
            assertEquals(Boolean.TRUE, h.invoke(Map.of("n", 3L)));
            assertEquals(Boolean.FALSE, h.invoke(Map.of("n", -1L)));
        }
    }

    @Nested
    @DisplayName("queues")
    class Queues {

        @Test
        @DisplayName("steps use the workflow name as the default queue")
        void defaultQueueIsWorkflowName() {
            Pipeline<Map<String, Object>> p = pipeline();
            WorkflowDefinition def = linearTaskGraph(p, "a");
            assertEquals("wf", def.node(def.startNode()).queue());
            assertTrue(def.queues().contains("wf"));
        }

        @Test
        @DisplayName("defaultQueue changes the queue of subsequently added steps")
        void changingDefaultQueue() {
            Pipeline<Map<String, Object>> p = pipeline();
            String a = p.addStep("a", ctx -> ctx, null);   // on "wf"
            p.defaultQueue("batch");
            String b = p.addStep("b", ctx -> ctx, null);   // on "batch"
            p.startAt(a);
            p.wireNext(a, b);
            p.wireNext(b, p.addEnd(null));
            WorkflowDefinition def = p.build().definition();

            assertEquals("wf", def.node(a).queue());
            assertEquals("batch", def.node(b).queue());
            assertTrue(def.queues().containsAll(List.of("wf", "batch")));
        }

        @Test
        @DisplayName("setQueue reroutes a single node and records the new queue")
        void setQueueReroutesOneNode() {
            Pipeline<Map<String, Object>> p = pipeline();
            String a = p.addStep("a", ctx -> ctx, null);
            p.setQueue(a, "gpu");
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
            Pipeline<Map<String, Object>> p1 = pipeline();
            assertEquals(ExecutionMode.DEFAULT, linearTaskGraph(p1, "a").executionMode());

            Pipeline<Map<String, Object>> p2 = pipeline();
            p2.executionMode(ExecutionMode.LOCAL_ASYNC);
            assertEquals(ExecutionMode.LOCAL_ASYNC, linearTaskGraph(p2, "a").executionMode());
        }

        @Test
        @DisplayName("markCheckpoint records the node id on the definition")
        void checkpoints() {
            Pipeline<Map<String, Object>> p = pipeline();
            String a = p.addStep("a", ctx -> ctx, null);
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
            Pipeline<Map<String, Object>> p = pipeline();
            String g = p.addGuard("g", ctx -> true, null);
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
            Pipeline<Map<String, Object>> p = pipeline();
            String fork = p.addFork();
            String join = p.addJoin(2);
            String b1 = p.addStep("b1", ctx -> ctx, null);
            String b2 = p.addStep("b2", ctx -> ctx, null);
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
            Pipeline<Map<String, Object>> p = pipeline();
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("defines no steps"), e.getMessage());
        }

        @Test
        @DisplayName("a task with no successor fails validation")
        void taskWithoutSuccessor() {
            Pipeline<Map<String, Object>> p = pipeline();
            String a = p.addStep("a", ctx -> ctx, null);
            p.startAt(a);   // never wired onward
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("has no successor"), e.getMessage());
        }

        @Test
        @DisplayName("a predicate with no false branch fails validation")
        void predicateWithoutFalseBranch() {
            Pipeline<Map<String, Object>> p = pipeline();
            String g = p.addGuard("g", ctx -> true, null);
            p.startAt(g);
            p.wireNext(g, p.addEnd(null));   // true edge only; altNext left null
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("no false branch"), e.getMessage());
        }

        @Test
        @DisplayName("a fork with fewer than two branches fails validation")
        void forkTooFewBranches() {
            Pipeline<Map<String, Object>> p = pipeline();
            String fork = p.addFork();
            String only = p.addStep("only", ctx -> ctx, null);
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
            Pipeline<Map<String, Object>> p = pipeline();
            String a = p.addStep("a", ctx -> ctx, null);
            p.startAt(a);
            p.wireNext(a, "ghost");   // no such node
            IllegalStateException e = assertThrows(IllegalStateException.class, p::build);
            assertTrue(e.getMessage().contains("unknown node"), e.getMessage());
        }

        @Test
        @DisplayName("a valid linear graph builds")
        void validGraphBuilds() {
            Pipeline<Map<String, Object>> p = pipeline();
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
            Pipeline<Map<String, Object>> async = pipeline();
            async.executionMode(ExecutionMode.LOCAL_ASYNC);
            assertNotEquals(linearTaskGraph(pipeline(), "a").version(),
                    linearTaskGraph(async, "a").version());
        }
    }

    @Test
    @DisplayName("a null retry policy falls back to the workflow default")
    void retryDefaulting() {
        RetryPolicy custom = RetryPolicy.exponential(7, Duration.ofMillis(250));
        Pipeline<Map<String, Object>> p = new Pipeline<>("wf", ContextCodec.jsonMap(), custom);
        String a = p.addStep("a", ctx -> ctx, null);            // inherits the default
        String b = p.addStep("b", ctx -> ctx, RetryPolicy.none());   // explicit override
        p.startAt(a);
        p.wireNext(a, b);
        p.wireNext(b, p.addEnd(null));
        WorkflowDefinition def = p.build().definition();

        assertEquals(7, def.node(a).retry().maxAttempts());
        assertEquals(1, def.node(b).retry().maxAttempts());
        assertFalse(def.node(a).id().isBlank());
    }
}
