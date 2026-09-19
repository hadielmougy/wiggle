package com.wiggle.client.flow;

import com.wiggle.core.Node;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Where retry and queue are accepted, they reach the node. The split is deliberate: a node you named
 * with a handler takes both, a node a construct creates for you takes only the queue.
 */
class CombineOptsTest {

    private static final RetryPolicy P = RetryPolicy.fixed(7, Duration.ofMillis(10));

    interface S {
        Map<String, Object> a(Map<String, Object> c);
        Map<String, Object> b(Map<String, Object> c);
        Map<String, Object> merge(Map<String, Object> x, Map<String, Object> y);
        boolean more(Map<String, Object> c);
        Map<String, Object> drain(Map<String, Object> c);
        Map<String, Object> each(Map<String, Object> item);
        Map<String, Object> collect(List<Map<String, Object>> items);
    }

    private static Node named(WorkflowDefinition d, String n) {
        return d.nodes().values().stream().filter(x -> n.equals(x.name())).findFirst().orElseThrow();
    }

    @Test @DisplayName("an allOf combine takes a retry policy and a queue, in either order")
    void allOfCombineTakesBoth() {
        WorkflowDefinition d = FlowSpec.define("co-fork", 1, Map.class, S.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a), f.thenApply(s::b))
                        .combine(s::merge, P, "merge-q")).definition();
        assertEquals("merge-q", named(d, "merge").queue());
        assertEquals(7, named(d, "merge").retry().maxAttempts());

        WorkflowDefinition flipped = FlowSpec.define("co-fork-flipped", 1, Map.class, S.class, (f, s) ->
                Wiggle.allOf(f.thenApply(s::a), f.thenApply(s::b))
                        .combine(s::merge, "merge-q", P)).definition();
        assertEquals("merge-q", named(flipped, "merge").queue());
        assertEquals(7, named(flipped, "merge").retry().maxAttempts());
    }

    @Test @DisplayName("a forEach combine takes a retry policy and a queue, in either order")
    void forEachCombineTakesBoth() {
        WorkflowDefinition d = FlowSpec.define("co-each", 1, Map.class, S.class, (f, s) ->
                f.thenForEach("items", Map.class, i -> i.thenApply(s::each))
                        .combine(s::collect, P, "collect-q")).definition();
        assertEquals("collect-q", named(d, "collect").queue());
        assertEquals(7, named(d, "collect").retry().maxAttempts());

        WorkflowDefinition flipped = FlowSpec.define("co-each-flipped", 1, Map.class, S.class, (f, s) ->
                f.thenForEach("items", Map.class, i -> i.thenApply(s::each))
                        .combine(s::collect, "collect-q", P)).definition();
        assertEquals("collect-q", named(flipped, "collect").queue());
        assertEquals(7, named(flipped, "collect").retry().maxAttempts());
    }

    @Test @DisplayName("a loop condition takes a queue; its retry stays the workflow default")
    void loopConditionTakesQueueOnly() {
        RetryPolicy wfDefault = RetryPolicy.fixed(3, Duration.ofMillis(5));

        WorkflowDefinition d = FlowSpec.define("co-loop", 1, wfDefault, Map.class, S.class, (f, s) ->
                f.repeatWhile(s::more, b -> b.thenApply(s::drain), "loop-q")).definition();
        assertEquals("loop-q", named(d, "more").queue());
        assertEquals(3, named(d, "more").retry().maxAttempts(), "the condition falls back to the default");

        WorkflowDefinition capped = FlowSpec.define("co-loop-capped", 1, wfDefault, Map.class, S.class, (f, s) ->
                f.repeatWhile(s::more, 4, b -> b.thenApply(s::drain), "loop-q")).definition();
        assertEquals("loop-q", named(capped, "more").queue());
        assertEquals(3, named(capped, "more").retry().maxAttempts());
    }
}
