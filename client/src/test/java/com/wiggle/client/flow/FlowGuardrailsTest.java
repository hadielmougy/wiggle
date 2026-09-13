package com.wiggle.client.flow;

import com.wiggle.client.flow.Fixtures.Order;
import com.wiggle.client.flow.Fixtures.OrderHandlers;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rules that keep a future-shaped definition honest about the model underneath it. The important
 * ones are enforced by the types and so cannot be written down as a test at all -- see
 * {@link #forkStagesDoNotYieldAFutureSoAForgottenCombineCannotBeWritten()} -- and this covers the rest.
 */
class FlowGuardrailsTest {

    private final OrderHandlers h = new OrderHandlers();

    @Test
    void continuingTheSameFutureTwiceIsRejectedAndPointsAtFork() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                Wiggle.define("reuse", Order.class, f -> {
                    WiggleFuture<Order> validated = f.thenApply(h::validate);
                    validated.thenApply(h::vipPath);          // one successor...
                    return validated.thenApply(h::standardPath);   // ...and a second: that is a fan-out
                }));

        assertTrue(ex.getMessage().contains("single-use"), ex.getMessage());
        assertTrue(ex.getMessage().contains("thenFork"),
                "the error must name the construct that does express parallelism: " + ex.getMessage());
    }

    @Test
    void forkStagesDoNotYieldAFutureSoAForgottenCombineCannotBeWritten() {
        // thenFork returns a Fork2/Fork3/ForkN, not a WiggleFuture, and only combine() turns one back
        // into a future -- so a fork without a combine has nothing to continue or return, and the
        // "forgotten combine" the name-based DSL can only catch at build() is unwritable here.
        // This test documents that; there is nothing to assert at run time.
        Wiggle.define("combined", Order.class, f -> f
                .thenFork(Arm.of("a", a -> a.thenApply(h::vipPath)),
                          Arm.of("b", a -> a.thenApply(h::standardPath)))
                .combine("merge", Order.class));
    }

    @Test
    void aBodyThatReturnsNullIsRejectedWithTheReason() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                Wiggle.define("null-body", Order.class, f -> null));

        assertTrue(ex.getMessage().contains("must return the future it ends on"), ex.getMessage());
    }

    @Test
    void anArmThatReturnsNullIsRejectedNamingTheArm() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                Wiggle.define("null-arm", Order.class, f -> f
                        .thenFork(Arm.of("payment", a -> null),
                                  Arm.of("shipping", a -> a.thenApply(h::standardPath)))
                        .combine("merge", Order.class)));

        assertTrue(ex.getMessage().contains("payment"), ex.getMessage());
    }

    @Test
    void referencingTheSameHandlerTwiceIsRejectedAsADuplicateNode() {
        // node names address the graph, so they must be unique -- two references to one method are two
        // nodes with one name. The fix is a second handler method (renamed, or @Handles-renamed).
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
                Wiggle.define("dup", Order.class, f -> f
                        .thenApply(h::validate)
                        .thenApply(h::validate)));

        assertTrue(ex.getMessage().contains("duplicate step name"), ex.getMessage());
    }

    @Test
    void javaLoopsInADefinitionBodyUnrollIntoNodes() {
        // the body runs once at definition time, so ordinary control flow shapes the graph rather than
        // running in it -- worth pinning, because it is the most surprising consequence of the design.
        // Each iteration must reference a different handler, since one method binds one node (above).
        java.util.List<FlowFn<Order, Order>> steps = java.util.List.of(h::validate, h::vipPath, h::standardPath);

        var flow = Wiggle.define("unrolled", Order.class, f -> {
            WiggleFuture<Order> chain = f.thenApply(h::drain);
            for (FlowFn<Order, Order> step : steps) {
                chain = chain.thenApply(step);
            }
            return chain;
        });

        java.util.List<String> names = flow.definition().nodes().values().stream()
                .map(com.wiggle.core.Node::name).toList();
        assertTrue(names.containsAll(java.util.List.of("drain", "validate", "vipPath", "standardPath")),
                "the loop emitted one node per iteration: " + names);
    }

    @Test
    void aFutureHasNoGetOrJoin() throws Exception {
        // there is nothing to block on while a graph is being described; the only blocking handle is
        // the client-side one returned when an instance is started
        for (String blocking : java.util.List.of("get", "join", "getNow", "complete")) {
            assertThrows(NoSuchMethodException.class,
                    () -> WiggleFuture.class.getMethod(blocking),
                    "WiggleFuture must not expose " + blocking + "()");
        }
    }
}