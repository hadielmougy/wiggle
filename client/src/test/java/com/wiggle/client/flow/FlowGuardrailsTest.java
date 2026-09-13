package com.wiggle.client.flow;

import com.wiggle.client.flow.Fixtures.Order;
import com.wiggle.client.flow.Fixtures.OrderHandlers;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rules that keep a future-shaped definition honest about the model underneath it. Continuing one
 * future twice is legal -- it is how {@link Wiggle#allOf} gets its arms -- so the checks that matter
 * are the ones that catch a split nobody rejoined, and a combine that does not match the fan-out it
 * merges.
 */
class FlowGuardrailsTest {

    private final OrderHandlers h = new OrderHandlers();

    @Test
    void aSplitThatIsNeverCombinedIsRejectedAndNamesBothEnds() {
        // branches run on isolated copies of the context, so there is no meaning to a split that
        // never rejoins -- the engine would have two successors and no barrier
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            Wiggle.define("dangling", Order.class, f -> {
                var validated = f.thenApply(h::validate);
                validated.thenApply(h::vipPath);                  // one continuation...
                return validated.thenApply(h::standardPath);      // ...and a second, never combined
            });
        });

        assertTrue(ex.getMessage().contains("never rejoined"), ex.getMessage());
        assertTrue(ex.getMessage().contains("vipPath") && ex.getMessage().contains("standardPath"),
                "the error must name the two ends that dangle: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("allOf"),
                "and the construct that fixes it: " + ex.getMessage());
    }

    @Test
    void aFanOutWithNoCombineIsRejected() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            Wiggle.define("no-combine", Order.class, f -> {
                var payment = f.thenApply(h::charge).named("payment");
                var shipping = f.thenApply(h::label).named("shipping");
                Wiggle.allOf(payment, shipping);       // stage dropped on the floor
                return payment;
            });
        });

        assertTrue(ex.getMessage().contains("combine"), ex.getMessage());
    }

    @Test
    void armsMustFanOutFromOneCommonPoint() {
        // a future belonging to another definition has no junction with this one
        WiggleFuture<?>[] alien = new WiggleFuture<?>[1];
        Wiggle.define("other-flow", Order.class, g -> alien[0] = g.thenApply(h::label).named("alien"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            Wiggle.define("two-flows", Order.class, f -> {
                var payment = f.thenApply(h::charge).named("payment");
                return Wiggle.allOf(payment, alien[0]).combine("merge", Order.class);
            });
        });

        assertTrue(ex.getMessage().contains("common point"), ex.getMessage());
    }

    @Test
    void armsMustHaveDistinctNames() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            Wiggle.define("same-name", Order.class, f -> {
                var a = f.thenApply(h::charge).named("arm");
                var b = f.thenApply(h::label).named("arm");
                return Wiggle.allOf(a, b).combine("merge", Order.class);
            });
        });

        assertTrue(ex.getMessage().contains("distinct names"), ex.getMessage());
    }

    @Test
    void aBodyThatReturnsNullIsRejectedWithTheReason() {
        IllegalStateException ex = assertThrows(IllegalStateException.class, () ->
                Wiggle.define("null-body", Order.class, f -> null));

        assertTrue(ex.getMessage().contains("must return the future it ends on"), ex.getMessage());
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
    void aFutureHasNoGetOrJoin() {
        // there is nothing to block on while a graph is being described; the only blocking handle is
        // the client-side one returned when an instance is started
        for (String blocking : java.util.List.of("get", "join", "getNow", "complete")) {
            assertThrows(NoSuchMethodException.class,
                    () -> WiggleFuture.class.getMethod(blocking),
                    "WiggleFuture must not expose " + blocking + "()");
        }
    }
}
