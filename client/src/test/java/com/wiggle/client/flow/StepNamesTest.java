package com.wiggle.client.flow;

import com.wiggle.client.flow.Fixtures.Fulfilment;
import com.wiggle.client.flow.Fixtures.Label;
import com.wiggle.client.flow.Fixtures.Order;
import com.wiggle.client.flow.Fixtures.OrderHandlers;
import com.wiggle.client.flow.Fixtures.Payment;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The bridge between a typed method reference and the name-keyed graph: reading the referenced
 * method's name off the reference, and refusing the two shapes that have no name a worker could bind.
 */
class StepNamesTest {

    private final OrderHandlers h = new OrderHandlers();

    @Test
    void aMethodReferenceYieldsTheReferencedMethodsName() {
        assertEquals("validate", StepNames.of((FlowFn<Order, Order>) h::validate));
        assertEquals("inStock", StepNames.of((FlowGate<Order>) h::inStock));
        assertEquals("notifyCustomer", StepNames.of((FlowEffect<Fulfilment>) h::notifyCustomer));
        assertEquals("settle", StepNames.of((FlowFn2<Payment, Label, Fulfilment>) h::settle));
    }

    @Test
    void handlesAnnotationOnTheReferencedMethodWins() {
        // the graph node is what the worker binds by, so @Handles has to reach the topology too
        assertEquals("capture-payment", StepNames.of((FlowFn<Order, Payment>) h::doCapture));
    }

    @Test
    void aLambdaIsRejectedBecauseItHasNoHandlerMethodToBind() {
        FlowFn<Order, Order> lambda = o -> o;

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> StepNames.of(lambda));
        assertTrue(ex.getMessage().contains("direct method reference"), ex.getMessage());
    }

    @Test
    void aLambdaCapturingALocalIsRejectedRatherThanSilentlyDroppingTheCapturedValue() {
        // the trap this API exists to close: a captured value is not part of the topology, so it
        // would not survive registration, replay or a restart
        int discount = 10;
        FlowFn<Order, Order> capturing = o -> new Order(o.id(), o.quantity() - discount, o.lines());

        assertThrows(IllegalArgumentException.class, () -> StepNames.of(capturing));
    }

    @Test
    void aStaticMethodReferenceIsAValidStepName() {
        // a static method reference captures nothing; it is still a perfectly good step name
        assertEquals("passThrough", StepNames.of((FlowFn<Order, Order>) StepNamesTest::passThrough));
    }

    static Order passThrough(Order o) {
        return o;
    }

    @Test
    void parameterCountReadsJvmDescriptors() {
        assertEquals(0, StepNames.parameterCount("()V"));
        assertEquals(1, StepNames.parameterCount("(Lcom/wiggle/Order;)Lcom/wiggle/Payment;"));
        assertEquals(3, StepNames.parameterCount("(Lcom/wiggle/Order;I[Ljava/lang/String;)Z"));
        assertEquals(2, StepNames.parameterCount("([[JLjava/util/List;)V"));
    }
}