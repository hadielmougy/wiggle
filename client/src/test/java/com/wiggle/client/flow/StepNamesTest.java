package com.wiggle.client.flow;

import com.wiggle.client.flow.Fixtures.Fulfilment;
import com.wiggle.client.flow.Fixtures.Label;
import com.wiggle.client.flow.Fixtures.Order;
import com.wiggle.client.flow.Fixtures.Steps;
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

    /** The contract the specs name their steps through -- inert, never invoked. */
    private final Steps h = Steps.class.cast(java.lang.reflect.Proxy.newProxyInstance(
            Steps.class.getClassLoader(), new Class<?>[] {Steps.class},
            (p, m, a) -> { throw new IllegalStateException(m.getName()); }));

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
    void aStepReferencedOnAnImplementationIsRejected() {
        // the confusion this closes: a spec never runs a step, it names one -- so a reference to a
        // concrete method promises code the spec will never call, and goes quietly wrong when the
        // worker binds some other object
        final class Impl implements Fixtures.Steps {
            public Order validate(Order o) { return o; }
            public boolean inStock(Order o) { return true; }
            public Payment charge(Order o) { return null; }
            public Order reserve(Order o) { return o; }
            public Label label(Order o) { return null; }
            public Fulfilment settle(Payment p, Label l) { return null; }
            public Fulfilment settleWithBase(Order b, Payment p, Label l) { return null; }
            public Fulfilment audit(Payment p, Label l, Fixtures.Shipment s) { return null; }
            public Fulfilment settlePositionally(Payment p, Label l) { return null; }
            public Fulfilment unannotatedBase(Order b, Payment p, Label l) { return null; }
            public void notifyCustomer(Fulfilment f) { }
            public boolean isVip(Order o) { return true; }
            public Order vipPath(Order o) { return o; }
            public Order standardPath(Order o) { return o; }
            public boolean hasMore(Order o) { return true; }
            public Order drain(Order o) { return o; }
            public Fixtures.Line price(Fixtures.Line l) { return l; }
            public Order total(java.util.List<Fixtures.Line> p) { return null; }
            public Order totalWithBase(Order b, java.util.List<Fixtures.Line> p) { return b; }
            public Fulfilment settleFive(Order b, Payment a, Label c, Fixtures.Shipment d, Order e,
                                         Fixtures.Line g) { return null; }
            public Payment armA(Order o) { return null; }
            public Label armB(Order o) { return null; }
            public Fixtures.Shipment armC(Order o) { return null; }
            public Order armD(Order o) { return o; }
            public Fixtures.Line armE(Order o) { return null; }
            public Payment doCapture(Order o) { return null; }
        }
        Impl impl = new Impl();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> StepNames.of((FlowFn<Order, Order>) impl::validate));
        assertTrue(ex.getMessage().contains("which is a class"), ex.getMessage());
        assertTrue(ex.getMessage().contains("FlowSpec.define"), "and shows the interface form: " + ex.getMessage());
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