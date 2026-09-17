package com.wiggle.sandbox;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.DirectConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.*;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

public class WorkerMain {


    interface OrderSteps {                       // the steps, as a contract
        Order   validate(Order o);
        boolean inStock(Order o);
        Order   authorise(Order o);
        Order   capture(Order o);
        CompensableActivity<Order, Order> reserveStock();
        Order   printLabel(Order o);
        Order   merge(@Context Order base, Order payment, Order shipping);
        CompensableActivity<Order, Order>   notify0();
    }

    record Order() {}

    public static void main(String[] args) throws InterruptedException {
        DirectConnection conn = WiggleConnection.direct("127.0.0.1:18100");
        var client = conn.client();
        FlowSpec spec = FlowSpec.define("test-flow",RetryPolicy.fixed(5, Duration.ofSeconds(1)),Order.class, OrderSteps.class, (f, s) -> {
            var checked = f.apply(s::validate).thenFilter(s::inStock);
            var payment  = checked.thenApply(s::authorise, RetryPolicy.exponential(5, Duration.ofMillis(100)))
                    .thenApply(s::capture);
            var shipping = checked.thenApplyCompensable(s::reserveStock)
                    .thenApply(s::printLabel);
            return Wiggle.allOf(payment, shipping)
                    .combineWithContext(s::merge)    // mandatory — there is no implicit join
                    .thenApplyCompensable(s::notify0);
            });
            client.register(spec);
            client.start(spec, new Order());

            Worker worker = new Worker(client, "123").registerHandler("test-flow",new OrderStepsImpl());
            worker.start();

            Runtime.getRuntime().addShutdownHook(new Thread(worker::close));
            Thread.currentThread().join();
    }

    @ForFlow("test-flow")
    static class OrderStepsImpl implements WorkerMain.OrderSteps {
        @Override
        public WorkerMain.Order validate(WorkerMain.Order o) {
            System.out.println("validate");
            return o;
        }

        @Override
        public boolean inStock(WorkerMain.Order o) {
            System.out.println("inStock");
            return true;
        }

        @Override
        public WorkerMain.Order authorise(WorkerMain.Order o) {
            System.out.println("authorise");
            return o;
        }

        @Override
        public WorkerMain.Order capture(WorkerMain.Order o) {
            System.out.println("capture");
            return o;
        }

        @Override
        public CompensableActivity<Order, Order> reserveStock() {
            // Printed once at registerHandler() time, before any step runs: the binder invokes each
            // activity factory once to inventory what it returns. The step itself is execute().
            System.out.println("reserveStock (factory)");
            return new CompensableActivity<>() {
                @Override
                public Order execute(Order ctx) {
                    System.out.println("reserveStock attempt " + Step.attempt());
                    if (Step.attempt() <=2 )
                        throw new RuntimeException("invalid attempt");
                    return ctx;
                }

                @Override
                public void compensate(Compensation<Order, Order> comp) {
                    System.out.println("compensate reserveStock");

                }
            };
        }

        @Override
        public WorkerMain.Order printLabel(WorkerMain.Order o) {
            System.out.println("printLabel");
            return o;
        }

        @Override
        public WorkerMain.Order merge(@Context WorkerMain.Order base, WorkerMain.Order payment, WorkerMain.Order shipping) {
            System.out.println("merge");
            return base;
        }

        @Override
        public CompensableActivity<Order, Order> notify0() {
            return new CompensableActivity<>() {

                @Override
                public void compensate(Compensation<Order, Order> comp) {
                    System.out.println("compensate notify");
                }

                @Override
                public Order execute(Order ctx) {
                    throw new RuntimeException("not implemented");
                }
            };
        }
    }
}
