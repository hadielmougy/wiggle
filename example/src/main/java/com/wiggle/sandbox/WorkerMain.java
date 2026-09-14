package com.wiggle.sandbox;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

public class WorkerMain {


    interface OrderSteps {                       // the steps, as a contract
        Order   validate(Order o);
        boolean inStock(Order o);
        Order   authorise(Order o);
        Order   capture(Order o);
        Order   reserveStock(Order o);
        Order   printLabel(Order o);
        Order   merge(@Context Order base, Order payment, Order shipping);
        Order   notify(Order o);
    }

    record Order() {}

    public static void main(String[] args) throws InterruptedException {
        CoordinatedConnection conn = WiggleConnection.coordinator("127.0.0.1:18099");
        var client = conn.clientForNamespace("abc");
        FlowSpec spec = FlowSpec.define("test-flow",Order.class, OrderSteps.class, (f, s) -> {
            var checked = f.thenApply(s::validate).thenFilter(s::inStock);
            var payment  = checked.thenApply(s::authorise, RetryPolicy.exponential(5, Duration.ofMillis(100)))
                    .thenApply(s::capture);
            var shipping = checked.thenApply(s::reserveStock)
                    .thenApply(s::printLabel);
            return Wiggle.allOf(payment, shipping)
                    .combineWithContext(s::merge)    // mandatory — there is no implicit join
                    .thenApply(s::notify);
            });
            client.register(spec);
            client.start(spec, new Order());


            Worker worker = new Worker(client, "123").registerHandler(new OrderStepsImpl());
            worker.start();

            Runtime.getRuntime().addShutdownHook(new Thread(worker::close));

            // Every thread the worker starts is a daemon, so main has to park or the JVM exits.
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
        public WorkerMain.Order reserveStock(WorkerMain.Order o) {
            System.out.println("reserveStock");
            return o;
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
        public WorkerMain.Order notify(WorkerMain.Order o) {
            System.out.println("notify");
            return o;
        }
    }
}
