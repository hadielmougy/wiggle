package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.Decode;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.RecordMapper;

import java.util.Map;

/**
 * The code in {@code docs/versioning.md}. See {@link SagaSnippet} for why these live as source.
 *
 * <p>Two axes get confused constantly, so the fixture keeps them apart: the workflow's version is a
 * hash of its <em>topology</em>, while the context is a record whose shape evolves separately and is
 * not versioned by the engine at all. {@code VersioningTest} runs both.
 */
public final class VersioningSnippet {

    public record Order(String id, String status, String currency) {}

    // docs:begin contract-v1
    interface OrderSteps {
        Order validate(Order o);
        Order charge(Order o);
    }
    // docs:end contract-v1

    interface OrderStepsV2 extends OrderSteps {
        Order fraudCheck(Order o);
    }

    public static FlowSpec v1() {
        // docs:begin topology-v1
        FlowSpec v1 = FlowSpec.define("orders", Order.class, OrderSteps.class, (f, s) -> f
                .thenApply(s::validate)
                .thenApply(s::charge));
        // docs:end topology-v1
        return v1;
    }

    public static FlowSpec v2() {
        // docs:begin topology-v2
        FlowSpec v2 = FlowSpec.define("orders", Order.class, OrderStepsV2.class, (f, s) -> f
                .thenApply(s::validate)
                .thenApply(s::fraudCheck)      // a new step -- so a new content hash, so a new version
                .thenApply(s::charge));
        // docs:end topology-v2
        return v2;
    }

    static void startPinned(WiggleClient client, FlowSpec spec, Map<String, Object> ctx) {
        // docs:begin start
        client.start("orders", ctx);                          // latest registered version
        client.start("orders", ctx, spec.version(), "corr-1"); // pinned: immune to a mid-deploy change
        // docs:end start
    }

    static void scopedWorkers(WiggleClient client, FlowSpec v1, FlowSpec v2) {
        // docs:begin scoped-workers
        new Worker(client, "service-a").registerHandler(new V1Handlers(), v1.version());  // only v1
        new Worker(client, "service-b").registerHandler(new V2Handlers(), v2.version());  // only v2
        // docs:end scoped-workers
    }

    @ForFlow("orders")
    public static class V1Handlers implements OrderSteps {
        @Override public Order validate(Order o) { return new Order(o.id(), "VALIDATED", o.currency()); }
        @Override public Order charge(Order o) { return new Order(o.id(), "CHARGED", o.currency()); }
    }

    @ForFlow("orders")
    public static class V2Handlers implements OrderStepsV2 {
        @Override public Order validate(Order o) { return new Order(o.id(), "VALIDATED", o.currency()); }
        @Override public Order fraudCheck(Order o) { return new Order(o.id(), "CHECKED", o.currency()); }
        @Override public Order charge(Order o) { return new Order(o.id(), "CHARGED", o.currency()); }
    }

    // docs:begin decode
    @ForFlow("orders")
    static class UpcastingHandlers implements OrderSteps {

        /** Runs instead of the reflective mapping wherever an Order parameter is bound. */
        @Decode
        public Order load(Map<String, Object> raw) {
            raw.putIfAbsent("currency", "USD");   // a field added after these instances started
            return (Order) RecordMapper.fromJson(raw, Order.class);
        }

        @Override public Order validate(Order o) { return new Order(o.id(), "VALIDATED", o.currency()); }
        @Override public Order charge(Order o) { return new Order(o.id(), "CHARGED", o.currency()); }
    }
    // docs:end decode

    private VersioningSnippet() {}
}
