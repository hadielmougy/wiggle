package com.wiggle.order;

import com.wiggle.client.worker.ForFlow;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Step logic for the {@code bench-linear} throughput workflow built by {@link Benchmark}. The chain's
 * many intermediate steps all canonicalise to {@link #hop} (an identity pass-through), while the final
 * {@code sink} step counts the instance down so the driver can time the drain.
 */
@ForFlow("bench-linear")
public final class BenchHandlers implements Benchmark.BenchSteps {

    private final CountDownLatch done;

    public BenchHandlers(CountDownLatch done) {
        this.done = done;
    }

    /** Every intermediate step: pass the context straight through. */
    private Map<String, Object> hop(Map<String, Object> ctx) {
        return ctx;
    }

    // One method per declared hop. They are identical on purpose: the benchmark measures dispatch,
    // not step work, and a step is named by the method it references -- so a chain of N nodes needs
    // N names. Implementing Benchmark.BenchSteps is what keeps these in step with the topology.
    @Override public Map<String, Object> hop1(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop2(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop3(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop4(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop5(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop6(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop7(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop8(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop9(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop10(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop11(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop12(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop13(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop14(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop15(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop16(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop17(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop18(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop19(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop20(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop21(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop22(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop23(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop24(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop25(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop26(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop27(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop28(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop29(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop30(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop31(Map<String, Object> ctx) { return hop(ctx); }

    @Override public Map<String, Object> hop32(Map<String, Object> ctx) { return hop(ctx); }

    /** The final step: record that this instance finished. */
    @Override public Map<String, Object> sink(Map<String, Object> ctx) {
        done.countDown();
        return ctx;
    }
}
