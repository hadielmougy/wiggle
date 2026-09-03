package com.wiggle.order;

import com.wiggle.client.worker.Handlers;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Step logic for the {@code bench-linear} throughput workflow built by {@link Benchmark}. The chain's
 * many intermediate steps all canonicalise to {@link #hop} (an identity pass-through), while the final
 * {@code sink} step counts the instance down so the driver can time the drain.
 */
@Handlers("bench-linear")
public final class BenchHandlers {

    private final CountDownLatch done;

    public BenchHandlers(CountDownLatch done) {
        this.done = done;
    }

    /** Every intermediate step: pass the context straight through. */
    public Map<String, Object> hop(Map<String, Object> ctx) {
        return ctx;
    }

    /** The final step: record that this instance finished. */
    public Map<String, Object> sink(Map<String, Object> ctx) {
        done.countDown();
        return ctx;
    }
}
