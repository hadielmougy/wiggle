package dev.wiggle.client.worker;

import java.time.Duration;

/** Worker tuning. Defaults are chosen for interactive workloads, not throughput benchmarks. */
public record WorkerOptions(int concurrency, Duration lease, Duration longPollWait,
                            Duration idleBackoff, Duration errorBackoff, boolean registerOnStart,
                            int localBatchSize) {

    public WorkerOptions {
        if (localBatchSize < 1) throw new IllegalArgumentException("localBatchSize must be >= 1");
    }

    public static WorkerOptions defaults() {
        return new WorkerOptions(Runtime.getRuntime().availableProcessors(),
                Duration.ofSeconds(30), Duration.ofSeconds(10),
                Duration.ofMillis(200), Duration.ofSeconds(2), true, 64);
    }

    public WorkerOptions withConcurrency(int c) {
        return new WorkerOptions(c, lease, longPollWait, idleBackoff, errorBackoff, registerOnStart, localBatchSize);
    }

    public WorkerOptions withLease(Duration d) {
        return new WorkerOptions(concurrency, d, longPollWait, idleBackoff, errorBackoff, registerOnStart, localBatchSize);
    }

    public WorkerOptions withLongPollWait(Duration d) {
        return new WorkerOptions(concurrency, lease, d, idleBackoff, errorBackoff, registerOnStart, localBatchSize);
    }

    /**
     * Max steps a LOCAL_ASYNC worker buffers before flushing to the server (default 64). Larger
     * batches mean fewer round-trips but a wider crash-replay window and bigger transactions.
     * Ignored by SERVER and LOCAL_SYNC (which flush every step).
     */
    public WorkerOptions withLocalBatchSize(int size) {
        return new WorkerOptions(concurrency, lease, longPollWait, idleBackoff, errorBackoff, registerOnStart, size);
    }
}
