package com.wiggle.client.worker;

import java.time.Duration;
import java.util.Set;

/** Worker tuning. Defaults are chosen for interactive workloads, not throughput benchmarks. */
public record WorkerOptions(int concurrency, Duration lease, Duration longPollWait,
                            Duration idleBackoff, Duration errorBackoff, boolean registerOnStart,
                            int localBatchSize, Set<String> queues, Duration awaitRegistration) {

    public WorkerOptions {
        if (localBatchSize < 1) throw new IllegalArgumentException("localBatchSize must be >= 1");
        queues = queues == null ? Set.of() : Set.copyOf(queues);
        awaitRegistration = awaitRegistration == null ? Duration.ZERO : awaitRegistration;
    }

    public static WorkerOptions defaults() {
        return new WorkerOptions(Runtime.getRuntime().availableProcessors(),
                Duration.ofSeconds(30), Duration.ofSeconds(10),
                Duration.ofMillis(200), Duration.ofSeconds(2), true, 64, Set.of(), Duration.ZERO);
    }

    public WorkerOptions withConcurrency(int c) {
        return new WorkerOptions(c, lease, longPollWait, idleBackoff, errorBackoff, registerOnStart,
                localBatchSize, queues, awaitRegistration);
    }

    public WorkerOptions withLease(Duration d) {
        return new WorkerOptions(concurrency, d, longPollWait, idleBackoff, errorBackoff, registerOnStart,
                localBatchSize, queues, awaitRegistration);
    }

    public WorkerOptions withLongPollWait(Duration d) {
        return new WorkerOptions(concurrency, lease, d, idleBackoff, errorBackoff, registerOnStart,
                localBatchSize, queues, awaitRegistration);
    }

    /**
     * Max steps a LOCAL_ASYNC worker buffers before flushing to the server (default 64). Larger
     * batches mean fewer round-trips but a wider crash-replay window and bigger transactions.
     * Ignored by SERVER and LOCAL_SYNC (which flush every step).
     */
    public WorkerOptions withLocalBatchSize(int size) {
        return new WorkerOptions(concurrency, lease, longPollWait, idleBackoff, errorBackoff, registerOnStart,
                size, queues, awaitRegistration);
    }

    /**
     * Restricts this worker to the given queues (worker specialization). Empty -- the default --
     * means "serve every queue of the registered blueprints". A specialized worker never claims
     * steps routed elsewhere, and a local-execution chain hands back at a step it does not serve.
     */
    public WorkerOptions withQueues(String... only) {
        return new WorkerOptions(concurrency, lease, longPollWait, idleBackoff, errorBackoff, registerOnStart,
                localBatchSize, Set.of(only), awaitRegistration);
    }

    /**
     * How long {@link Worker#start()} waits for a {@link Worker#handle}-bound workflow's graph to
     * appear before failing, when the authoring client may still be registering it. Zero -- the
     * default -- fails fast: register the graph before starting the worker. Ignored when no handlers
     * are bound by name.
     */
    public WorkerOptions withAwaitRegistration(Duration d) {
        return new WorkerOptions(concurrency, lease, longPollWait, idleBackoff, errorBackoff, registerOnStart,
                localBatchSize, queues, d);
    }
}
