package dev.wiggle.client.worker;

import java.time.Duration;

/** Worker tuning. Defaults are chosen for interactive workloads, not throughput benchmarks. */
public record WorkerOptions(int concurrency, Duration lease, Duration longPollWait,
                            Duration idleBackoff, Duration errorBackoff, boolean registerOnStart) {

    public static WorkerOptions defaults() {
        return new WorkerOptions(Runtime.getRuntime().availableProcessors(),
                Duration.ofSeconds(30), Duration.ofSeconds(10),
                Duration.ofMillis(200), Duration.ofSeconds(2), true);
    }

    public WorkerOptions withConcurrency(int c) {
        return new WorkerOptions(c, lease, longPollWait, idleBackoff, errorBackoff, registerOnStart);
    }

    public WorkerOptions withLease(Duration d) {
        return new WorkerOptions(concurrency, d, longPollWait, idleBackoff, errorBackoff, registerOnStart);
    }

    public WorkerOptions withLongPollWait(Duration d) {
        return new WorkerOptions(concurrency, lease, d, idleBackoff, errorBackoff, registerOnStart);
    }
}
