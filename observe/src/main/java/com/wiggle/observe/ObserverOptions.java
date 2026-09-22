package com.wiggle.observe;

import com.wiggle.core.Tls;

import java.lang.management.ManagementFactory;
import java.time.Duration;

/**
 * How an {@link Observer} reports. {@code reporter} names this process on every run it reports;
 * {@code batchSize} and {@code linger} bound how long a step waits in a run's buffer before it
 * is flushed (a run's end always flushes at once); {@code queueCapacity} bounds the reports
 * waiting on the flusher, beyond which they are dropped and counted rather than block the
 * application; {@code captureContext} ships each step's return value as the instance context,
 * off by default so an observed application never leaks its data by accident.
 */
public record ObserverOptions(String reporter, int batchSize, Duration linger, int queueCapacity,
                              boolean captureContext, Tls.Options tls, boolean requireTls) {

    public ObserverOptions {
        if (reporter == null || reporter.isBlank()) throw new IllegalArgumentException("reporter must be named");
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be >= 1");
        if (linger == null || linger.isNegative()) throw new IllegalArgumentException("linger must be >= 0");
        if (queueCapacity < 1) throw new IllegalArgumentException("queueCapacity must be >= 1");
        tls = tls == null ? Tls.Options.DISABLED : tls;
    }

    public static ObserverOptions defaults() {
        return new ObserverOptions(defaultReporter(), 64, Duration.ofSeconds(1), 10_000, false,
                Tls.Options.DISABLED, false);
    }

    public ObserverOptions withReporter(String r) {
        return new ObserverOptions(r, batchSize, linger, queueCapacity, captureContext, tls, requireTls);
    }

    public ObserverOptions withBatchSize(int n) {
        return new ObserverOptions(reporter, n, linger, queueCapacity, captureContext, tls, requireTls);
    }

    public ObserverOptions withLinger(Duration d) {
        return new ObserverOptions(reporter, batchSize, d, queueCapacity, captureContext, tls, requireTls);
    }

    public ObserverOptions withQueueCapacity(int n) {
        return new ObserverOptions(reporter, batchSize, linger, n, captureContext, tls, requireTls);
    }

    public ObserverOptions withCaptureContext(boolean on) {
        return new ObserverOptions(reporter, batchSize, linger, queueCapacity, on, tls, requireTls);
    }

    public ObserverOptions withTls(Tls.Options t) {
        return new ObserverOptions(reporter, batchSize, linger, queueCapacity, captureContext, t, requireTls);
    }

    public ObserverOptions withRequireTls(boolean on) {
        return new ObserverOptions(reporter, batchSize, linger, queueCapacity, captureContext, tls, on);
    }

    /** {@code host@pid}, which tells two reporting processes on one host apart. */
    static String defaultReporter() {
        String jvm = ManagementFactory.getRuntimeMXBean().getName();   // pid@host
        int at = jvm.indexOf('@');
        return at < 0 ? jvm : jvm.substring(at + 1) + "@" + jvm.substring(0, at);
    }
}
