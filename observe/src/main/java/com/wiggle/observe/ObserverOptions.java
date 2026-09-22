package com.wiggle.observe;

import com.wiggle.core.Tls;

import java.lang.management.ManagementFactory;
import java.time.Duration;

/**
 * How an {@link Observer} reports. {@code reporter} names this process on every report;
 * {@code linger} bounds how long a report waits to travel with others; {@code queueCapacity}
 * bounds the reports waiting on the flusher, beyond which they are dropped and counted rather
 * than block the application.
 */
public record ObserverOptions(String reporter, Duration linger, int queueCapacity, Tls.Options tls, boolean requireTls) {

    public ObserverOptions {
        if (reporter == null || reporter.isBlank()) throw new IllegalArgumentException("reporter must be named");
        if (linger == null || linger.isNegative()) throw new IllegalArgumentException("linger must be >= 0");
        if (queueCapacity < 1) throw new IllegalArgumentException("queueCapacity must be >= 1");
        tls = tls == null ? Tls.Options.DISABLED : tls;
    }

    public static ObserverOptions defaults() {
        return new ObserverOptions(defaultReporter(), Duration.ofMillis(200), 10_000, Tls.Options.DISABLED, false);
    }

    public ObserverOptions withReporter(String r) {
        return new ObserverOptions(r, linger, queueCapacity, tls, requireTls);
    }

    public ObserverOptions withLinger(Duration d) {
        return new ObserverOptions(reporter, d, queueCapacity, tls, requireTls);
    }

    public ObserverOptions withQueueCapacity(int n) {
        return new ObserverOptions(reporter, linger, n, tls, requireTls);
    }

    public ObserverOptions withTls(Tls.Options t) {
        return new ObserverOptions(reporter, linger, queueCapacity, t, requireTls);
    }

    public ObserverOptions withRequireTls(boolean on) {
        return new ObserverOptions(reporter, linger, queueCapacity, tls, on);
    }

    /** {@code host@pid}, which tells two reporting processes on one host apart. */
    static String defaultReporter() {
        String jvm = ManagementFactory.getRuntimeMXBean().getName();   // pid@host
        int at = jvm.indexOf('@');
        return at < 0 ? jvm : jvm.substring(at + 1) + "@" + jvm.substring(0, at);
    }
}
