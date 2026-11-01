package com.wiggle.client;

import io.github.shield.Interceptor;
import io.github.shield.Shield;
import io.github.shield.ShieldedSupplier;
import io.github.shield.internal.RetriesExhaustedException;

import java.util.function.Supplier;

/**
 * Transient-failure retry for {@link WiggleClient}'s calls. Retries only a call that was unavailable —
 * "the server isn't reachable" (down, restarting, or an active/passive failover in flight), or it
 * answered 502/503 — where the call almost certainly never executed, so retrying is safe even for
 * non-idempotent operations. Permanent errors (404, 400, …) and a timed-out call (which may have run
 * server-side) are NOT retried. Tunable per JVM: {@code -Dwiggle.rpc.maxAttempts} / {@code WIGGLE_RPC_MAX_ATTEMPTS}
 * (default 5; 1 disables retry) and {@code -Dwiggle.rpc.retryDelayMillis} /
 * {@code WIGGLE_RPC_RETRY_DELAY_MILLIS} (default 200, exponential backoff).
 */
final class RpcRetry {

    private RpcRetry() { }

    static int maxAttempts() { return intConfig("wiggle.rpc.maxAttempts", "WIGGLE_RPC_MAX_ATTEMPTS", 5); }

    private static long delayMillis() { return intConfig("wiggle.rpc.retryDelayMillis", "WIGGLE_RPC_RETRY_DELAY_MILLIS", 200); }

    /**
     * Runs {@code op}, retrying while it is unavailable. Returns its value, or throws: the original
     * {@link HttpTransport.RpcException} for a permanent error, or {@link RetriesExhaustedException}
     * once the transient retries are used up (see {@link #lastFailure}).
     */
    static <T> T retrying(Supplier<T> op) {
        int attempts = maxAttempts();
        if (attempts <= 1) return op.get();   // retry disabled -> the original fast path
        ShieldedSupplier<T> shielded = Shield.decorate(() -> {
                    try {
                        return op.get();
                    } catch (HttpTransport.RpcException e) {
                        if (e.unavailable) throw new TransientRpc(e);
                        throw e;               // permanent -> propagate, not retried
                    }
                })
                .with(Interceptor.retry()
                        .maxRetries(attempts)
                        .delayMillis(delayMillis())
                        .backOff()
                        .onException(TransientRpc.class))
                .build();
        try {
            return shielded.get();
        } finally {
            shielded.close();
        }
    }

    /** The last unavailable failure from an exhausted retry, or null if there is none. */
    static HttpTransport.RpcException lastFailure(RetriesExhaustedException e) {
        return e.getCause() instanceof TransientRpc t ? t.failure : null;
    }

    /** Marks an unavailable call as retryable, carrying the original failure for the exhausted case. */
    private static final class TransientRpc extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final transient HttpTransport.RpcException failure;
        TransientRpc(HttpTransport.RpcException f) { super(f); this.failure = f; }
    }

    private static int intConfig(String prop, String env, int def) {
        String v = System.getProperty(prop);
        if (v == null || v.isBlank()) v = System.getenv(env);
        if (v == null || v.isBlank()) return def;
        try { return Integer.parseInt(v.trim()); } catch (NumberFormatException e) { return def; }
    }
}
