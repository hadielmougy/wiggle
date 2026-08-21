package dev.wiggle.core;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Exponential backoff with optional jitter. {@code maxAttempts} counts the first
 * try, so {@code maxAttempts == 1} means "no retry".
 */
public record RetryPolicy(int maxAttempts, long initialBackoffMillis, double multiplier,
                          long maxBackoffMillis, double jitter) {

    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (multiplier < 1.0) throw new IllegalArgumentException("multiplier must be >= 1.0");
    }

    public static RetryPolicy forever() {
        return new RetryPolicy(Integer.MAX_VALUE, Duration.ofSeconds(1).toMillis(), 1.0, Duration.ofMinutes(1).toMillis(), 0);
    }

    public static RetryPolicy none() {
        return new RetryPolicy(1, 0, 1.0, 0, 0);
    }

    public static RetryPolicy exponential(int maxAttempts, Duration initialBackoff) {
        return new RetryPolicy(maxAttempts, initialBackoff.toMillis(), 2.0,
                Duration.ofMinutes(5).toMillis(), 0.2);
    }

    public static RetryPolicy fixed(int maxAttempts, Duration backoff) {
        return new RetryPolicy(maxAttempts, backoff.toMillis(), 1.0, backoff.toMillis(), 0);
    }

    /** Delay before attempt {@code attempt + 1}, where {@code attempt} is the number of failures so far. */
    public long backoffMillis(int attempt) {
        if (initialBackoffMillis <= 0) return 0;
        double d = initialBackoffMillis * Math.pow(multiplier, Math.max(0, attempt - 1));
        long capped = (long) Math.min(d, maxBackoffMillis <= 0 ? d : maxBackoffMillis);
        if (jitter > 0) {
            double f = 1 - jitter + (Math.random() * 2 * jitter);
            capped = (long) (capped * f);
        }
        return Math.max(0, capped);
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("maxAttempts", (long) maxAttempts);
        m.put("initialBackoffMillis", initialBackoffMillis);
        m.put("multiplier", multiplier);
        m.put("maxBackoffMillis", maxBackoffMillis);
        m.put("jitter", jitter);
        return m;
    }

    public static RetryPolicy fromJson(Object o) {
        if (o == null) return null;
        Map<String, Object> m = Json.asObject(o);
        return new RetryPolicy(
                (int) Json.num(m, "maxAttempts", 1),
                Json.num(m, "initialBackoffMillis", 0),
                Json.dbl(m, "multiplier", 1.0),
                Json.num(m, "maxBackoffMillis", 0),
                Json.dbl(m, "jitter", 0));
    }
}
