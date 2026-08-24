package dev.wiggle.server.grpc;

import dev.wiggle.server.ServerConfig;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;

/**
 * Memory-pressure admission control for the gRPC server. A {@link MemorySizeInterceptor} keeps this
 * guard's running sum of the serialized bytes held by every in-flight request+response cycle; the
 * guard turns the JVM's GC-accurate heap utilization into a per-poll admit/reject decision. When
 * utilization crosses the configured threshold the server is under pressure and rejects a
 * {@code rejectRatio} fraction of new polls (each returned empty with a jittered hold-off), easing
 * load gently instead of shedding everything at once; it recovers on its own once utilization falls
 * back below the threshold.
 */
public final class MemoryGuard {

    private final boolean enabled;
    private final double threshold;
    private final double rejectRatio;
    private final long retryMs;
    private final long jitterMs;
    private final DoubleSupplier utilization;
    private final AtomicLong inFlightBytes = new AtomicLong();

    public MemoryGuard(ServerConfig.Memory config) {
        this(config, MemoryGuard::heapUtilization);
    }

    /** Test seam: supply the heap-utilization reading (live / max, in [0,1]) directly. */
    MemoryGuard(ServerConfig.Memory config, DoubleSupplier utilization) {
        this.enabled = config.enabled();
        this.threshold = config.threshold();
        this.rejectRatio = config.rejectRatio();
        this.retryMs = config.retryInterval().toMillis();
        this.jitterMs = config.retryJitter().toMillis();
        this.utilization = utilization;
    }

    /**
     * GC-accurate heap utilization: the live set over the heap max. It sums each heap pool's
     * <em>collection usage</em> -- the memory still in use right after the JVM last recycled that
     * pool -- so it excludes not-yet-collected garbage and the guard never sheds on memory a GC
     * would reclaim. Falls back to the instantaneous heap usage on collectors that don't report
     * collection usage (which is only slightly conservative -- it includes garbage).
     */
    static double heapUtilization() {
        long liveUsed = 0;
        boolean reported = false;
        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() != MemoryType.HEAP) continue;
            MemoryUsage afterGc = pool.getCollectionUsage();   // usage after this pool was last collected
            if (afterGc == null) continue;                     // this collector doesn't track it
            liveUsed += afterGc.getUsed();
            reported = true;
        }
        long max = Runtime.getRuntime().maxMemory();
        if (reported && max > 0) return (double) liveUsed / max;
        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long m = heap.getMax();
        return m <= 0 ? 0.0 : (double) heap.getUsed() / m;
    }

    public boolean enabled() { return enabled; }

    /** Add/subtract the bytes an in-flight request or response message is holding. */
    public void add(long bytes) { if (bytes > 0) inFlightBytes.addAndGet(bytes); }
    public void release(long bytes) { if (bytes > 0) inFlightBytes.addAndGet(-bytes); }

    /** The summed serialized size of everything currently in flight. */
    public long inFlightBytes() { return inFlightBytes.get(); }

    /** Whether the server is over its memory threshold (so admission control is active). */
    public boolean underPressure() { return enabled && utilization.getAsDouble() > threshold; }

    /**
     * The per-poll admission decision: under memory pressure, reject a {@code rejectRatio} fraction
     * of polls (each call is an independent random draw). Returns {@code false} when not enabled or
     * not under pressure -- then every poll is served normally.
     */
    public boolean rejectPoll() {
        return underPressure() && ThreadLocalRandom.current().nextDouble() < rejectRatio;
    }

    /** How long to tell a rejected worker to wait before polling again: the retry interval plus jitter. */
    public long retryAfterMillis() {
        return retryMs + (jitterMs <= 0 ? 0 : ThreadLocalRandom.current().nextLong(jitterMs + 1));
    }
}
