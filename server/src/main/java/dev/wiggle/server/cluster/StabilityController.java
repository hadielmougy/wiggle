package dev.wiggle.server.cluster;

import dev.wiggle.server.ServerConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import java.util.random.RandomGenerator;

/**
 * Per-node load-shedding for worker polls, driven by the gRPC handler thread-pool queue. It samples
 * the queue depth (requests waiting for a handler thread) on an interval and flips a cached
 * {@code shedding} flag when it is high <em>and growing</em> -- the sign that the server is saturated
 * and about to accumulate parked long-poll requests it can't service. While shedding, the poll path
 * returns immediately with a jittered {@link #retryAfterMillis()} instead of parking, and already
 * parked long-polls exit early, so the queue drains rather than growing until clients block forever
 * or memory runs out. The flag clears once the queue stops growing or drops below the low-water mark.
 *
 * <p>The signal is entirely server-local (the thread-pool queue), so shedding never depends on -- and
 * can never be defeated by -- storage state. The hot poll path only reads the cached flag.
 */
public final class StabilityController implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(StabilityController.class.getName());

    private final IntSupplier queueDepth;
    private final ServerConfig.Stability config;
    private final RandomGenerator jitter = RandomGenerator.getDefault();
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-stability");
                t.setDaemon(true);
                return t;
            });

    private volatile boolean shedding;
    private int lastDepth;

    /** {@code queueDepth} supplies the current thread-pool queue size (or any saturation signal). */
    public StabilityController(IntSupplier queueDepth, ServerConfig.Stability config) {
        this.queueDepth = queueDepth;
        this.config = config;
    }

    public void start() {
        if (!config.enabled()) return;
        long period = Math.max(100, config.checkInterval().toMillis());
        scheduler.scheduleAtFixedRate(this::tick, period, period, TimeUnit.MILLISECONDS);
        LOG.log(System.Logger.Level.INFO, () -> "stability load-shedding enabled: shed when the request "
                + "thread-pool queue >= " + config.highWatermark() + " and growing, recover below "
                + config.lowWatermark());
    }

    /** One sample + state transition. Package-private so tests can drive it deterministically. */
    void tick() {
        if (!config.enabled()) return;
        int depth;
        try {
            depth = queueDepth.getAsInt();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "stability sample failed: " + e);
            return;   // a transient hiccup must not kill the sampler
        }
        boolean growing = depth > lastDepth;
        if (!shedding) {
            if (depth >= config.highWatermark() && growing) {
                shedding = true;
                int d = depth;
                LOG.log(System.Logger.Level.WARNING, () -> "request queue growing (" + d
                        + " >= " + config.highWatermark() + "): shedding worker polls to protect the server");
            }
        } else if (depth <= config.lowWatermark() || depth < lastDepth) {
            shedding = false;
            int d = depth;
            LOG.log(System.Logger.Level.INFO, () -> "request queue recovering (" + d
                    + "): accepting worker polls again");
        }
        lastDepth = depth;
    }

    /** Whether the poll path should shed right now (cheap, cached). */
    public boolean shedding() { return shedding; }

    /** How long a worker should hold off before polling again: the configured base plus per-call jitter. */
    public long retryAfterMillis() {
        long base = config.holdOff().toMillis();
        long spread = config.holdOffJitter().toMillis();
        return base + (spread <= 0 ? 0 : jitter.nextLong(spread + 1));
    }

    @Override public void close() { scheduler.shutdownNow(); }
}
