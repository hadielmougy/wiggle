package dev.wiggle.server.grpc;

import dev.wiggle.server.ServerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The memory-pressure admission decision + the in-flight byte accounting, driven with a fake heap reading. */
class MemoryGuardTest {

    private static ServerConfig.Memory cfg(boolean enabled, double threshold, double rejectRatio) {
        return new ServerConfig.Memory(enabled, threshold, rejectRatio, Duration.ofSeconds(2), Duration.ofSeconds(1));
    }

    @Test @DisplayName("under pressure only above the threshold")
    void underPressureOverThreshold() {
        double[] util = {0.50};
        MemoryGuard g = new MemoryGuard(cfg(true, 0.90, 1.0), () -> util[0]);
        assertFalse(g.underPressure(), "well under threshold");
        assertFalse(g.rejectPoll(), "not under pressure -> never reject");
        util[0] = 0.95;
        assertTrue(g.underPressure(), "over threshold");
        assertTrue(g.rejectPoll(), "ratio 1.0 rejects every poll under pressure");
        util[0] = 0.20;
        assertFalse(g.underPressure(), "recovers when utilization drops back");
    }

    @Test @DisplayName("disabled never rejects, even at 99% heap")
    void disabledNeverRejects() {
        MemoryGuard g = new MemoryGuard(cfg(false, 0.50, 1.0), () -> 0.99);
        assertFalse(g.underPressure());
        assertFalse(g.rejectPoll());
    }

    @Test @DisplayName("rejects roughly the configured fraction of polls while under pressure")
    void rejectRatioIsHonored() {
        MemoryGuard g = new MemoryGuard(cfg(true, 0.90, 0.30), () -> 0.95);   // always over threshold
        int n = 40_000, rejected = 0;
        for (int i = 0; i < n; i++) if (g.rejectPoll()) rejected++;
        double fraction = (double) rejected / n;
        assertTrue(fraction > 0.27 && fraction < 0.33, "rejected ~30%, got " + fraction);

        MemoryGuard none = new MemoryGuard(cfg(true, 0.90, 0.0), () -> 0.95);
        for (int i = 0; i < 1000; i++) assertFalse(none.rejectPoll(), "ratio 0 rejects nothing");

        MemoryGuard all = new MemoryGuard(cfg(true, 0.90, 1.0), () -> 0.95);
        for (int i = 0; i < 1000; i++) assertTrue(all.rejectPoll(), "ratio 1 rejects everything under pressure");
    }

    @Test @DisplayName("in-flight bytes are summed and released")
    void inFlightAccounting() {
        MemoryGuard g = new MemoryGuard(cfg(true, 0.90, 0.10), () -> 0.0);
        assertEquals(0, g.inFlightBytes());
        g.add(1000);
        g.add(500);
        assertEquals(1500, g.inFlightBytes(), "request + response bytes accumulate");
        g.release(1000);
        assertEquals(500, g.inFlightBytes(), "released on call end");
        g.add(-5);       // negatives ignored
        g.release(-5);
        assertEquals(500, g.inFlightBytes());
    }

    @Test @DisplayName("the retry hold-off is the interval plus jitter within bounds")
    void retryJitter() {
        MemoryGuard g = new MemoryGuard(cfg(true, 0.90, 0.10), () -> 0.0);
        for (int i = 0; i < 200; i++) {
            long r = g.retryAfterMillis();
            assertTrue(r >= 2000 && r <= 3000, "retryAfter in [interval, interval+jitter]: " + r);
        }
    }
}
