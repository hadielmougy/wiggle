package com.wiggle.server.engine;

import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.ClaimFilter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the long poll's fallback wait: a plain poll waits the configured interval; a
 * narrowed poll doubles its wait on every unsignaled empty claim up to its cap and carries it to the
 * worker's next poll until work is found; the opt-in adaptive ramp starts fast and a signal jumps it
 * to the interval.
 */
class FallbackPollPolicyTest {

    private static final ClaimFilter PLAIN = ClaimFilter.byName(Set.of("orders"), null);
    private static final ClaimFilter SCOPED =
            ClaimFilter.byName(Set.of("orders"), Set.of(new WorkflowVersion("w", 1)));

    private static FallbackPollPolicy policy(PollerRegistry pollers, boolean adaptive) {
        return new FallbackPollPolicy(pollers, 100, 800, adaptive);
    }

    private static FallbackPollPolicy policy(boolean adaptive) {
        return policy(new PollerRegistry(60_000), adaptive);
    }

    @Test @DisplayName("a plain poll waits the interval, signaled or not")
    void plainPollWaitsTheInterval() {
        FallbackPollPolicy.Schedule s = policy(false).schedule("w1", PLAIN);
        assertEquals(100, s.waitMillis());
        s.afterClaim(false);
        assertEquals(100, s.waitMillis());
        s.afterClaim(true);
        assertEquals(100, s.waitMillis());
    }

    @Test @DisplayName("a narrowed poll doubles its wait on unsignaled claims, up to its cap; a signal leaves it")
    void scopedWaitDoublesToCap() {
        FallbackPollPolicy.Schedule s = policy(false).schedule("w1", SCOPED);
        assertEquals(100, s.waitMillis());
        long[] expected = {200, 400, 800, 800};
        for (long e : expected) {
            s.afterClaim(false);
            assertEquals(e, s.waitMillis());
        }
        s.afterClaim(true);
        assertEquals(800, s.waitMillis(), "a same-node wake says nothing about cross-node work");
    }

    @Test @DisplayName("the adaptive ramp starts at a quarter of the interval and a signal jumps it to the interval")
    void adaptiveRampsAndSignalJumps() {
        FallbackPollPolicy p = policy(true);
        FallbackPollPolicy.Schedule s = p.schedule("w1", PLAIN);
        assertEquals(25, s.waitMillis());
        s.afterClaim(false);
        assertEquals(50, s.waitMillis());
        s.afterClaim(false);
        s.afterClaim(false);
        assertEquals(100, s.waitMillis(), "capped at the interval");

        s = p.schedule("w1", PLAIN);
        s.afterClaim(true);
        assertEquals(100, s.waitMillis(), "a signal jumps straight to the interval");

        assertEquals(10, new FallbackPollPolicy(new PollerRegistry(60_000), 20, 800, true)
                .schedule("w1", PLAIN).waitMillis(), "never starts below 10ms");
        assertEquals(100, p.schedule("w1", SCOPED).waitMillis(), "a narrowed poll does not ramp");
    }

    @Test @DisplayName("a narrowed poll's wait carries to the worker's next poll until one finds work")
    void scopedWaitCarriesOverUntilWorkIsFound() {
        FallbackPollPolicy p = policy(false);
        FallbackPollPolicy.Schedule s = p.schedule("w1", SCOPED);
        s.afterClaim(false);
        s.afterClaim(false);
        s.finish(false);
        assertEquals(400, p.schedule("w1", SCOPED).waitMillis(), "carried over");
        assertEquals(100, p.schedule("w2", SCOPED).waitMillis(), "per worker");
        assertEquals(100, p.schedule("w1", PLAIN).waitMillis(), "only for a narrowed poll");

        p.schedule("w1", SCOPED).finish(true);
        assertEquals(100, p.schedule("w1", SCOPED).waitMillis(), "found work: starts over");
    }

    @Test @DisplayName("past 1,024 remembered workers, only those still polling are kept")
    void prunesWorkersThatStoppedPolling() {
        PollerRegistry pollers = new PollerRegistry(60_000);
        pollers.seen("live", SCOPED, System.currentTimeMillis());
        FallbackPollPolicy p = policy(pollers, false);
        for (int i = -1; i < 1_024; i++) {
            FallbackPollPolicy.Schedule s = p.schedule(i < 0 ? "live" : "gone-" + i, SCOPED);
            s.afterClaim(false);
            s.finish(false);
        }
        assertEquals(200, p.schedule("live", SCOPED).waitMillis(), "a polling worker keeps its wait");
        assertEquals(100, p.schedule("gone-0", SCOPED).waitMillis(), "one that stopped is forgotten");
    }
}
