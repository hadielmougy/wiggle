package dev.wiggle.server.cluster;

import dev.wiggle.server.ServerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The load-shedding state machine: enter when the backlog is high and growing, recover when it drains. */
class StabilityControllerTest {

    private static ServerConfig.Stability config() {
        return new ServerConfig.Stability(true, /*threads*/ 8, /*high*/ 100, /*low*/ 50,
                Duration.ofMillis(1), Duration.ofSeconds(2), Duration.ofSeconds(1));
    }

    @Test @DisplayName("sheds when the backlog is high and growing, recovers when it drains")
    void transitions() {
        AtomicInteger depth = new AtomicInteger(0);
        try (StabilityController c = new StabilityController(depth::get, config())) {
            c.tick();                       // depth 0
            assertFalse(c.shedding(), "empty backlog does not shed");

            depth.set(80); c.tick();        // below high water mark
            assertFalse(c.shedding(), "below the high-water mark does not shed");

            depth.set(120); c.tick();       // crosses high AND grew (80 -> 120)
            assertTrue(c.shedding(), "high and growing sheds");

            depth.set(140); c.tick();       // still climbing
            assertTrue(c.shedding(), "stays shedding while still high");

            depth.set(130); c.tick();       // shrinking -> recovering
            assertFalse(c.shedding(), "a shrinking backlog recovers");

            depth.set(200); c.tick();       // grows again -> shed
            assertTrue(c.shedding());

            depth.set(40); c.tick();        // below low-water mark -> recover
            assertFalse(c.shedding(), "below the low-water mark recovers");
        }
    }

    @Test @DisplayName("the hold-off is the configured base plus jitter within bounds")
    void holdOffJitter() {
        try (StabilityController c = new StabilityController(() -> 0, config())) {
            for (int i = 0; i < 200; i++) {
                long r = c.retryAfterMillis();
                assertTrue(r >= 2000 && r <= 3000, "retryAfter in [base, base+jitter]: " + r);
            }
        }
    }

    @Test @DisplayName("disabled never sheds")
    void disabled() {
        try (StabilityController c = new StabilityController(() -> 1_000_000, ServerConfig.Stability.DISABLED)) {
            c.start();                      // no-op scheduler
            c.tick();                       // even a huge backlog is ignored when disabled
            assertFalse(c.shedding(), "disabled controller never sheds");
        }
    }
}
