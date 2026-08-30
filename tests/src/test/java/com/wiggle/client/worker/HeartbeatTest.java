package com.wiggle.client.worker;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * White-box test (same package) for the lease-extension guarantee: once {@link Heartbeat#stop()}
 * returns, no extension is ever sent -- which is what lets the worker settle a task without a
 * heartbeat racing or trailing the complete/fail RPC.
 */
class HeartbeatTest {

    @Test
    void noExtensionIsSentAfterStop() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        AtomicBoolean stopped = new AtomicBoolean(false);
        AtomicInteger sends = new AtomicInteger();
        AtomicReference<String> violation = new AtomicReference<>();

        Heartbeat.Sender sender = extend -> {
            if (stopped.get()) violation.compareAndSet(null, "extend() was called after stop() returned");
            sends.incrementAndGet();
            // Widen the beat so stop() is likely to land while a beat is in flight.
            try { Thread.sleep(2); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        };

        Heartbeat hb = new Heartbeat(scheduler, sender, 3 /* ms lease -> 1ms period */, "task-1");
        hb.start();
        try {
            // Prove the beat is live before we test that it stops.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (sends.get() < 3 && System.nanoTime() < deadline) Thread.sleep(1);
            assertTrue(sends.get() >= 3, "heartbeat should beat while the task runs");

            hb.stop();
            stopped.set(true);          // after stop() returns, any extend() is a violation
            int atStop = sends.get();

            Thread.sleep(100);          // many beat periods; nothing may fire
            assertEquals(atStop, sends.get(), "no extension may be sent after stop()");
            assertNull(violation.get(), violation.get());

            hb.stop();                  // idempotent
            Thread.sleep(20);
            assertEquals(atStop, sends.get(), "still nothing after a second stop()");
        } finally {
            scheduler.shutdownNow();
        }
    }
}
