package com.wiggle.server.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class SweeperTest {

    private static final List<Integer> ITEMS = IntStream.range(0, 40).boxed().toList();

    @Test @DisplayName("a parallelism of 1 runs items in list order and counts the ones answered true")
    void serialRunsInOrder() {
        List<Integer> seen = new ArrayList<>();
        int done = new Sweeper(1).run(ITEMS, i -> "item " + i, i -> {
            seen.add(i);
            return i % 2 == 0;
        });
        assertEquals(ITEMS, seen);
        assertEquals(20, done);
    }

    @Test @DisplayName("items run concurrently, never more than the parallelism at once")
    void parallelIsBounded() {
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        List<Integer> seen = Collections.synchronizedList(new ArrayList<>());
        int done = new Sweeper(4).run(ITEMS, i -> "item " + i, i -> {
            peak.accumulateAndGet(running.incrementAndGet(), Math::max);
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            running.decrementAndGet();
            seen.add(i);
            return true;
        });
        assertEquals(40, done, "every item ran before run() returned");
        assertEquals(40, seen.size());
        assertTrue(peak.get() > 1, "items overlapped: peak " + peak.get());
        assertTrue(peak.get() <= 4, "never above the bound: peak " + peak.get());
    }

    @Test @DisplayName("an item that throws counts as not done and leaves the others running")
    void failureIsIsolated() {
        for (int parallelism : new int[]{1, 4}) {
            AtomicInteger ran = new AtomicInteger();
            int done = new Sweeper(parallelism).run(ITEMS, i -> "item " + i, i -> {
                ran.incrementAndGet();
                if (i % 10 == 0) throw new IllegalStateException("boom " + i);
                return true;
            });
            assertEquals(40, ran.get(), "parallelism " + parallelism + ": every item was attempted");
            assertEquals(36, done, "parallelism " + parallelism + ": the four that threw are not counted");
        }
    }
}
