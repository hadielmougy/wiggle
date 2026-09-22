package com.wiggle.server.engine;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** Drift as a pure function: durations newest first, a verdict on the medians. */
class DegradationTest {

    private static List<Long> newestFirst(long recent, int window, long before, int baseline) {
        List<Long> out = new ArrayList<>();
        for (int i = 0; i < window; i++) out.add(recent);
        for (int i = 0; i < baseline; i++) out.add(before);
        return out;
    }

    @Test @DisplayName("a step twice as slow as before, on the median, is drifting")
    void driftDetected() {
        Degradation.Drift d = Degradation.detect(newestFirst(200, 20, 90, 40), 20, 30, 2.0, 5).orElseThrow();
        assertEquals(200, d.recentP50());
        assertEquals(90, d.baselineP50());
        assertEquals(20, d.recent());
        assertEquals(40, d.baseline());
        assertTrue(d.factor() > 2.2 && d.factor() < 2.23);
    }

    @Test @DisplayName("one slow run cannot tip it: the median holds")
    void outlierIgnored() {
        List<Long> sample = newestFirst(90, 20, 90, 40);
        sample.set(0, 5000L);
        assertEquals(Optional.empty(), Degradation.detect(sample, 20, 30, 2.0, 5));
    }

    @Test @DisplayName("too little history, too small a gain, or too small a ratio: no finding")
    void thresholds() {
        assertEquals(Optional.empty(), Degradation.detect(newestFirst(200, 20, 90, 10), 20, 30, 2.0, 5), "baseline too short");
        assertEquals(Optional.empty(), Degradation.detect(newestFirst(3, 20, 1, 40), 20, 30, 2.0, 5), "1 ms -> 3 ms is noise");
        assertEquals(Optional.empty(), Degradation.detect(newestFirst(150, 20, 90, 40), 20, 30, 2.0, 5), "1.7x is under 2x");
        assertTrue(Degradation.detect(newestFirst(150, 20, 90, 40), 20, 30, 1.5, 5).isPresent(), "unless the ratio says so");
    }
}
