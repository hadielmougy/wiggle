package com.wiggle.server.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Detects a step getting slower over time: its recent runs against the runs before them, on the
 * median, which a single slow run cannot move. Pure: a list of durations newest first, and a
 * verdict.
 */
final class Degradation {

    private Degradation() {}

    /** A step whose recent median is {@code factor} times its earlier one. */
    record Drift(long recentP50, long baselineP50, double factor, int recent, int baseline) {}

    /**
     * @param newestFirst   the step's durations, newest first
     * @param window        how many recent runs form "now"
     * @param baselineMin   how many earlier runs "before" needs before a verdict is worth anything
     * @param ratio         how many times slower the recent median must be
     * @param minGainMillis and by how much at least, so a 1 ms step becoming 2 ms is not a finding
     */
    static Optional<Drift> detect(List<Long> newestFirst, int window, int baselineMin, double ratio, long minGainMillis) {
        if (newestFirst.size() < window + baselineMin) return Optional.empty();
        List<Long> recent = new ArrayList<>(newestFirst.subList(0, window));
        List<Long> baseline = new ArrayList<>(newestFirst.subList(window, newestFirst.size()));
        long recentP50 = median(recent);
        long baselineP50 = median(baseline);
        if (recentP50 - baselineP50 < minGainMillis) return Optional.empty();
        if (baselineP50 > 0 && (double) recentP50 / baselineP50 < ratio) return Optional.empty();
        double factor = baselineP50 == 0 ? Double.POSITIVE_INFINITY : (double) recentP50 / baselineP50;
        return Optional.of(new Drift(recentP50, baselineP50, factor, recent.size(), baseline.size()));
    }

    private static long median(List<Long> values) {
        values.sort(null);
        int n = values.size();
        return n % 2 == 1 ? values.get(n / 2) : (values.get(n / 2 - 1) + values.get(n / 2)) / 2;
    }
}
