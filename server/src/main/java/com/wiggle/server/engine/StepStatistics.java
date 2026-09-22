package com.wiggle.server.engine;

import com.wiggle.core.NodeStats;
import com.wiggle.server.store.Rows.StepDuration;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Per-node duration statistics over a sample of settled steps, slowest 95th percentile first. */
final class StepStatistics {

    private StepStatistics() {}

    static List<NodeStats> summarise(List<StepDuration> sample, Function<String, String> nameOf) {
        Map<String, List<Long>> byNode = new LinkedHashMap<>();
        for (StepDuration d : sample) byNode.computeIfAbsent(d.nodeId(), k -> new ArrayList<>()).add(d.millis());
        List<NodeStats> out = new ArrayList<>(byNode.size());
        byNode.forEach((nodeId, millis) -> {
            millis.sort(null);
            long sum = 0;
            for (long m : millis) sum += m;
            out.add(new NodeStats(nodeId, nameOf.apply(nodeId), millis.size(), (double) sum / millis.size(),
                    percentile(millis, 50), percentile(millis, 95), millis.getLast()));
        });
        out.sort(Comparator.comparingLong(NodeStats::p95Millis).reversed());
        return out;
    }

    /** Nearest-rank percentile over an ascending list. */
    private static long percentile(List<Long> sorted, int pct) {
        int rank = (int) Math.ceil(pct / 100.0 * sorted.size());
        return sorted.get(Math.max(0, Math.min(sorted.size() - 1, rank - 1)));
    }
}
