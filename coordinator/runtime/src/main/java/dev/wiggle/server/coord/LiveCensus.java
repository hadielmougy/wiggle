package dev.wiggle.server.coord;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The coordinator's rolling view of live instances per epoch, fed by node heartbeats and read by the
 * reconciler to retire drained epochs (R21). Each node reports its cell's live-by-epoch counts; because
 * the nodes of one cell share a database they all report the same counts, so aggregation takes the
 * <em>max</em> within a cell and <em>sums</em> across cells (whose databases are disjoint).
 *
 * <p>Reports carry a timestamp; {@link #aggregate} ignores stale ones, so a cell whose nodes have all
 * gone silent contributes nothing -- and its epochs are never retired without a fresh zero, which is
 * the safe direction (never retire an epoch that might still hold work).
 */
public final class LiveCensus {

    private record Entry(String namespace, String cellId, Map<Long, Long> byEpoch, long ts) {}

    private final Map<String, Entry> byNode = new ConcurrentHashMap<>();

    /** Record a node's latest per-epoch live counts. */
    public void record(String nodeId, String namespace, String cellId, Map<Long, Long> byEpoch, long nowMillis) {
        byNode.put(nodeId, new Entry(namespace, cellId, Map.copyOf(byEpoch), nowMillis));
    }

    /** Drop a node's report (on deregister, or when it is reaped). */
    public void forget(String nodeId) {
        byNode.remove(nodeId);
    }

    /** Drop reports older than {@code deadline}, bounding memory as nodes come and (ungracefully) go. */
    public void prune(long deadline) {
        byNode.values().removeIf(e -> e.ts() < deadline);
    }

    /**
     * Aggregated live counts per epoch for {@code namespace}, using only reports at or after
     * {@code freshSince}. {@link Aggregate#hasFresh()} is false when no cell reported recently -- the
     * caller must then decline to retire, since it has no confirmation of zero.
     */
    public Aggregate aggregate(String namespace, long freshSince) {
        Map<String, Map<Long, Long>> perCell = new HashMap<>();
        boolean hasFresh = false;
        for (Entry e : byNode.values()) {
            if (!namespace.equals(e.namespace()) || e.ts() < freshSince) continue;
            hasFresh = true;
            Map<Long, Long> cell = perCell.computeIfAbsent(e.cellId(), k -> new HashMap<>());
            for (Map.Entry<Long, Long> en : e.byEpoch().entrySet()) cell.merge(en.getKey(), en.getValue(), Math::max);
        }
        Map<Long, Long> total = new HashMap<>();
        for (Map<Long, Long> cell : perCell.values()) {
            for (Map.Entry<Long, Long> en : cell.entrySet()) total.merge(en.getKey(), en.getValue(), Long::sum);
        }
        return new Aggregate(hasFresh, total);
    }

    /** The namespace-wide live counts, and whether any recent report backed them. */
    public record Aggregate(boolean hasFresh, Map<Long, Long> liveByEpoch) {
        public long count(long epoch) { return liveByEpoch.getOrDefault(epoch, 0L); }
    }
}
