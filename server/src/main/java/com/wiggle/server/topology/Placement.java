package com.wiggle.server.topology;

import com.wiggle.core.Ids;
import com.wiggle.core.ShardIds;
import com.wiggle.server.engine.InstanceIds;
import com.wiggle.server.store.ShardState;
import com.wiggle.server.topology.Topology.Generation;
import com.wiggle.server.topology.Topology.Role;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Mints instance ids on a {@link Topology}: each root instance on a shard of the generation that
 * applies at that moment, in proportion to the shards' weights. The pick is smooth weighted
 * round-robin, so over any run of picks every shard gets its share to within one, with no randomness.
 */
public final class Placement implements InstanceIds {

    private final Topology topology;
    private final LongSupplier clock;
    private final int observedShard;
    private Generation generation;
    private List<int[]> wheel;   // {shard, weight, current}

    public Placement(Topology topology, LongSupplier clock) {
        this.topology = topology;
        this.clock = clock;
        this.observedShard = topology.shards().stream()
                .filter(s -> s.has(Role.INSTANCES) && s.state() == ShardState.ACTIVE)
                .findFirst().orElseThrow().id();
    }

    /** The shard the next root instance goes to. */
    public synchronized int nextShard() {
        Generation g = topology.generationAt(clock.getAsLong());
        if (g != generation) {
            generation = g;
            wheel = new ArrayList<>();
            for (Map.Entry<Integer, Integer> w : g.weights().entrySet()) {
                if (w.getValue() > 0) wheel.add(new int[]{w.getKey(), w.getValue(), 0});
            }
        }
        int total = 0;
        int[] best = null;
        for (int[] slot : wheel) {
            slot[2] += slot[1];
            total += slot[1];
            if (best == null || slot[2] > best[2]) best = slot;
        }
        best[2] -= total;
        return best[0];
    }

    @Override public String next() {
        return ShardIds.next("wfi", nextShard());
    }

    /** Every observed run goes to the first ACTIVE instance shard the document lists: its id is derived
     *  from its key, so it cannot take a weighted turn. */
    @Override public String forKey(String workflow, String key) {
        return ShardIds.format("wfo", observedShard, Ids.digest(workflow + ":" + key));
    }
}
