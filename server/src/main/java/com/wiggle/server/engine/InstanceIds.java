package com.wiggle.server.engine;

import com.wiggle.core.Ids;
import com.wiggle.core.ShardIds;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Mints instance ids. Every id carries the shard its instance lives on ({@link ShardIds}). */
public interface InstanceIds {

    /** A new root instance's id. */
    String next();

    /** The id the observed run of {@code workflow} keyed by {@code key} has; the same key always yields it. */
    String forKey(String workflow, String key);

    /** A sub-workflow's id, on the shard of the token that starts it. */
    default String childOf(String parentTokenId) {
        return ShardIds.inherit("wfi", parentTokenId);
    }

    /** The id {@link #forKey} gave before ids carried a shard, so a run reported across an upgrade
     *  stays one run. */
    default String legacyForKey(String workflow, String key) {
        return "wfo_" + Ids.digest(workflow + ":" + key);
    }

    /** Mints every root instance and observed run on {@code shard}. */
    static InstanceIds onShard(int shard) {
        return across(List.of(shard));
    }

    /**
     * Mints root instances on {@code shards} in turn, starting with the first, and every observed run
     * on the first of them -- an observed run's id is derived from its key, so it cannot take a turn.
     */
    static InstanceIds across(List<Integer> shards) {
        if (shards.isEmpty()) throw new IllegalArgumentException("no shard to mint on");
        List<Integer> pool = List.copyOf(shards);
        int observed = pool.getFirst();
        AtomicInteger turn = new AtomicInteger();
        return new InstanceIds() {
            @Override public String next() {
                return ShardIds.next("wfi", pool.get(Math.floorMod(turn.getAndIncrement(), pool.size())));
            }
            @Override public String forKey(String workflow, String key) {
                return ShardIds.format("wfo", observed, Ids.digest(workflow + ":" + key));
            }
        };
    }
}
