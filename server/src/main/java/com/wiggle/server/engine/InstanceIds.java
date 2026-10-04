package com.wiggle.server.engine;

import com.wiggle.core.ShardIds;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** Mints instance ids. Every id carries the shard its instance lives on ({@link ShardIds}). */
public interface InstanceIds {

    /** A new root instance's id. */
    String next();

    /** A sub-workflow's id, on the shard of the token that starts it. */
    default String childOf(String parentTokenId) {
        return ShardIds.inherit("wfi", parentTokenId);
    }

    /** Mints every root instance on {@code shard}. */
    static InstanceIds onShard(int shard) {
        return across(List.of(shard));
    }

    /** Mints root instances on {@code shards} in turn, starting with the first. */
    static InstanceIds across(List<Integer> shards) {
        if (shards.isEmpty()) throw new IllegalArgumentException("no shard to mint on");
        List<Integer> pool = List.copyOf(shards);
        AtomicInteger turn = new AtomicInteger();
        return new InstanceIds() {
            @Override public String next() {
                return ShardIds.next("wfi", pool.get(Math.floorMod(turn.getAndIncrement(), pool.size())));
            }
        };
    }
}
