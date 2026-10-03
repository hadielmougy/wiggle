package com.wiggle.server.engine;

import com.wiggle.core.Ids;
import com.wiggle.core.ShardIds;

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
        return new InstanceIds() {
            @Override public String next() { return ShardIds.next("wfi", shard); }
            @Override public String forKey(String workflow, String key) {
                return ShardIds.format("wfo", shard, Ids.digest(workflow + ":" + key));
            }
        };
    }
}
