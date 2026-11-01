package com.wiggle.server.store;

/**
 * The shard identity, replica-lag heartbeat and shard registry: the writes, and the reads that
 * must see the primary. Held on this database.
 */
public interface ShardRepository extends ShardReads {

    /** Claims this database for {@code shardId} when nothing has claimed it; leaves an existing claim
     *  alone, so the caller compares {@link #shardIdentity} afterwards. */
    void claimShardIdentity(int shardId);

    /** Stamps the replica-lag heartbeat with {@code now}. Needs a claimed shard identity. */
    void writeShardBeat(long now);

    /** Writes a shard's registry row, replacing any it had. */
    void putShardRecord(Rows.ShardRecord record);
}
