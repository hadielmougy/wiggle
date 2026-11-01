package com.wiggle.server.store;

import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * The shard identity, replica-lag heartbeat and shard registry: the reads, which a read-only
 * connection such as a read replica can serve. Held on this database.
 */
public interface ShardReads {

    /** The shard this database was claimed for, or empty when no shard has claimed it. */
    OptionalInt shardIdentity();

    /** When this database's primary last wrote the replica-lag heartbeat (epoch millis), or empty when it
     *  never has. Read on a replica, its distance from now is the replica's lag. */
    OptionalLong shardBeat();

    /** Every shard the cluster has used, by id. Held on the home shard. */
    List<Rows.ShardRecord> shardRegistry();
}
