package com.wiggle.server.store;

/** Where a shard is in its life. It only ever moves forward. */
public enum ShardState {
    /** New instances may be minted on it, in proportion to its weight. */
    ACTIVE,
    /** Nothing new is minted on it; it keeps serving every id it holds. */
    DRAINING,
    /** Empty and decommissioned; an id naming it is not found. */
    RETIRED
}
