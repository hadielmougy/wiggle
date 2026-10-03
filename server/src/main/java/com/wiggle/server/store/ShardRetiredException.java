package com.wiggle.server.store;

/** An id names a shard that has been retired: whatever it named is gone, so the caller is told "not found". */
public final class ShardRetiredException extends StorageException {

    private static final long serialVersionUID = 1L;

    public ShardRetiredException(int shard) {
        super("shard " + shard + " is retired; nothing on it can be found", null, Classification.PERMANENT);
    }
}
