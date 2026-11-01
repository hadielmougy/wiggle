package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;

import java.util.Optional;

/** The locks taken through a token: its instance, and the token with it. Held on an instance shard. */
public interface TaskLocks extends InstanceRepository, TokenReads {

    /** {@code lockInstance} on the instance that owns token {@code tokenId}; empty when either the
     *  token or its instance is missing. */
    default Optional<Instance> lockInstanceOf(String tokenId) {
        return findToken(tokenId).flatMap(t -> lockInstance(t.instanceId));
    }

    /** {@code lockInstanceOf}, and token {@code tokenId} read under that lock; empty when either the
     *  token or its instance is missing. */
    default Optional<Rows.LockedTask> lockTask(String tokenId) {
        return lockInstanceOf(tokenId).flatMap(i -> findToken(tokenId).map(t -> new Rows.LockedTask(i, t)));
    }
}
