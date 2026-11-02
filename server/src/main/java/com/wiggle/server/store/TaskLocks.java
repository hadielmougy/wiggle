package com.wiggle.server.store;

import com.wiggle.server.store.Rows.Instance;

import java.util.Optional;

/** The locks taken through a token: its instance, and the token with it. Held on an instance shard. */
public interface TaskLocks {

    /** {@code lockInstance} on the instance that owns token {@code tokenId}; empty when either the
     *  token or its instance is missing. */
    Optional<Instance> lockInstanceOf(String tokenId);

    /** {@code lockInstanceOf}, and token {@code tokenId} read under that lock; empty when either the
     *  token or its instance is missing. */
    Optional<Rows.LockedTask> lockTask(String tokenId);
}
