package com.wiggle.server.store;

/** How current a read must be. Chosen per call site, not per method. */
public enum Freshness {
    /** Read from the primary: sees every committed write. */
    PRIMARY,
    /** A replica may serve it, within the store's lag bound; a store without replicas reads the primary. */
    REPLICA_OK
}
