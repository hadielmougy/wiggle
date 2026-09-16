package com.wiggle.server.coord;

import com.wiggle.placement.Ring;

import java.util.Map;

/**
 * A namespace's placement policy as the store holds it: the shared {@link Ring} model plus the
 * compare-and-set token guarding reconfiguration writes (see {@link CoordinatorStore#casPolicy}).
 *
 * <p>The ring model itself lives in {@code :placement} so the server, the coordinator and the client
 * share one definition of what a ring means. What stays here is the part that is purely about
 * storage -- the revision -- which is why this record is thin.
 *
 * <p>Control-plane state: O(namespaces x epochs x cells), never per-instance.
 */
public record CoordPolicy(String namespace, long currentEpoch, long revision,
                          Map<Long, Ring.Epoch> epochs) {

    /** This policy as the pure model the placement rules take. */
    public Ring.Policy ring() {
        return new Ring.Policy(namespace, currentEpoch, epochs);
    }
}
