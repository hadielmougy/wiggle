package com.wiggle.server.coord;

import com.wiggle.placement.PlacementStore;
import com.wiggle.placement.Ring;

import java.util.Optional;

/**
 * The coordinator's {@link CoordinatorStore} seen through the placement module's SPI.
 *
 * <p>An adapter rather than a second store: {@code CoordinatorStore} is broad -- nodes, namespaces,
 * workflow allocations, policies -- and the placement rules need two methods of it. Narrowing here
 * means {@code Epochs} cannot reach anything else, and it keeps the module free of the coordinator's
 * own vocabulary.
 */
final class CoordinatorPlacementStore implements PlacementStore {

    private final CoordinatorStore store;

    CoordinatorPlacementStore(CoordinatorStore store) {
        this.store = store;
    }

    @Override public Optional<Versioned> get(String namespace) {
        return store.getPolicy(namespace)
                .map(p -> new Versioned(p.ring(), p.revision()));
    }

    @Override public boolean compareAndSet(String namespace, long expectedRevision, Ring.Policy updated) {
        // casPolicy takes the revision it expects and returns rows written; the stored revision is
        // assigned by the store, so the value passed here is not read back
        return store.casPolicy(namespace, expectedRevision,
                new CoordPolicy(updated.namespace(), updated.currentEpoch(), 0, updated.epochs())) > 0;
    }
}
