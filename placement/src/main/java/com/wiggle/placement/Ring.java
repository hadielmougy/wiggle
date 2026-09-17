package com.wiggle.placement;

import java.util.List;
import java.util.Map;

/**
 * A namespace's placement: which cell owns which shard, in which epoch.
 *
 * <p>A ring is published per epoch and never edited afterwards -- reshaping means opening a new
 * epoch, so an id minted long ago still resolves. The status moves {@code OPEN -> DRAINING ->
 * RETIRED} as work moves on; the mapping itself does not change.
 */
public final class Ring {

    private Ring() {}

    /** A region-tagged slot: {@code shard -> cell}. */
    public record Slot(int shard, String cellId, String region) {
        public Slot {
            if (cellId == null || cellId.isBlank()) {
                throw new IllegalArgumentException("a ring slot must name a cell (shard " + shard + ")");
            }
        }
    }

    /** One epoch's ring plus where it is in its lifecycle. */
    public record Epoch(List<Slot> ring, Status status) {
        public Epoch {
            ring = ring == null ? List.of() : List.copyOf(ring);
        }

        /** True while this epoch still mints new ids; a draining or retired one only serves. */
        public boolean mints() { return status == Status.OPEN; }
    }

    public enum Status {OPEN, DRAINING, RETIRED}

    /**
     * The whole placement for one namespace: every epoch's ring, and which one new ids are minted
     * into. Epochs are kept rather than replaced -- an id carries the epoch it was minted in.
     */
    public record Policy(String namespace, long currentEpoch, Map<Long, Epoch> epochs) {
        public Policy {
            epochs = epochs == null ? Map.of() : Map.copyOf(epochs);
        }

        /** The ring new ids are minted into, or null when the namespace has never been placed. */
        public Epoch current() { return epochs.get(currentEpoch); }

        public boolean isCurrentEpoch(long epoch) {
            return currentEpoch == epoch;
        }
    }
}
