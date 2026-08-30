package dev.wiggle.server.coord;

import java.util.List;
import java.util.Map;

/**
 * A namespace's placement policy: which cells host each epoch, and which epoch new roots use. The
 * {@code revision} is the compare-and-set token guarding reconfiguration writes (see
 * {@link CoordinatorStore#casPolicy}). This is control-plane state -- O(namespaces x epochs x cells),
 * never per-instance.
 */
public record CoordPolicy(String namespace, long currentEpoch, long revision, Map<Long, EpochRing> epochs) {

    /** A region-tagged slot of a placement ring: {@code shard -> cell}. */
    public record RingSlot(int shard, String cellId, String region) {}

    /** One epoch's ring plus its lifecycle status. */
    public record EpochRing(List<RingSlot> ring, EpochStatus status) {}

    public enum EpochStatus { OPEN, DRAINING, RETIRED }
}
