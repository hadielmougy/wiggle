package com.wiggle.placement;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The epoch lifecycle: the transitions as pure functions, and the operations against a store.
 *
 * <p>The transitions were previously reachable only by standing up a coordinator, so the properties
 * below — an old ring is never rewritten, statuses only go forwards, the current epoch is always
 * open — were things the code did rather than things anything checked.
 */
class EpochsTest {

    private static final long SEED = 20260916L;

    private static List<Ring.Slot> ring(String... cells) {
        List<Ring.Slot> out = new ArrayList<>();
        for (int i = 0; i < cells.length; i++) out.add(new Ring.Slot(i, cells[i], null));
        return out;
    }

    // ---------------------------------------------------------------- transitions

    @Test @DisplayName("opening a first epoch starts at 0 and is OPEN")
    void firstEpoch() {
        Ring.Policy p = Epochs.opening("acme", null, ring("cell-a"));
        assertEquals(0, p.currentEpoch());
        assertSame(Ring.Status.OPEN, p.current().status());
    }

    @Test @DisplayName("opening advances the epoch and drains the previous one, keeping its ring")
    void openingDrainsThePrevious() {
        Ring.Policy first = Epochs.opening("acme", null, ring("cell-a"));
        Ring.Policy second = Epochs.opening("acme", first, ring("cell-b"));

        assertEquals(1, second.currentEpoch());
        assertSame(Ring.Status.OPEN, second.current().status());

        Ring.Epoch drained = second.epochs().get(0L);
        assertSame(Ring.Status.DRAINING, drained.status());
        assertEquals(first.epochs().get(0L).ring(), drained.ring(),
                "a drained epoch keeps its ring -- ids minted into it still have to resolve");
    }

    @Test @DisplayName("an epoch's ring is never rewritten by any later opening, over a random history")
    void ringsAreImmutableAcrossAHistory() {
        Random rnd = new Random(SEED);
        for (int trial = 0; trial < 500; trial++) {
            Ring.Policy policy = null;
            Map<Long, List<Ring.Slot>> asPublished = new LinkedHashMap<>();

            int steps = 1 + rnd.nextInt(6);
            for (int i = 0; i < steps; i++) {
                List<Ring.Slot> next = ring("cell-" + (char) ('a' + rnd.nextInt(4)),
                        "cell-" + (char) ('a' + rnd.nextInt(4)));
                policy = Epochs.opening("acme", policy, next);
                asPublished.put(policy.currentEpoch(), policy.current().ring());
            }
            for (Map.Entry<Long, List<Ring.Slot>> e : asPublished.entrySet()) {
                assertEquals(e.getValue(), policy.epochs().get(e.getKey()).ring(),
                        "trial " + trial + " (seed " + SEED + "): epoch " + e.getKey()
                        + "'s ring changed after later epochs opened");
            }
        }
    }

    @Test @DisplayName("exactly one epoch is OPEN at any point in a history")
    void onlyOneOpenEpoch() {
        Random rnd = new Random(SEED);
        for (int trial = 0; trial < 500; trial++) {
            Ring.Policy policy = null;
            for (int i = 0; i < 1 + rnd.nextInt(6); i++) {
                policy = Epochs.opening("acme", policy, ring("cell-a", "cell-b"));
            }
            long open = policy.epochs().values().stream().filter(Ring.Epoch::mints).count();
            assertEquals(1, open, "trial " + trial + " (seed " + SEED + "): " + open + " open epochs");
            assertSame(Ring.Status.OPEN, policy.current().status(),
                    "and it is the one new ids are minted into");
        }
    }

    @Test @DisplayName("a status only goes forwards")
    void statusesOnlyAdvance() {
        Ring.Policy policy = Epochs.opening("acme", Epochs.opening("acme", null, ring("cell-a")),
                ring("cell-b"));                                    // epoch 0 DRAINING, 1 OPEN

        Ring.Policy retired = Epochs.withStatus(policy, 0, Ring.Status.RETIRED);
        assertSame(Ring.Status.RETIRED, retired.epochs().get(0L).status());

        IllegalArgumentException back = assertThrows(IllegalArgumentException.class,
                () -> Epochs.withStatus(retired, 0, Ring.Status.DRAINING));
        assertTrue(back.getMessage().contains("cannot go back"), back.getMessage());
    }

    @Test @DisplayName("the epoch new ids are minted into cannot be retired")
    void cannotRetireTheCurrentEpoch() {
        Ring.Policy policy = Epochs.opening("acme", null, ring("cell-a"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Epochs.withStatus(policy, 0, Ring.Status.RETIRED));
        assertTrue(e.getMessage().contains("open a new epoch"), e.getMessage());
    }

    @Test @DisplayName("an epoch must place something")
    void anEpochMustPlaceAShard() {
        assertThrows(IllegalArgumentException.class, () -> Epochs.opening("acme", null, List.of()));
    }

    // ---------------------------------------------------------------- operations against a store

    /** A store that loses the first n compare-and-sets, to exercise the retry. */
    static final class FlakyStore implements PlacementStore {
        private Ring.Policy policy;
        private long revision;
        private final AtomicInteger loseNext;
        final AtomicInteger attempts = new AtomicInteger();

        FlakyStore(int loseNext) { this.loseNext = new AtomicInteger(loseNext); }

        @Override public Optional<Versioned> get(String namespace) {
            return policy == null ? Optional.empty() : Optional.of(new Versioned(policy, revision));
        }

        @Override public boolean compareAndSet(String namespace, long expectedRevision, Ring.Policy updated) {
            attempts.incrementAndGet();
            if (loseNext.getAndDecrement() > 0) {
                // a lost CAS means somebody else wrote -- so actually write something, or the
                // caller's retry would re-read the same state and lose forever
                policy = Epochs.opening("acme", policy, List.of(new Ring.Slot(0, "other-cell", null)));
                revision++;
                return false;
            }
            if (expectedRevision != revision) return false;
            policy = updated;
            revision++;
            return true;
        }

    }

    @Test @DisplayName("openEpoch retries a lost compare-and-set and recomputes from the fresh read")
    void openEpochRetries() {
        FlakyStore store = new FlakyStore(2);            // two other writers get in first
        Ring.Policy written = Epochs.openEpoch(store, "acme", ring("cell-a"));

        assertEquals(3, store.attempts.get(), "two losses, then a win");
        assertEquals(2, written.currentEpoch(),
                "recomputed from the fresh read: epochs 0 and 1 were taken while we retried, so "
                + "ours is 2 -- a transition built from the stale first read would have written 0");
        assertSame(Ring.Status.OPEN, written.current().status());
        assertEquals("cell-a", written.current().ring().get(0).cellId(), "and it is OUR ring");
        assertSame(Ring.Status.DRAINING, written.epochs().get(1L).status(),
                "the epoch we raced is drained, not overwritten");
    }

    @Test @DisplayName("a store that stays contended fails loudly rather than writing something stale")
    void openEpochGivesUp() {
        FlakyStore store = new FlakyStore(Integer.MAX_VALUE);
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> Epochs.openEpoch(store, "acme", ring("cell-a")));
        assertTrue(e.getMessage().contains("stayed contended"), e.getMessage());
        assertEquals(Epochs.CAS_ATTEMPTS, store.attempts.get());
    }

    @Test @DisplayName("two openings through the store produce 0 OPEN then 0 DRAINING, 1 OPEN")
    void openTwiceThroughTheStore() {
        FlakyStore store = new FlakyStore(0);
        Epochs.openEpoch(store, "acme", ring("cell-a"));
        Ring.Policy second = Epochs.openEpoch(store, "acme", ring("cell-b"));

        assertEquals(1, second.currentEpoch());
        assertSame(Ring.Status.DRAINING, second.epochs().get(0L).status());
        assertSame(Ring.Status.OPEN, second.epochs().get(1L).status());
    }
}
