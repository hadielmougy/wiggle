package com.wiggle.election;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The election rule, which both the cell engine and the control plane now run.
 *
 * <p>The rule is a pure function of the roster, so most of this needs no clock, no threads and no
 * store: feed it rosters and check who wins. That is the property that makes the scheme work at all
 * -- every process evaluates the same function over the same table and reaches the same answer
 * without talking to any other process.
 */
class LeaderElectionTest {

    private static final long DEAD_AFTER = 5_000;

    private static Member m(String id, long first, long last) {
        return new Member(id, id, first, last);
    }

    @Test @DisplayName("the longest-running live member wins")
    void seniorityWins() {
        List<Member> roster = List.of(m("young", 2_000, 9_000), m("old", 1_000, 9_000));
        assertEquals("old", LeaderElection.electedLeader(roster, 9_500, DEAD_AFTER));
    }

    @Test @DisplayName("ties break by id, so every process computes the same answer")
    void tiesBreakByIdDeterministically() {
        List<Member> a = List.of(m("b", 1_000, 9_000), m("a", 1_000, 9_000));
        List<Member> reversed = new ArrayList<>(a);
        java.util.Collections.reverse(reversed);
        assertEquals("a", LeaderElection.electedLeader(a, 9_500, DEAD_AFTER));
        assertEquals("a", LeaderElection.electedLeader(reversed, 9_500, DEAD_AFTER),
                "iteration order must not change the verdict -- every node must agree");
    }

    @Test @DisplayName("a member whose heartbeat fell outside the window is not eligible")
    void staleMembersAreSkipped() {
        List<Member> roster = List.of(m("old", 1_000, 1_000), m("fresh", 5_000, 9_000));
        assertEquals("fresh", LeaderElection.electedLeader(roster, 9_500, DEAD_AFTER),
                "seniority only counts among the living");
        assertNull(LeaderElection.electedLeader(roster, 100_000, DEAD_AFTER),
                "nobody alive -> nobody leads");
        assertNull(LeaderElection.electedLeader(List.of(), 9_500, DEAD_AFTER), "empty roster -> nobody");
    }

    @Test @DisplayName("the boundary is exclusive: exactly one window since the last beat is already dead")
    void windowBoundary() {
        List<Member> roster = List.of(m("x", 0, 1_000));
        assertEquals("x", LeaderElection.electedLeader(roster, 1_000 + DEAD_AFTER - 1, DEAD_AFTER));
        assertNull(LeaderElection.electedLeader(roster, 1_000 + DEAD_AFTER, DEAD_AFTER));
    }

    @Test @DisplayName("a single process elects itself, and a second one defers to it")
    void endToEndOverAStore() {
        FakeStore store = new FakeStore();
        try (LeaderElection first = new LeaderElection(store, m("first", 0, 0), "test", 50, 4)) {
            first.start();
            assertTrue(first.isLeader(), "the only live process leads");

            // A second process announces later, so it is junior and must not take over.
            try (LeaderElection second = new LeaderElection(store, m("second", 0, 0), "test", 50, 4)) {
                second.start();
                assertFalse(second.isLeader(), "a later arrival defers to the incumbent");
                assertTrue(first.isLeader(), "and the incumbent keeps it");
                assertEquals(2, store.members().size());
            }
        }
    }

    @Test @DisplayName("standing down backdates the heartbeat so peers re-elect without waiting")
    void standDownHandsOverImmediately() {
        FakeStore store = new FakeStore();
        LeaderElection first = new LeaderElection(store, m("first", 0, 0), "test", 50, 4);
        first.start();
        LeaderElection second = new LeaderElection(store, m("second", 0, 0), "test", 50, 4);
        second.start();
        assertTrue(first.isLeader());

        first.close();
        // The roster now shows `first` with a zeroed heartbeat, so the rule picks `second` at once
        // rather than after a full timeout window.
        assertEquals("second", LeaderElection.electedLeader(store.members(), System.currentTimeMillis(), 200));
        second.close();
    }

    @Test @DisplayName("a store that throws stands the process down rather than leaving it leading")
    void aFailingStoreDoesNotLeaveAStaleLeader() throws Exception {
        FakeStore store = new FakeStore();
        // The window has to be long enough that a healthy process is comfortably inside it, and
        // short enough to wait out: 20ms x 2 missed beats = 40ms.
        LeaderElection e = new LeaderElection(store, m("only", 0, 0), "test", 20, 2);
        e.start();
        assertTrue(e.isLeader(), "healthy and alone -> leader");

        store.fail = true;
        Thread.sleep(200);   // well past the 40ms window, with every beat in it failing
        assertFalse(e.isLeader(),
                "isLeader() is fenced on the last successful beat, so a store outage stands it down");
        e.close();
    }

    /** A roster in a map, with the same one-atomic-step contract a real store must honour. */
    private static final class FakeStore implements ElectionStore {
        private final Map<String, Member> rows = new LinkedHashMap<>();
        volatile boolean fail;

        @Override
        public synchronized List<Member> step(Member self, long pruneBefore, ElectionRule elect) {
            if (fail) throw new IllegalStateException("store is down");
            rows.put(self.id(), self);
            rows.values().removeIf(m -> m.lastHeartbeat() < pruneBefore);
            List<Member> roster = new ArrayList<>(rows.values());
            elect.leaderOf(roster);
            return roster;
        }

        @Override public synchronized void standDown(Member self) {
            rows.put(self.id(), new Member(self.id(), self.name(), self.firstHeartbeat(), 0));
        }

        @Override public synchronized List<Member> members() {
            return new ArrayList<>(rows.values());
        }
    }
}
