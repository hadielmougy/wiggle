package com.wiggle.election;

import java.util.List;

/**
 * Who wins an election, given the roster as the store just read it. The rule is the election's,
 * not the backend's -- {@link ElectionStore#step} calls it inside its transaction and persists
 * whatever it returns.
 */
@FunctionalInterface
public interface ElectionRule {

    /** The winning member's id, or null when the roster holds no live member. */
    String leaderOf(List<Member> roster);
}
