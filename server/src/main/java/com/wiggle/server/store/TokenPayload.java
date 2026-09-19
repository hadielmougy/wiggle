package com.wiggle.server.store;

import com.wiggle.core.Doc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The engine's own bookkeeping on a token, as a value rather than as reserved keys inside the
 * user's context. Three independent things travel here, and each used to be a magic key in one
 * shared JSON object the engine had to keep sorting out from the caller's data:
 *
 * <ul>
 *   <li>{@code scopes} -- the nesting stack. A fork pushes an {@code ARM} frame onto each branch, a
 *       forEach an {@code ITEM} frame onto each element, and a join pops by restoring the fork
 *       token's payload. The TOP frame's view is the token's complete current context; outside any
 *       frame that is the shared instance context.</li>
 *   <li>{@code loopCounts} -- per-loop-guard true-evaluation counts, carried along the token chain
 *       so a budget survives the whole loop.</li>
 *   <li>{@code staged} -- inputs waiting for a combine handler (each arm's result by name, or a
 *       forEach's collected results under one key), and the snapshot pair a compensator receives.
 *       Non-empty only between a join and the combine that consumes it.</li>
 * </ul>
 *
 * <p>Immutable, and shared freely: {@link Rows.Token#clone()} is shallow, so a mutable payload would
 * alias across stored copies in the in-memory store. Every {@code with}/{@code push} returns a new
 * value. Frame views are treated as immutable too -- they are replaced, never edited in place.
 */
public record TokenPayload(List<Frame> scopes, Map<String, Long> loopCounts, Map<String, Object> staged) {

    public static final TokenPayload EMPTY = new TokenPayload(List.of(), Map.of(), Map.of());

    /** Which construct pushed a frame. An {@code ITEM} frame also carries the element's position. */
    public enum FrameKind { ARM, ITEM }

    /**
     * One nesting level's private view of the context. {@code idx} is the arm's or item's position
     * in fork order; {@code mapKey} is the source key when a forEach iterated a map, else null.
     */
    public record Frame(FrameKind kind, long idx, String mapKey, Doc view) {}

    public TokenPayload {
        scopes = scopes == null ? List.of() : List.copyOf(scopes);
        loopCounts = loopCounts == null ? Map.of() : Map.copyOf(loopCounts);
        staged = staged == null ? Map.of() : Map.copyOf(staged);
    }

    public boolean isEmpty() {
        return scopes.isEmpty() && loopCounts.isEmpty() && staged.isEmpty();
    }

    /** The innermost frame, or null outside every scope. */
    public Frame top() {
        return scopes.isEmpty() ? null : scopes.getLast();
    }

    /** This payload with one more frame on the stack -- what a fork or forEach hands each child. */
    public TokenPayload push(FrameKind kind, long idx, String mapKey, Doc view) {
        List<Frame> next = new ArrayList<>(scopes);
        next.add(new Frame(kind, idx, mapKey, view));
        return new TokenPayload(next, loopCounts, staged);
    }

    /** Replaces the top frame's view. A no-op outside any scope, where the view is the shared context. */
    public TokenPayload withTopView(Doc view) {
        if (scopes.isEmpty()) return this;
        List<Frame> next = new ArrayList<>(scopes);
        Frame t = next.getLast();
        next.set(next.size() - 1, new Frame(t.kind(), t.idx(), t.mapKey(), view));
        return new TokenPayload(next, loopCounts, staged);
    }

    /** The position of the innermost {@code ITEM} frame, or -1: where a step's ambient item index,
     *  map key and base context come from, at any nesting depth. */
    public int innermostItem() {
        for (int i = scopes.size() - 1; i >= 0; i--) {
            if (scopes.get(i).kind() == FrameKind.ITEM) return i;
        }
        return -1;
    }

    public long loopCount(String nodeId) {
        return loopCounts.getOrDefault(nodeId, 0L);
    }

    public TokenPayload withLoopCount(String nodeId, long count) {
        Map<String, Long> next = new LinkedHashMap<>(loopCounts);
        next.put(nodeId, count);
        return new TokenPayload(scopes, next, staged);
    }

    public TokenPayload withStaged(Map<String, Object> inputs) {
        return new TokenPayload(scopes, loopCounts, inputs);
    }

    /** Drops the staged inputs, once the combine that was waiting for them has run. */
    public TokenPayload withoutStaged() {
        return staged.isEmpty() ? this : new TokenPayload(scopes, loopCounts, Map.of());
    }
}
