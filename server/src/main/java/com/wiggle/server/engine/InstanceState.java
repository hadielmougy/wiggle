package com.wiggle.server.engine;

import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;


import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * An instance's state: what it is, what it may become, and how it gets there. One constant per
 * {@link InstanceStatus}, matched by name -- the parent half of the pair {@link TokenState}
 * completes.
 *
 * <p>Every write to an instance's status happens in this file and nowhere else. Reading one
 * constant tells you whether the instance is still live, whether a caller may cancel it, which
 * of its tokens are dispatchable, and every state it may move to; the transitions below are the
 * only ways to make one, and {@link #move} refuses any a state does not declare.
 *
 * <p>What a transition MEANS for the rest of the engine -- cancelling the instance's tokens,
 * resuming a waiting parent, handing over to the saga reverse pass -- stays in
 * {@link Instances}, which needs the graph and the drive loop to do it.
 */
enum InstanceState {

    /** The normal life of an instance: tokens moving over the graph. */
    RUNNING(Liveness.LIVE, InstanceStatus.COMPLETED, InstanceStatus.FAILED,
            InstanceStatus.CANCELLED, InstanceStatus.COMPENSATING) {
        @Override boolean running() { return true; }

        @Override boolean cancellable() { return true; }

        @Override boolean dispatches(boolean compensation) { return !compensation; }

        @Override void requireRunning(Instance inst) { }
    },

    /** The saga reverse pass owns it: forward work has stopped, undo tasks are in flight. */
    COMPENSATING(Liveness.LIVE, InstanceStatus.COMPENSATED, InstanceStatus.COMPENSATION_FAILED) {
        @Override boolean compensating() { return true; }

        @Override boolean dispatches(boolean compensation) { return compensation; }
    },

    /** A token reached a successful END and nothing was left running. */
    COMPLETED(Liveness.TERMINAL),

    /** Something unrecoverable, with nothing recorded to undo. */
    FAILED(Liveness.TERMINAL),

    /** Cancelled by a caller. Never compensates: only a failure starts the reverse pass. */
    CANCELLED(Liveness.TERMINAL),

    /** The reverse pass undid every recorded step. */
    COMPENSATED(Liveness.TERMINAL),

    /** A compensator ran out of retries. Stuck, and deliberately loud. */
    COMPENSATION_FAILED(Liveness.TERMINAL);

    private enum Liveness { LIVE, TERMINAL }

    private final Liveness liveness;
    private final Set<InstanceStatus> successors;

    InstanceState(Liveness liveness, InstanceStatus... successors) {
        this.liveness = liveness;
        this.successors = successors.length == 0
                ? Collections.unmodifiableSet(EnumSet.noneOf(InstanceStatus.class))
                : Collections.unmodifiableSet(EnumSet.copyOf(Set.of(successors)));
    }

    static InstanceState of(InstanceStatus status) {
        return valueOf(status.name());
    }

    static { for (InstanceStatus s : InstanceStatus.values()) of(s); }   // every status has a state

    /** Not finished: either running forward, or undoing. Mirrors {@code InstanceView.isTerminal}. */
    boolean live() {
        return liveness == Liveness.LIVE;
    }

    /** The forward flow is live: work may be reported against it, and a finished sub-workflow
     *  may resume its parent's token here. Only RUNNING -- COMPENSATING is going backwards. */
    boolean running() {
        return false;
    }

    /** A cancel request is honoured here; anywhere else it is ignored as already-decided. */
    boolean cancellable() {
        return false;
    }

    /** The reverse pass owns the instance: an undo may settle its comp-log entry here, and only
     *  here. The mirror of {@link #running}, which is the forward pass. */
    boolean compensating() {
        return false;
    }

    /** Whether a token of this instance may be handed to a worker. RUNNING dispatches the forward
     *  flow, COMPENSATING only the reverse pass, and a terminal instance neither. */
    boolean dispatches(boolean compensation) {
        return false;
    }

    /** The states this one may move to; empty for a terminal state. */
    Set<InstanceStatus> successors() {
        return successors;
    }

    /** Rejects work against an instance that is no longer running. */
    void requireRunning(Instance inst) {
        throw EngineException.conflict("instance " + inst.id + " is " + inst.status);
    }

    /** As {@link TokenState#moveTo}: the status for a legal move, or a failure. */
    InstanceStatus moveTo(InstanceStatus target) {
        if (target != InstanceStatus.valueOf(name()) && !successors.contains(target)) {
            throw new IllegalStateException("illegal instance transition " + name() + " -> " + target
                    + "; " + name() + " may only become " + successors);
        }
        return target;
    }
}
