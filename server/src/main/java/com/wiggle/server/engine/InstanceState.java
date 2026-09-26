package com.wiggle.server.engine;

import com.wiggle.server.store.Rows.Instance;
import com.wiggle.core.InstanceStatus;


import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * What an instance in each state may BECOME, and what it PERMITS. One constant per
 * {@link InstanceStatus}, matched by name -- the parent half of the pair {@link TokenState}
 * completes.
 *
 * <p>What the status MEANS -- live, running forward, undoing -- belongs to {@link InstanceStatus}
 * itself, so a reader holding only the status does not need this type. What is left here is what
 * needs the transition graph: whether a caller may cancel, which of its tokens are dispatchable,
 * every state it may move to, and the refusal of any move a state does not declare.
 *
 * <p>What a transition MEANS for the rest of the engine -- cancelling the instance's tokens,
 * resuming a waiting parent, handing over to the saga reverse pass -- stays in
 * {@link Instances}, which needs the graph and the drive loop to do it.
 */
enum InstanceState {

    /** The normal life of an instance: tokens moving over the graph. */
    RUNNING(InstanceStatus.COMPLETED, InstanceStatus.FAILED,
            InstanceStatus.CANCELLED, InstanceStatus.COMPENSATING) {
        @Override boolean cancellable() { return true; }

        @Override boolean dispatches(boolean compensation) { return !compensation; }

        @Override void requireRunning(Instance inst) { }
    },

    /** The saga reverse pass owns it: forward work has stopped, undo tasks are in flight. */
    COMPENSATING(InstanceStatus.COMPENSATED, InstanceStatus.COMPENSATION_FAILED) {
        @Override boolean dispatches(boolean compensation) { return compensation; }
    },

    /** A token reached a successful END and nothing was left running. */
    COMPLETED(),

    /** Something unrecoverable, with nothing recorded to undo. */
    FAILED(),

    /** Cancelled by a caller. Never compensates: only a failure starts the reverse pass. */
    CANCELLED(),

    /** The reverse pass undid every recorded step. */
    COMPENSATED(),

    /** A compensator ran out of retries. Stuck, and deliberately loud. */
    COMPENSATION_FAILED();

    private final Set<InstanceStatus> successors;

    InstanceState(InstanceStatus... successors) {
        this.successors = successors.length == 0
                ? Collections.unmodifiableSet(EnumSet.noneOf(InstanceStatus.class))
                : Collections.unmodifiableSet(EnumSet.copyOf(Set.of(successors)));
    }

    static InstanceState of(InstanceStatus status) {
        return valueOf(status.name());
    }

    static { for (InstanceStatus s : InstanceStatus.values()) of(s); }   // every status has a state

    /** A cancel request is honoured here; anywhere else it is ignored as already-decided. */
    boolean cancellable() {
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
