package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TaskActivation;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.List;
import java.util.Map;

/**
 * The saga reverse pass. {@link #capture} appends a compensable step's completion to the comp-log
 * (in the same transaction as the completion); {@link #begin} takes over a failing instance when
 * uncompensated entries exist, running the undos newest-first as ordinary leased tasks under
 * activity {@code "<activity>#compensate"}.
 */
final class Sagas {

    private static final System.Logger LOG = System.getLogger(Sagas.class.getName());

    private final Instances instances;
    private final Tokens tokens;

    Sagas(Instances instances, Tokens tokens) {
        this.instances = instances;
        this.tokens = tokens;
    }

    /** The comp-log seq a compensation token settles, or null for a forward token. */
    static Long seqOf(Token t) {
        return t.compSeq;
    }

    static boolean isCompensation(Token t) {
        return t.compSeq != null;
    }

    /** A compensation task's activation: the context is the {input, result} snapshot pair the
     *  reverse pass staged for it; the worker's compensator wrapper splits it. Always SERVER mode --
     *  the reverse pass never chains. */
    static TaskActivation activation(Instance inst, Token t, String workerId, long until) {
        return new TaskActivation(t.id, inst.id, inst.workflow, inst.version,
                t.nodeId, t.activity, t.activity, NodeKind.TASK, t.nextAttempt(), until, workerId,
                t.payload.staged(), null, 0, null, ExecutionMode.SERVER);
    }

    /** Appends this compensable step's completion to the comp-log: both snapshots, captured
     *  atomically with the completion itself. Result = the context as the step left it. */
    static void capture(Tx tx, Instance inst, Token t, Node node, Doc input, long now) {
        Rows.CompLog e = new Rows.CompLog();
        e.instanceId = inst.id;
        e.seq = tx.compensationLog(inst.id).size() + 1L;
        e.nodeId = node.id();
        e.activity = node.activity();
        e.queue = node.queue();
        e.input = input;
        e.result = Scopes.dispatchContext(inst, t);   // post-apply: as the step left it
        tx.appendCompensation(e);
    }

    /**
     * Takes over a failing instance when uncompensated entries exist: COMPENSATING, then the
     * reverse pass. The parent (for a sub-workflow) is notified only when the pass lands
     * COMPENSATED or COMPENSATION_FAILED -- the child's outcome isn't known until then.
     *
     * @return false when there is nothing to undo; the caller fails the instance plainly
     */
    boolean begin(Tx tx, Instance inst, String error, long now) {
        boolean hasUndo = tx.compensationLog(inst.id).stream().anyMatch(e -> !e.compensated);
        if (!hasUndo) return false;
        instances.compensating(tx, inst, error, now);
        LOG.log(System.Logger.Level.INFO, () -> "instance " + inst.id + " failed (" + error
                + ") -> compensating");
        createNext(tx, inst, now);
        return true;
    }

    /** A compensator completed: settle its entry and drive the next-newest, or finish the pass. */
    void complete(Tx tx, Instance inst, Token t, long seq, long now) {
        if (inst.status != InstanceStatus.COMPENSATING) {
            throw EngineException.conflict("instance " + inst.id + " is " + inst.status);
        }
        Tokens.settle(tx, t, now);
        tx.markCompensated(inst.id, seq);
        Instances.touch(tx, inst, now);
        createNext(tx, inst, now);
    }

    /** A compensator itself is out of retries: the one thing worse than a stuck saga is a stuck
     *  saga reported as success -- refuse to pretend and demand a human. */
    void compensatorExhausted(Tx tx, Instance inst, Node node, long seq, String failReason, long now) {
        if (inst.status != InstanceStatus.COMPENSATING) return;
        LOG.log(System.Logger.Level.WARNING, () -> "instance " + inst.id
                + " COMPENSATION_FAILED at undo seq " + seq + ": " + failReason);
        instances.compensationFailed(tx, inst,
                "compensator '" + node.name() + "' (undo seq " + seq + ") failed: " + failReason, now);
    }

    /** Mints the reverse pass's next token: the newest uncompensated entry, dispatched to the
     *  step's own queue under activity "<activity>#compensate" -- a real leased task, executed by
     *  a worker through the normal claim path. No entry left -> the pass is done: COMPENSATED. */
    private void createNext(Tx tx, Instance inst, long now) {
        List<Rows.CompLog> log = tx.compensationLog(inst.id);
        Rows.CompLog next = null;
        for (int i = log.size() - 1; i >= 0; i--) {
            if (!log.get(i).compensated) { next = log.get(i); break; }
        }
        if (next == null) {
            LOG.log(System.Logger.Level.INFO, () -> "instance " + inst.id
                    + " compensated cleanly (" + log.size() + " undo(s))");
            instances.compensated(tx, inst, now);
            return;
        }
        TokenPayload payload = TokenPayload.EMPTY.withStaged(Map.of(
                "input", next.input.raw(),
                "result", next.result.raw()));
        Token t = Tokens.create(inst, next.nodeId, "", payload, now);
        t.compSeq = next.seq;
        t.activity = next.activity + "#compensate";
        t.queue = next.queue;
        tx.insertToken(t);
        tokens.wake(t.queue);   // wake-on-produce, post-commit
        Rows.CompLog fNext = next;
        LOG.log(System.Logger.Level.DEBUG, () -> "compensation: instance " + inst.id
                + " dispatching undo seq " + fNext.seq + " (" + fNext.activity + ")");
    }
}
