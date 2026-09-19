package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.InstanceStatus;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

/**
 * The instance's state machine: every write to an instance's status, error and termination reason
 * happens here, and nowhere else in the engine. Its context is data rather than lifecycle state,
 * and lives half in this row and half in the token payload, so {@link Scopes} owns that.
 *
 * <p>An instance is born RUNNING with one token at the start node and stays that way until a
 * token reaches an END node, a token runs out of retries, or someone cancels it. Each of those
 * terminal transitions cancels the instance's remaining tokens and, for a sub-workflow, resumes
 * its parent. Failing is not always final: {@link Sagas} takes over when there is something to
 * undo.
 *
 * <p>This is the upper of the two lifecycles -- it drives {@link TokenLifecycle}, never the
 * reverse. Transaction boundaries are not its concern; every method here runs inside a
 * transaction its caller owns.
 */
final class InstanceLifecycle {

    private static final System.Logger LOG = System.getLogger(InstanceLifecycle.class.getName());

    /** Advancing tokens over the graph until each parks -- {@link WorkflowEngine}'s drive loop,
     *  as the instance lifecycle needs it to continue a flow it has just resumed. */
    interface Pump {
        void drive(Tx tx, LazyGraph def, Instance inst, Deque<Token> work, long now);
    }

    private final DefinitionRegistry definitions;
    private final TokenLifecycle tokens;
    private final Supplier<String> idMinter;
    private final Pump pump;
    private final Sagas sagas;

    InstanceLifecycle(DefinitionRegistry definitions, TokenLifecycle tokens,
                      Supplier<String> idMinter, Pump pump) {
        this.definitions = definitions;
        this.tokens = tokens;
        this.idMinter = idMinter;
        this.pump = pump;
        this.sagas = new Sagas(this, tokens);
    }

    /**
     * Starts an instance and drives it to its first parking place. {@code parentTokenId} links a
     * sub-workflow to the token awaiting it.
     */
    String start(Tx tx, String workflow, Integer version, Object context,
                 String correlationId, String parentTokenId) {
        int v = version != null ? version : tx.latestVersion(workflow).orElseThrow(
                () -> EngineException.notFound("workflow '" + workflow + "'"));
        LazyGraph def = definitions.graph(tx, workflow, v);
        long now = System.currentTimeMillis();
        Instance inst = insert(tx, def, context, correlationId, parentTokenId, now);
        Token t = TokenLifecycle.mint(inst, def.startNode(), "", null, now);
        tx.insertToken(t);
        LOG.log(System.Logger.Level.DEBUG, () -> "start: instance " + inst.id + " of " + def.key()
                + " at node " + def.startNode() + " correlationId=" + correlationId);
        pump.drive(tx, def, inst, new ArrayDeque<>(List.of(t)), now);
        return inst.id;
    }

    private Instance insert(Tx tx, LazyGraph def, Object context, String correlationId,
                            String parentTokenId, long now) {
        Instance inst = new Instance();
        inst.id = idMinter.get();
        inst.workflow = def.name();
        inst.version = def.version();
        inst.correlationId = correlationId;
        inst.parentTokenId = parentTokenId;
        inst.status = InstanceStatus.RUNNING;
        inst.context = Doc.of(context);
        inst.createdAt = now;
        inst.updatedAt = now;
        tx.insertInstance(inst);
        return inst;
    }

    /**
     * Cancels one instance and returns its children, for the caller to cancel in their own
     * transactions -- cascading in-transaction would take the parent lock before the child's.
     * An instance that is no longer RUNNING is left alone, and has no children to report.
     */
    List<String> cancel(Tx tx, String instanceId, String reason) {
        Instance inst = tx.lockInstance(instanceId).orElseThrow(() -> EngineException.notFound("instance"));
        if (inst.status != InstanceStatus.RUNNING) {
            LOG.log(System.Logger.Level.DEBUG, () ->
                    "cancel: instance " + instanceId + " ignored, already " + inst.status);
            return List.of();
        }
        long now = System.currentTimeMillis();
        TokenLifecycle.cancelAll(tx, inst.id, now);
        inst.status = InstanceStatus.CANCELLED;
        inst.terminationReason = reason;
        inst.updatedAt = now;
        tx.updateInstance(inst);
        notifyParent(tx, inst, now);
        LOG.log(System.Logger.Level.DEBUG, () -> "cancel: instance " + instanceId + " cancelled, reason=" + reason);
        return tx.childInstanceIds(instanceId);
    }

    /** The instance ran out of flow at a successful END node with no token left anywhere. */
    void complete(Tx tx, Instance inst, String reason, long now) {
        inst.status = InstanceStatus.COMPLETED;
        inst.terminationReason = reason;
        inst.updatedAt = now;
        tx.updateInstance(inst);
        notifyParent(tx, inst, now);
    }

    /** Fails the instance, unless there is something to undo -- then the saga reverse pass
     *  takes it over instead, and the outcome is not known until that pass lands. */
    void fail(Tx tx, Instance inst, String error, long now) {
        TokenLifecycle.cancelAll(tx, inst.id, now);
        if (sagas.begin(tx, inst, error, now)) return;
        inst.status = InstanceStatus.FAILED;
        inst.error = error;
        inst.updatedAt = now;
        tx.updateInstance(inst);
        LOG.log(System.Logger.Level.INFO, () -> "instance " + inst.id + " failed: " + error);
        notifyParent(tx, inst, now);
    }

    /** The saga reverse pass has taken the instance over; its outcome is not known until the
     *  pass lands {@link #compensated} or {@link #compensationFailed}. */
    void compensating(Tx tx, Instance inst, String error, long now) {
        inst.status = InstanceStatus.COMPENSATING;
        inst.error = error;
        inst.updatedAt = now;
        tx.updateInstance(inst);
    }

    /** The reverse pass undid everything it had recorded. */
    void compensated(Tx tx, Instance inst, long now) {
        inst.status = InstanceStatus.COMPENSATED;
        inst.updatedAt = now;
        tx.updateInstance(inst);
        notifyParent(tx, inst, now);
    }

    /** The reverse pass could not finish: the instance is stuck and needs a human. */
    void compensationFailed(Tx tx, Instance inst, String error, long now) {
        inst.status = InstanceStatus.COMPENSATION_FAILED;
        inst.error = (inst.error == null ? "" : inst.error + "; ") + error;
        inst.updatedAt = now;
        tx.updateInstance(inst);
        notifyParent(tx, inst, now);
    }

    static void touch(Tx tx, Instance inst, long now) {
        inst.updatedAt = now;
        tx.updateInstance(inst);
    }

    static void requireRunning(Instance inst) {
        if (inst.status != InstanceStatus.RUNNING) {
            throw EngineException.conflict("instance " + inst.id + " is " + inst.status);
        }
    }

    /**
     * Called whenever an instance reaches a terminal state: if it was a sub-workflow, resume (or
     * fail) the parent's waiting token. Lock ordering is always child -> parent, never the
     * reverse in one transaction, so parent/child completions cannot deadlock.
     */
    void notifyParent(Tx tx, Instance child, long now) {
        if (child.parentTokenId == null) return;
        Token probe = tx.findToken(child.parentTokenId).orElse(null);
        if (probe == null) return;
        Instance parent = tx.lockInstance(probe.instanceId).orElse(null);
        if (parent == null || parent.status != InstanceStatus.RUNNING) return;
        Token t = tx.findToken(child.parentTokenId).orElse(null);   // re-read under the lock
        if (t == null || t.status != TokenStatus.AWAITING || t.kind != NodeKind.SUB_WORKFLOW) return;
        LazyGraph def = definitions.graph(tx, parent.workflow, parent.version);
        Node node = def.node(t.nodeId);
        if (child.status != InstanceStatus.COMPLETED) {
            TokenLifecycle.failParked(tx, t, now);
            fail(tx, parent, "sub-workflow '" + node.activity() + "' " + child.status
                    + (child.error == null ? "" : ": " + child.error), now);
            return;
        }
        TokenPayload contPayload = Scopes.mergeIntoScope(parent, t.payload, child.context.raw());
        TokenLifecycle.settle(tx, t, now);
        touch(tx, parent, now);
        Token cont = TokenLifecycle.mint(parent, node.next(), t.joinStack, contPayload, now);
        tx.insertToken(cont);
        LOG.log(System.Logger.Level.DEBUG, () -> "sub-workflow " + child.id + " completed -> resuming parent "
                + parent.id + " at " + node.next());
        pump.drive(tx, def, parent, new ArrayDeque<>(List.of(cont)), now);
    }

    /** A compensator finished its undo: settle it and drive the reverse pass on. */
    void compensatorCompleted(Tx tx, Instance inst, Token t, long seq, long now) {
        sagas.complete(tx, inst, t, seq, now);
    }

    /** A compensator is itself out of retries: the reverse pass cannot finish. */
    void compensatorExhausted(Tx tx, Instance inst, Node node, long seq, String failReason, long now) {
        sagas.compensatorExhausted(tx, inst, node, seq, failReason, now);
    }

    /** Drops terminal instances last touched before {@code cutoff}. */
    static int purgeTerminalBefore(Tx tx, long cutoff, int max) {
        return tx.deleteTerminalInstancesBefore(cutoff, max);
    }
}
