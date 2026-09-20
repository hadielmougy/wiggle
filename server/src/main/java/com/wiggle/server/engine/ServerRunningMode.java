package com.wiggle.server.engine;

import com.wiggle.core.Doc;
import com.wiggle.core.Node;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Tx;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class ServerRunningMode implements RunningMode {

    private final NodeBehaviourFactory nodeBehaviourFactory;
    private final DefinitionRegistry definitions;
    private final Instances instances;


    ServerRunningMode(Instances instances, NodeBehaviourFactory nodeBehaviourFactory, DefinitionRegistry definitions) {
        this.instances = instances;
        this.nodeBehaviourFactory = nodeBehaviourFactory;
        this.definitions = definitions;
    }

    @Override
    public void advance(RunningModeContext ctx) {
        Tokens.LockedTask locked = Tokens.lock(ctx.tx(), ctx.taskId());
        Rows.Instance inst = locked.inst();
        Rows.Token t = locked.token();
        Tokens.requireLease(t, ctx.leaseOwner());
        long now = System.currentTimeMillis();
        Instances.requireRunning(inst);
        LazyGraph def = definitions.graph(ctx.tx(), t.workflow, t.version);
        Node node = def.node(t.nodeId);
        Doc compInput = node.compensable() ? Scopes.dispatchContext(inst, t) : null;
        NodeBehaviour behaviour = nodeBehaviourFactory.getNodeBehaviour(node.kind());
        String next = behaviour.route(inst, t, node, ctx.result());
        if (node.compensable()) Sagas.capture(ctx.tx(), inst, t, node, compInput, now);
        String overrun = behaviour.overrunAfter( t, node, ctx.result(), ctx.loopMaxIterations());
        if (overrun != null) {
            Tokens.settle(ctx.tx(), t, now);
            instances.fail(ctx.tx(), inst, overrun, now);
            return;
        }
        Tokens.settle(ctx.tx(), t, now);
        Instances.touch(ctx.tx(), inst, now);
        Rows.Token cont = Tokens.continueAt(ctx.tx(), inst, t, next,
                Scopes.stripCombineScratch(node, t.payload), now);
        drive(ctx.tx(), def, inst, new ArrayDeque<>(List.of(cont)), now);

    }

    private void drive(Tx tx, LazyGraph def, Rows.Instance inst, Deque<Rows.Token> work, long now) {
        long budget = 10_000;
        long guard = 0;
        while (!work.isEmpty()) {
            if (++guard > budget) {
                throw new IllegalStateException("drive budget exceeded in workflow " + def.key()
                        + ": " + guard + " advances without parking (runaway server-side node chain)");
            }
            Rows.Token t = work.pop();
            int before = work.size();
            Step s = new Step(tx, def, inst, t, def.node(t.nodeId), work, now);
            if (!nodeBehaviourFactory.getNodeBehaviour(s.node().kind()).advance(s)) return;
            budget += Math.max(0, work.size() - before - 1);
        }
    }
}
