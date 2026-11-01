package com.wiggle.server.engine;

import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;

import java.util.Deque;

/**
 * The drive pump: advances tokens over server-side nodes until they park. Shared by the
 * engine's own sweeps (timers, signals) and by {@link StepChain}, so the runaway
 * guard has exactly one definition.
 */
final class Drive {

    private Drive() {}

    /**
     * Advances every token in {@code work} until it parks or the instance ends. Caps chain
     * DEPTH, not fan-out breadth: every extra child a fork/forEach enqueues beyond the usual
     * single continuation grows the budget by one, so any fan-out width advances fine while a
     * runaway chain of server-side nodes still trips the cap.
     */
    static void pump(NodeBehaviourFactory behaviours, LazyGraph def, Instance inst,
                     Deque<Token> work, long now) {
        long budget = 10_000;
        long guard = 0;
        while (!work.isEmpty()) {
            if (++guard > budget) {
                throw new IllegalStateException("drive budget exceeded in workflow " + def.key()
                        + ": " + guard + " advances without parking (runaway server-side node chain)");
            }
            Token t = work.pop();
            int before = work.size();
            Step s = new Step(def, inst, t, def.node(t.nodeId), work, now);
            if (!behaviours.getNodeBehaviour(s.node().kind()).advance(s)) return;
            budget += Math.max(0, work.size() - before - 1);
        }
    }
}
