package com.wiggle.server.engine;

import com.wiggle.core.*;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.*;

abstract class NodeBehaviour {

    abstract boolean advance(Step s);

    String route(Rows.Instance inst, Rows.Token t, Node node, StepReport report) {
        throw new IllegalStateException("node kind " + node.kind() + " is not worker-reported");
    }

    Overrun overrun(Rows.Token t, Node node, StepReport report, long maxIterations) {
        return Overrun.NONE;
    }

    static final class TaskNodeBehaviour extends NodeBehaviour {

        private final Tokens tokens;

        TaskNodeBehaviour(Tokens tokens) {
            this.tokens = tokens;
        }


        @Override boolean advance(Step s) {
            tokens.markReady(s.tx(), s.token(), s.node(), s.now());
            return true;
        }

        @Override String route(Rows.Instance inst, Rows.Token t, Node node, StepReport report) {
            Scopes.applyStepResult(inst, t, report.nextContext());
            return node.next();
        }
    }


    static final class PredicateNodeBehaviour extends NodeBehaviour {

        private final Tokens tokens;

        PredicateNodeBehaviour(Tokens tokens) {
            this.tokens = tokens;
        }

        @Override boolean advance(Step s) {
            tokens.markReady(s.tx(), s.token(), s.node(), s.now());
            return true;
        }

        @Override String route(Rows.Instance inst, Rows.Token t, Node node, StepReport report) {
            return GraphTraversal.successor(node, report.predicate());
        }

        @Override Overrun overrun(Rows.Token t, Node node, StepReport report, long maxIterations) {
            return tickLoopBudget(t, node, report.predicate(), maxIterations);
        }

        private static Overrun tickLoopBudget(Rows.Token t, Node node, boolean value, long maxIterations) {
            if (node.kind() != NodeKind.PREDICATE || !value || node.loopBudget() == 0) return Overrun.NONE;
            long budget = node.loopBudget() > 0 ? node.loopBudget() : maxIterations;
            long n = t.payload.loopCount(node.id()) + 1;
            if (n > budget) {
                return Overrun.of("loop '" + node.name() + "' exceeded its budget of " + budget
                        + " iterations (raise it with doWhile(name, maxIterations, body) or fix the condition)");
            }
            t.payload = t.payload.withLoopCount(node.id(), n);
            return Overrun.NONE;
        }
    }

    static final class SleepNodeBehaviour extends NodeBehaviour {

        @Override
        boolean advance(Step s) {
            Tokens.markWaiting(s.tx(), s.token(), s.now() + s.node().sleepMillis(), s.now());
            return true;
        }
    }


    static final class ForkNodeBehaviour extends NodeBehaviour {

        @Override boolean advance(Step s) {
            Tokens.spend(s.tx(), s.token(), NodeKind.FORK, s.now());
            String group = s.token().id;
            String childStack = s.token().pushJoinStack(group);
            List<String> starts = s.node().branches();
            Doc parentView = Scopes.currentView(s.inst(), s.token());
            for (int i = 0; i < starts.size(); i++) {
                Rows.Token child = Tokens.create(s.inst(), starts.get(i), childStack,
                        s.token().payload.push(TokenPayload.FrameKind.ARM, i, null, parentView), s.now());
                s.tx().insertToken(child);
                s.work().push(child);
            }
            return true;
        }
    }

    static final class DynForkNodeBehaviour extends NodeBehaviour {

        private final Instances instances;

        DynForkNodeBehaviour(Instances instances) {
            this.instances = instances;
        }

        @Override
        boolean advance(Step s) {
            Object items = Scopes.currentView(s.inst(), s.token()).get(s.node().itemsKey());
            if (items != null && !(items instanceof List) && !(items instanceof Map)) {
                instances.fail(s.tx(), s.inst(), "forEach '" + s.node().name() + "': context key '" + s.node().itemsKey()
                        + "' holds " + items.getClass().getSimpleName() + ", not a list or map", s.now());
                return false;
            }
            List<?> elements;
            List<String> mapKeys = null;   // non-null when iterating a map: the key each element came from
            if (items instanceof Map<?, ?> m) {
                mapKeys = new ArrayList<>(m.size());
                List<Object> vals = new ArrayList<>(m.size());
                for (Map.Entry<?, ?> entry : m.entrySet()) { mapKeys.add(String.valueOf(entry.getKey())); vals.add(entry.getValue()); }
                elements = vals;
            } else {
                elements = items == null ? List.of() : (List<?>) items;
            }
            Tokens.spend(s.tx(), s.token(), NodeKind.DYN_FORK, s.now());
            if (elements.isEmpty()) {
                LazyGraph def = s.def();
                Node join = def.node(s.node().next());
                Node after = def.node(join.next());
                String next = Scopes.isCombineNode(after) ? after.next() : join.next();
                Rows.Token cont = Tokens.continueAt(s.tx(), s.inst(), s.token(), next,
                        s.token().payload, s.now());
                s.work().push(cont);
                return true;
            }
            String group = s.token().id + "#" + elements.size();   // fork token id + width, parsed back at the join
            String childStack = s.token().pushJoinStack(group);
            String branchStart = s.node().branches().getFirst();
            for (int i = 0; i < elements.size(); i++) {
                String key = mapKeys == null ? null : mapKeys.get(i);
                Rows.Token child = Tokens.create(
                        s.inst(), branchStart, childStack, s.token().payload.push(TokenPayload.FrameKind.ITEM, i, key, Doc.of(elements.get(i))), s.now());
                s.tx().insertToken(child);
                s.work().push(child);
            }
            return true;
        }
    }

    static final class JoinNodeBehaviour extends NodeBehaviour {

        @Override
        boolean advance(Step s) {
            String group = s.token().currentJoinGroup();
            int expected = expectedAt(s.node(), group);
            Tokens.markJoined(s.tx(), s.token(), s.now());
            long arrived = s.tx().joinStacksAt(s.inst().id, s.node().id()).stream()
                    // TODO: evaluate postgres expression to count on the server side rather than pulling all the rows and counting here
                    .filter(stack -> group.equals(Rows.Token.innermostJoinGroup(stack)))
                    .count();
            if (arrived < expected) return true;
            List<Rows.Token> atBarrier = joinedAtBarrier(s.tx(), s.inst(), s.node(), group);
            Tokens.settleAll(s.tx(), atBarrier, s.now());
            TokenPayload contPayload = combinePayload(s.def(), s.node(), atBarrier, forkPayload(s.tx(), group));
            Rows.Token cont = Tokens.create(s.inst(), s.node().next(), s.token().popJoinStack(), contPayload, s.now());
            s.tx().insertToken(cont);
            s.work().push(cont);
            return true;
        }

        private static List<Rows.Token> joinedAtBarrier(Tx tx, Rows.Instance inst, Node node, String group) {
            return tx.tokensOf(inst.id).stream()
                    .filter(x -> x.status == TokenStatus.JOINED)
                    .filter(x -> node.id().equals(x.nodeId))
                    .filter(x -> Objects.equals(group, x.currentJoinGroup()))
                    .toList();
        }

        private static TokenPayload combinePayload(LazyGraph def, Node joinNode, List<Rows.Token> atBarrier,
                                                   TokenPayload basePayload) {
            Node agg = def.node(joinNode.next());
            if (!Scopes.isCombineNode(agg)) return basePayload;
            Map<String, Object> staged = new LinkedHashMap<>();
            String collectKey = agg.collectKey();
            if (collectKey == null) {
                List<String> armNames = agg.armNames();
                for (Rows.Token bt : atBarrier) {
                    TokenPayload.Frame frame = bt.payload.top();
                    if (frame == null) continue;   // defensive: a token that never carried a frame
                    staged.put(ScratchKeys.arm(armNames.get((int) frame.idx())), frame.view().raw());
                }
                return basePayload.withStaged(staged);
            }
            // forEach: collect each item's FINAL VIEW, ordered by item index; key by the source map
            // key when the input was a map. The views are exactly what each item's last step returned.
            TreeMap<Long, TokenPayload.Frame> ordered = new TreeMap<>();
            boolean mapInput = false;
            for (Rows.Token bt : atBarrier) {
                TokenPayload.Frame frame = bt.payload.top();
                if (frame == null) continue;
                mapInput |= frame.mapKey() != null;
                ordered.put(frame.idx(), frame);
            }
            if (mapInput) {
                Map<String, Object> byKey = new LinkedHashMap<>();
                for (TokenPayload.Frame frame : ordered.values()) byKey.put(frame.mapKey(), frame.view().raw());
                staged.put(collectKey, byKey);
            } else {
                List<Object> values = new ArrayList<>(ordered.size());
                for (TokenPayload.Frame frame : ordered.values()) values.add(frame.view().raw());
                staged.put(collectKey, values);
            }
            return basePayload.withStaged(staged);
        }

        /** A static join's width comes from the graph; a dynamic one travels in the group as "#n". */
        private static int expectedAt(Node node, String group) {
            if (node.expected() > 0) return node.expected();
            int hash = group == null ? -1 : group.lastIndexOf('#');
            return hash < 0 ? 1 : Integer.parseInt(group.substring(hash + 1));
        }

        /** The payload the fork token had when it spawned this group -- restoring it is how the join
         *  pops the children's frame. Empty for a group whose fork token is gone. */
        private static TokenPayload forkPayload(Tx tx, String group) {
            if (group == null) return TokenPayload.EMPTY;
            int hash = group.lastIndexOf('#');
            String forkTokenId = hash < 0 ? group : group.substring(0, hash);
            return tx.findToken(forkTokenId).map(f -> f.payload).orElse(TokenPayload.EMPTY);
        }
    }

    static final class SignalNodeBehaviour extends NodeBehaviour {

        @Override
        boolean advance(Step s) {
            Tokens.markAwaiting(s.tx(), s.token(), NodeKind.SIGNAL, s.node().name(), s.node().sleepMillis() > 0 ? s.now() + s.node().sleepMillis() : 0, s.now());
            return true;
        }
    }

    static final class SubflowNodeBehaviour extends NodeBehaviour {

        private final Instances instances;

        SubflowNodeBehaviour(Instances instances) {
            this.instances = instances;
        }

        @Override
        boolean advance(Step s) {
            Tokens.markAwaiting(s.tx(), s.token(), NodeKind.SUB_WORKFLOW,
                    s.node().activity(), 0, s.now());
            try {
                instances.start(s.tx(), s.node().activity(), null, Scopes.dispatchContext(s.inst(), s.token()), "sub:" + s.token().id, s.token().id);
            } catch (EngineException ex) {
                instances.fail(s.tx(), s.inst(), "sub-workflow '" + s.node().activity() + "': " + ex.getMessage(), s.now());
                return false;
            }
            return true;
        }
    }

    static final class EndNodeBehaviour extends NodeBehaviour {

        private final Instances instances;

        EndNodeBehaviour(Instances instances) {
            this.instances = instances;
        }

        @Override
        boolean advance(Step s) {
            Tokens.spend(s.tx(), s.token(), NodeKind.END, s.now());
            if (!s.node().success()) {
                instances.fail(s.tx(), s.inst(), s.node().reason() == null ? "terminated" : s.node().reason(), s.now());
                return false;
            }
            boolean anyActive = Tokens.anyActive(s.tx(), s.inst().id);
            if (anyActive || !s.work().isEmpty()) {
                return true;
            }
            instances.complete(s.tx(), s.inst(), s.node().reason(), s.now());
            return true;
        }
    }
}