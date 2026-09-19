package com.wiggle.server.engine;

import com.wiggle.core.*;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.TokenPayload;
import com.wiggle.server.store.Tx;

import java.util.*;

abstract class NodeBehaviour {

    private static final System.Logger LOG = System.getLogger(NodeBehaviour.class.getName());

    abstract boolean advance(Step s);

    String route(Rows.Instance inst, Rows.Token t, Node node, Object result) {
        throw new IllegalStateException("node kind " + name() + " is not worker-completed");
    }

    String routeReported(Rows.Instance inst, Rows.Token t, Node node, WorkflowEngine.StepInput step) {
        throw new IllegalStateException("node kind " + name() + " is not worker-reported");
    }

    String overrunAfter(WorkflowEngine e, Rows.Token t, Node node, Object result) {
        return null;
    }

    String overrunReported(WorkflowEngine e, Rows.Token t, Node node, WorkflowEngine.StepInput step) {
        return null;
    }

    private String name() {
        return NodeBehaviour.class.getSimpleName();
    }


    static final class TaskNodeBehaviour extends NodeBehaviour {

        private final TokenLifecycle tokenLifecycle;

        TaskNodeBehaviour(TokenLifecycle tokenLifecycle) {
            this.tokenLifecycle = tokenLifecycle;
        }


        @Override boolean advance(Step s) {
            tokenLifecycle.parkReady(s.tx(), s.inst(), s.token(), s.node(), s.now());
            return true;
        }

        @Override String route(Rows.Instance inst, Rows.Token t, Node node, Object result) {
            Scopes.applyStepResult(inst, t, result);
            return node.next();
        }

        @Override String routeReported(Rows.Instance inst, Rows.Token t, Node node, WorkflowEngine.StepInput step) {
            Scopes.applyStepResult(inst, t, step.merge());
            return node.next();
        }
    }


    static final class PredicateNodeBehaviour extends NodeBehaviour {

        private final TokenLifecycle tokenLifecycle;

        PredicateNodeBehaviour(TokenLifecycle tokenLifecycle) {
            this.tokenLifecycle = tokenLifecycle;
        }

        @Override boolean advance(Step s) {
            tokenLifecycle.parkReady(s.tx(), s.inst(), s.token(), s.node(), s.now());
            return true;
        }

        @Override String route(Rows.Instance inst, Rows.Token t, Node node, Object result) {
            boolean value = predicateValue(result);
            String next = GraphTraversal.successor(node, value);
            LOG.log(System.Logger.Level.DEBUG, () -> "complete: predicate " + node.name()
                    + " of instance " + inst.id + " evaluated " + value + " -> " + next);
            return next;
        }

        @Override String routeReported(Rows.Instance inst, Rows.Token t, Node node, WorkflowEngine.StepInput step) {
            boolean value = step.predicateValue() != null && step.predicateValue();
            return GraphTraversal.successor(node, value);
        }

        @Override String overrunAfter(WorkflowEngine e, Rows.Token t, Node node, Object result) {
            return tickLoopBudget(e, t, node, predicateValue(result));
        }

        @Override String overrunReported(WorkflowEngine e, Rows.Token t, Node node, WorkflowEngine.StepInput step) {
            return tickLoopBudget(e, t, node, step.predicateValue() != null && step.predicateValue());
        }

        private static boolean predicateValue(Object result) {
            if (result instanceof Boolean b) return b;
            if (result instanceof Map<?, ?> m && m.get("value") instanceof Boolean b) return b;
            throw EngineException.badRequest("predicate result must be a boolean or {\"value\": <boolean>}");
        }

        private static String tickLoopBudget(WorkflowEngine e, Rows.Token t, Node node, boolean value) {
            if (node.kind() != NodeKind.PREDICATE || !value || node.loopBudget() == 0) return null;
            long budget = node.loopBudget() > 0 ? node.loopBudget() : e.loopMaxIterations;
            long n = t.payload.loopCount(node.id()) + 1;
            if (n > budget) {
                return "loop '" + node.name() + "' exceeded its budget of " + budget
                        + " iterations (raise it with doWhile(name, maxIterations, body) or fix the condition)";
            }
            t.payload = t.payload.withLoopCount(node.id(), n);
            return null;
        }
    }

    static final class SleepNodeBehaviour extends NodeBehaviour {

        @Override
        boolean advance(Step s) {
            Tx tx = s.tx(); Rows.Instance inst = s.inst(); Rows.Token t = s.token();
            Node node = s.node(); long now = s.now();
            Rows.TokenStatus before = TokenState.parkWaiting(tx, t, now + node.sleepMillis(), now);
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (SLEEP) " + before + " -> WAITING until " + t.availableAt
                    + " (" + node.sleepMillis() + "ms)");
            return true;
        }
    }


    static final class ForkNodeBehaviour extends NodeBehaviour {

        @Override boolean advance(Step s) {
            Tx tx = s.tx(); Rows.Instance inst = s.inst(); Rows.Token t = s.token();
            Node node = s.node(); Deque<Rows.Token> work = s.work(); long now = s.now();
            Rows.TokenStatus before = TokenState.spend(tx, t, NodeKind.FORK, now);
            String group = t.id;
            String childStack = t.pushJoinStack(group);
            List<String> starts = node.branches();
            Doc parentView = Scopes.currentView(inst, t);
            for (int i = 0; i < starts.size(); i++) {
                Rows.Token child = TokenState.create(inst, starts.get(i), childStack,
                        t.payload.push(TokenPayload.FrameKind.ARM, i, null, parentView), now);
                tx.insertToken(child);
                work.push(child);
            }
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (FORK) " + before + " -> DONE, spawned " + node.branches().size()
                    + " branch(es) in group " + group + ": " + node.branches());
            return true;
        }
    }

    static final class DynForkNodeBehaviour extends NodeBehaviour {

        private final InstanceLifecycle instances;

        DynForkNodeBehaviour(InstanceLifecycle instances) {
            this.instances = instances;
        }

        @Override
        boolean advance(Step s) {
            Tx tx = s.tx();
            Rows.Instance inst = s.inst();
            Rows.Token t = s.token();
            Node node = s.node();
            Deque<Rows.Token> work = s.work();
            long now = s.now();
            Object items = Scopes.currentView(inst, t).get(node.itemsKey());
            if (items != null && !(items instanceof List) && !(items instanceof Map)) {
                instances.fail(tx, inst, "forEach '" + node.name() + "': context key '" + node.itemsKey()
                        + "' holds " + items.getClass().getSimpleName() + ", not a list or map", now);
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
            Rows.TokenStatus before = TokenState.spend(tx, t, NodeKind.DYN_FORK, now);
            if (elements.isEmpty()) {
                // Nothing to fan out over: continue past the paired join AND its combine (there is
                // nothing to collect, so the combine is skipped and the context is untouched).
                LazyGraph def = s.def();
                Node join = def.node(node.next());
                Node after = def.node(join.next());
                String next = Scopes.isCombineNode(after) ? after.next() : join.next();
                Rows.Token cont = TokenState.create(inst, next, t.joinStack, t.payload, now);
                tx.insertToken(cont);
                work.push(cont);
                LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                        + node.name() + " (DYN_FORK) " + before + " -> DONE, empty '" + node.itemsKey()
                        + "' skips the join and combine");
                return true;
            }
            String group = t.id + "#" + elements.size();   // fork token id + width, parsed back at the join
            String childStack = t.pushJoinStack(group);
            String branchStart = node.branches().getFirst();
            for (int i = 0; i < elements.size(); i++) {
                String key = mapKeys == null ? null : mapKeys.get(i);
                Rows.Token child = TokenState.create(inst, branchStart, childStack,
                        t.payload.push(TokenPayload.FrameKind.ITEM, i, key, Doc.of(elements.get(i))), now);
                tx.insertToken(child);
                work.push(child);
            }
            int n = elements.size();
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (DYN_FORK) " + before + " -> DONE, spawned " + n
                    + " isolated branch(es) over '" + node.itemsKey() + "' in group " + group);
            return true;
        }
    }

    static final class JoinNodeBehaviour extends NodeBehaviour {

        @Override
        boolean advance(Step s) {
            Tx tx = s.tx(); LazyGraph def = s.def(); Rows.Instance inst = s.inst(); Rows.Token t = s.token();
            Node node = s.node(); Deque<Rows.Token> work = s.work(); long now = s.now();
            String group = t.currentJoinGroup();
            int expected = expectedAt(node, group);
            Rows.TokenStatus before = TokenState.parkJoined(tx, t, now);
            List<Rows.Token> atBarrier = joinedAtBarrier(tx, inst, node, group);
            if (atBarrier.size() < expected) {
                LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                        + node.name() + " (JOIN) " + before + " -> JOINED, waiting on barrier " + group
                        + " (" + atBarrier.size() + "/" + expected + ")");
                return true;
            }
            TokenState.settleAll(tx, atBarrier, now);
            // Restore the payload the branches started from, so nesting scopes correctly, then (for a
            // combine fork) stage each isolated branch's result under its arm name for the aggregator.
            TokenPayload contPayload = combinePayload(def, node, atBarrier, forkPayload(tx, group));
            Rows.Token cont = TokenState.create(inst, node.next(), t.popJoinStack(), contPayload, now);
            tx.insertToken(cont);
            work.push(cont);
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (JOIN) " + before + " -> JOINED, barrier " + group
                    + " satisfied (" + atBarrier.size() + "/" + expected + ") -> " + node.next());
            return true;
        }

        private static List<Rows.Token> joinedAtBarrier(Tx tx, Rows.Instance inst, Node node, String group) {
            return tx.tokensOf(inst.id).stream()
                    .filter(x -> x.status == Rows.TokenStatus.JOINED)
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
            Tx tx = s.tx(); Rows.Instance inst = s.inst(); Rows.Token t = s.token();
            Node node = s.node(); long now = s.now();
            Rows.TokenStatus before = TokenState.parkAwaiting(tx, t, NodeKind.SIGNAL, node.name(),
                    node.sleepMillis() > 0 ? now + node.sleepMillis() : 0, now);
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (SIGNAL) " + before + " -> AWAITING, deadline="
                    + (t.availableAt > 0 ? t.availableAt : "none"));
            return true;
        }
    }

    static final class SubflowNodeBehaviour extends NodeBehaviour {

        private final InstanceLifecycle instances;

        SubflowNodeBehaviour(InstanceLifecycle instances) {
            this.instances = instances;
        }

        @Override
        boolean advance(Step s) {
            Tx tx = s.tx(); Rows.Instance inst = s.inst(); Rows.Token t = s.token();
            Node node = s.node(); long now = s.now();
            Rows.TokenStatus before = TokenState.parkAwaiting(tx, t, NodeKind.SUB_WORKFLOW,
                    node.activity(), 0, now);
            String childId;
            try {
                childId = instances.start(tx, node.activity(), null,
                        Scopes.dispatchContext(inst, t), "sub:" + t.id, t.id);
            } catch (EngineException ex) {
                instances.fail(tx, inst, "sub-workflow '" + node.activity() + "': " + ex.getMessage(), now);
                return false;
            }
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (SUB_WORKFLOW) " + before + " -> AWAITING child " + childId);
            return true;
        }
    }

    static final class EndNodeBehaviour extends NodeBehaviour {

        private final InstanceLifecycle instances;

        EndNodeBehaviour(InstanceLifecycle instances) {
            this.instances = instances;
        }

        @Override
        boolean advance(Step s) {
            Tx tx = s.tx(); Rows.Instance inst = s.inst(); Rows.Token t = s.token();
            Node node = s.node(); Deque<Rows.Token> work = s.work(); long now = s.now();
            Rows.TokenStatus before = TokenState.spend(tx, t, NodeKind.END, now);
            if (!node.success()) {
                LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                        + node.name() + " (END) " + before + " -> DONE, unsuccessful end -> failing instance");
                instances.fail(tx, inst, node.reason() == null ? "terminated" : node.reason(), now);
                return false;
            }
            boolean anyActive = TokenLifecycle.anyActive(tx, inst.id);
            if (anyActive || !work.isEmpty()) {
                LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                        + node.name() + " (END) " + before + " -> DONE, other tokens still active");
                return true;
            }
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (END) " + before + " -> DONE, no tokens remain -> instance COMPLETED"
                    + (node.reason() != null ? " (" + node.reason() + ")" : ""));
            instances.complete(tx, inst, node.reason(), now);
            return true;
        }
    }
}