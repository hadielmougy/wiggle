package com.wiggle.server.engine;

import com.wiggle.core.GraphTraversal;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.ScratchKeys;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Rows.TokenStatus;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * How each node kind advances a token ({@link #advance}), and — for the worker-parked kinds,
 * TASK and PREDICATE — how a reported result routes to the successor and counts against a loop
 * budget. One constant per {@link NodeKind}, matched by name.
 */
enum NodeBehaviours {

    TASK {
        @Override boolean advance(WorkflowEngine e, Step s) { return parkReady(e, s); }

        @Override String route(Instance inst, Token t, Node node, Object result) {
            WorkflowEngine.applyStepResult(inst, t, node, result);
            LOG.log(System.Logger.Level.DEBUG, () -> "complete: task " + node.name()
                    + " of instance " + inst.id + " done -> " + node.next());
            return node.next();
        }

        @Override String routeReported(Instance inst, Token t, Node node, WorkflowEngine.StepInput step) {
            WorkflowEngine.applyStepResult(inst, t, node, step.merge());
            return node.next();
        }
    },

    PREDICATE {
        @Override boolean advance(WorkflowEngine e, Step s) { return parkReady(e, s); }

        @Override String route(Instance inst, Token t, Node node, Object result) {
            boolean value = predicateValue(result);
            String next = GraphTraversal.successor(node, value);
            LOG.log(System.Logger.Level.DEBUG, () -> "complete: predicate " + node.name()
                    + " of instance " + inst.id + " evaluated " + value + " -> " + next);
            return next;
        }

        @Override String routeReported(Instance inst, Token t, Node node, WorkflowEngine.StepInput step) {
            boolean value = step.predicateValue() != null && step.predicateValue();
            return GraphTraversal.successor(node, value);
        }

        @Override String overrunAfter(WorkflowEngine e, Token t, Node node, Object result) {
            return tickLoopBudget(e, t, node, predicateValue(result));
        }

        @Override String overrunReported(WorkflowEngine e, Token t, Node node, WorkflowEngine.StepInput step) {
            return tickLoopBudget(e, t, node, step.predicateValue() != null && step.predicateValue());
        }
    },

    SLEEP {
        @Override boolean advance(WorkflowEngine e, Step s) {
            Tx tx = s.tx(); Instance inst = s.inst(); Token t = s.token();
            Node node = s.node(); long now = s.now();
            TokenStatus before = t.status;
            t.status = TokenStatus.WAITING;
            t.kind = NodeKind.SLEEP;
            t.availableAt = now + node.sleepMillis();
            t.updatedAt = now;
            WorkflowEngine.saveToken(tx, t);
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (SLEEP) " + before + " -> WAITING until " + t.availableAt
                    + " (" + node.sleepMillis() + "ms)");
            return true;
        }
    },

    /** Parks until the named signal arrives. No worker leases an AWAITING token; a positive
     *  availableAt is the (optional) deadline the leader sweeps. */
    SIGNAL {
        @Override boolean advance(WorkflowEngine e, Step s) {
            Tx tx = s.tx(); Instance inst = s.inst(); Token t = s.token();
            Node node = s.node(); long now = s.now();
            TokenStatus before = t.status;
            t.status = TokenStatus.AWAITING;
            t.kind = NodeKind.SIGNAL;
            t.activity = node.name();     // the signal's name, matched by signal()
            t.availableAt = node.sleepMillis() > 0 ? now + node.sleepMillis() : 0;
            t.updatedAt = now;
            WorkflowEngine.saveToken(tx, t);
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (SIGNAL) " + before + " -> AWAITING, deadline="
                    + (t.availableAt > 0 ? t.availableAt : "none"));
            return true;
        }
    },

    /** Starts a child instance of the workflow named by the node and parks this token until the
     *  child reaches a terminal state (notifyParent). The child's input is the parent's context
     *  (with any branch payload overlaid); an unregistered child workflow fails the parent. */
    SUB_WORKFLOW {
        @Override boolean advance(WorkflowEngine e, Step s) {
            Tx tx = s.tx(); Instance inst = s.inst(); Token t = s.token();
            Node node = s.node(); long now = s.now();
            TokenStatus before = t.status;
            t.status = TokenStatus.AWAITING;
            t.kind = NodeKind.SUB_WORKFLOW;
            t.activity = node.activity();   // the child workflow's name
            t.availableAt = 0;
            t.updatedAt = now;
            WorkflowEngine.saveToken(tx, t);
            String childId;
            try {
                childId = e.startInTx(tx, node.activity(), null,
                        WorkflowEngine.dispatchContext(inst, t), "sub:" + t.id, t.id);
            } catch (EngineException ex) {
                e.failInstance(tx, inst, "sub-workflow '" + node.activity() + "': " + ex.getMessage(), now);
                return false;
            }
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (SUB_WORKFLOW) " + before + " -> AWAITING child " + childId);
            return true;
        }
    },

    FORK {
        @Override boolean advance(WorkflowEngine e, Step s) {
            Tx tx = s.tx(); Instance inst = s.inst(); Token t = s.token();
            Node node = s.node(); Deque<Token> work = s.work(); long now = s.now();
            TokenStatus before = t.status;
            t.status = TokenStatus.DONE;
            t.kind = NodeKind.FORK;
            t.updatedAt = now;
            WorkflowEngine.saveToken(tx, t);
            String group = t.id;   // unique per fork execution; the join finds the fork token by it
            String childStack = t.pushJoinStack(group);
            List<String> starts = node.branches();
            for (int i = 0; i < starts.size(); i++) {
                // Tag each branch with its arm index and give it a private context overlay, so its
                // writes stay isolated from its siblings and the shared context until the combine.
                Token child = WorkflowEngine.newToken(inst, starts.get(i), childStack,
                        branchScope(t.payloadJson, i), now);
                work.push(child);
            }
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (FORK) " + before + " -> DONE, spawned " + node.branches().size()
                    + " branch(es) in group " + group + ": " + node.branches());
            return true;
        }
    },

    /** Runtime fan-out: one child per element of the list at the node's {@code itemsKey}, each
     *  carrying its element (and index) as a branch-scoped payload. The join group encodes the
     *  width, since a dynamic join's expected count varies per execution. An empty or missing
     *  list skips straight past the paired join; a non-list value fails the instance. */
    DYN_FORK {
        @Override boolean advance(WorkflowEngine e, Step s) {
            Tx tx = s.tx(); Instance inst = s.inst(); Token t = s.token();
            Node node = s.node(); Deque<Token> work = s.work(); long now = s.now();
            Object items = Json.parseObject(inst.contextJson).get(node.itemsKey());
            if (items != null && !(items instanceof List) && !(items instanceof Map)) {
                e.failInstance(tx, inst, "forEach '" + node.name() + "': context key '" + node.itemsKey()
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
            TokenStatus before = t.status;
            t.status = TokenStatus.DONE;
            t.kind = NodeKind.DYN_FORK;
            t.updatedAt = now;
            WorkflowEngine.saveToken(tx, t);
            if (elements.isEmpty()) {
                // Nothing to fan out over: continue past the paired join AND its combine (there is
                // nothing to collect, so the combine is skipped and the context is untouched).
                LazyGraph def = e.def(tx, inst);
                Node join = def.node(node.next());
                Node after = def.node(join.next());
                String next = WorkflowEngine.isCombineNode(after) ? after.next() : join.next();
                Token cont = WorkflowEngine.newToken(inst, next, t.joinStack, t.payloadJson, now);
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
                Token child = WorkflowEngine.newToken(inst, branchStart, childStack,
                        itemPayload(t, node, elements.get(i), i, key), now);
                work.push(child);
            }
            int n = elements.size();
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (DYN_FORK) " + before + " -> DONE, spawned " + n
                    + " isolated branch(es) over '" + node.itemsKey() + "' in group " + group);
            return true;
        }
    },

    JOIN {
        @Override boolean advance(WorkflowEngine e, Step s) {
            Tx tx = s.tx(); LazyGraph def = s.def(); Instance inst = s.inst(); Token t = s.token();
            Node node = s.node(); Deque<Token> work = s.work(); long now = s.now();
            String group = t.currentJoinGroup();
            int expected = expectedAt(node, group);
            TokenStatus before = t.status;
            t.status = TokenStatus.JOINED;
            t.kind = NodeKind.JOIN;
            t.updatedAt = now;
            WorkflowEngine.saveToken(tx, t);
            List<Token> atBarrier = joinedAtBarrier(tx, inst, node, group);
            if (atBarrier.size() < expected) {
                LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                        + node.name() + " (JOIN) " + before + " -> JOINED, waiting on barrier " + group
                        + " (" + atBarrier.size() + "/" + expected + ")");
                return true;
            }
            consumeBarrier(tx, atBarrier, now);
            // Restore the payload the branches started from, so nesting scopes correctly, then (for a
            // combine fork) stage each isolated branch's result under its arm name for the aggregator.
            String contPayload = combinePayload(def, node, atBarrier, forkPayload(tx, group));
            Token cont = WorkflowEngine.newToken(inst, node.next(), t.popJoinStack(), contPayload, now);
            work.push(cont);
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (JOIN) " + before + " -> JOINED, barrier " + group
                    + " satisfied (" + atBarrier.size() + "/" + expected + ") -> " + node.next());
            return true;
        }
    },

    END {
        @Override boolean advance(WorkflowEngine e, Step s) {
            Tx tx = s.tx(); Instance inst = s.inst(); Token t = s.token();
            Node node = s.node(); Deque<Token> work = s.work(); long now = s.now();
            TokenStatus before = t.status;
            t.status = TokenStatus.DONE;
            t.kind = NodeKind.END;
            t.updatedAt = now;
            WorkflowEngine.saveToken(tx, t);
            if (!node.success()) {
                LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                        + node.name() + " (END) " + before + " -> DONE, unsuccessful end -> failing instance");
                e.failInstance(tx, inst, node.reason() == null ? "terminated" : node.reason(), now);
                return false;
            }
            boolean anyActive = tx.tokensOf(inst.id).stream().anyMatch(Token::isActive);
            if (anyActive || !work.isEmpty()) {
                LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                        + node.name() + " (END) " + before + " -> DONE, other tokens still active");
                return true;
            }
            inst.status = Rows.InstanceStatus.COMPLETED;
            inst.terminationReason = node.reason();
            inst.updatedAt = now;
            tx.updateInstance(inst);
            LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                    + node.name() + " (END) " + before + " -> DONE, no tokens remain -> instance COMPLETED"
                    + (node.reason() != null ? " (" + node.reason() + ")" : ""));
            e.notifyParent(tx, inst, now);
            return true;
        }
    };

    private static final System.Logger LOG = System.getLogger(NodeBehaviours.class.getName());

    static NodeBehaviours of(NodeKind kind) {
        return valueOf(kind.name());
    }

    static { for (NodeKind k : NodeKind.values()) of(k); }   // every kind has a behaviour, or fail at load

    /** Advances {@code s}'s token; false stops the drive pass (the instance failed). */
    abstract boolean advance(WorkflowEngine e, Step s);

    /** Merges a task result (or routes a predicate) and returns the successor node id. */
    String route(Instance inst, Token t, Node node, Object result) {
        throw new IllegalStateException("node kind " + name() + " is not worker-completed");
    }

    String routeReported(Instance inst, Token t, Node node, WorkflowEngine.StepInput step) {
        throw new IllegalStateException("node kind " + name() + " is not worker-reported");
    }

    /** The loop-budget failure message after this completion, or null. Non-guards never overrun. */
    String overrunAfter(WorkflowEngine e, Token t, Node node, Object result) {
        return null;
    }

    String overrunReported(WorkflowEngine e, Token t, Node node, WorkflowEngine.StepInput step) {
        return null;
    }

    private static boolean parkReady(WorkflowEngine e, Step s) {
        Tx tx = s.tx(); Instance inst = s.inst(); Token t = s.token();
        Node node = s.node(); long now = s.now();
        TokenStatus before = t.status;
        t.status = TokenStatus.READY;
        t.kind = node.kind();
        t.activity = node.activity();
        t.queue = node.queue();
        t.availableAt = now;
        t.updatedAt = now;
        WorkflowEngine.saveToken(tx, t);
        e.wakeQueue(node.queue());   // signalled post-commit by tx()/txVoid()
        LOG.log(System.Logger.Level.DEBUG, () -> "drive: " + inst.id + " token " + t.id + " at "
                + node.name() + " (" + node.kind() + ") " + before + " -> READY, queue=" + node.queue());
        return true;
    }

    private static boolean predicateValue(Object result) {
        if (result instanceof Boolean b) return b;
        if (result instanceof Map<?, ?> m && m.get("value") instanceof Boolean b) return b;
        throw EngineException.badRequest("predicate result must be a boolean or {\"value\": <boolean>}");
    }

    /**
     * Counts a loop guard's true evaluation against its budget. Returns the failure message when
     * the budget is exhausted (the caller fails the instance and mints no continuation); otherwise
     * records the incremented count in the token's payload — the continuation inherits it, so the
     * count survives the whole loop. Non-loop guards (loopBudget 0) are untouched.
     */
    private static String tickLoopBudget(WorkflowEngine e, Token t, Node node, boolean value) {
        if (node.kind() != NodeKind.PREDICATE || !value || node.loopBudget() == 0) return null;
        long budget = node.loopBudget() > 0 ? node.loopBudget() : e.loopMaxIterations;
        Map<String, Object> payload = t.payloadJson == null
                ? new LinkedHashMap<>() : Json.parseObject(t.payloadJson);
        @SuppressWarnings("unchecked")
        Map<String, Object> counts = (Map<String, Object>) payload
                .computeIfAbsent(WorkflowEngine.LOOP_COUNTS, k -> new LinkedHashMap<String, Object>());
        long n = ((Number) counts.getOrDefault(node.id(), 0L)).longValue() + 1;
        if (n > budget) {
            return "loop '" + node.name() + "' exceeded its budget of " + budget
                    + " iterations (raise it with doWhile(name, maxIterations, body) or fix the condition)";
        }
        counts.put(node.id(), n);
        t.payloadJson = Json.write(payload);
        return null;
    }

    /** A static-fork branch's initial payload: the fork token's payload (so nesting keeps the outer
     *  scope) tagged with this branch's arm index, which marks it isolated. */
    private static String branchScope(String basePayload, int armIndex) {
        Map<String, Object> payload = basePayload == null
                ? new LinkedHashMap<>() : Json.parseObject(basePayload);
        payload.put(WorkflowEngine.ARM_IDX, (long) armIndex);
        return Json.write(payload);
    }

    /** The child's payload: the fork token's own payload (nesting) plus the element as the item's
     *  working value — arm-tagged so the item runs ISOLATED. Nothing is injected under user keys;
     *  the element IS the item's context, and the base travels on the activation instead. */
    private static String itemPayload(Token forkToken, Node node, Object item, int index, String mapKey) {
        Map<String, Object> payload = forkToken.payloadJson == null
                ? new LinkedHashMap<>() : Json.parseObject(forkToken.payloadJson);
        payload.put(WorkflowEngine.ARM_IDX, (long) index);
        payload.put(WorkflowEngine.ITEM_VALUE, item);
        if (mapKey != null) payload.put(WorkflowEngine.ITEM_MAP_KEY, mapKey);
        return Json.write(payload);
    }

    /** A static join's width comes from the graph; a dynamic one travels in the group as "#n". */
    private static int expectedAt(Node node, String group) {
        if (node.expected() > 0) return node.expected();
        int hash = group == null ? -1 : group.lastIndexOf('#');
        return hash < 0 ? 1 : Integer.parseInt(group.substring(hash + 1));
    }

    /** The payload the fork token had when it spawned this group, or null for legacy groups. */
    private static String forkPayload(Tx tx, String group) {
        if (group == null) return null;
        int hash = group.lastIndexOf('#');
        String forkTokenId = hash < 0 ? group : group.substring(0, hash);
        return tx.findToken(forkTokenId).map(f -> f.payloadJson).orElse(null);
    }

    private static List<Token> joinedAtBarrier(Tx tx, Instance inst, Node node, String group) {
        return tx.tokensOf(inst.id).stream()
                .filter(x -> x.status == TokenStatus.JOINED)
                .filter(x -> node.id().equals(x.nodeId))
                .filter(x -> Objects.equals(group, x.currentJoinGroup()))
                .toList();
    }

    /** Consumes the barrier: these tokens have served their purpose, and leaving them parked
     *  would keep the instance looking active forever. */
    private static void consumeBarrier(Tx tx, List<Token> atBarrier, long now) {
        for (Token parked : atBarrier) {
            parked.status = TokenStatus.DONE;
            parked.updatedAt = now;
            tx.updateToken(parked);
        }
    }

    /**
     * The continuation payload after a join, staging the branches' results for the mandatory
     * combine. A fork combine (itemsKey = arm-name array) gets each branch's final view staged
     * under its arm name. A forEach combine (itemsKey = a scratch-key string) gets the items'
     * final views COLLECTED — a list ordered by item index, or, when the input was a map, a map
     * keyed like the input — staged under that one scratch key. The arm tags are consumed.
     */
    private static String combinePayload(LazyGraph def, Node joinNode, List<Token> atBarrier, String basePayload) {
        Node agg = def.node(joinNode.next());
        if (!WorkflowEngine.isCombineNode(agg)) return basePayload;
        Map<String, Object> staged = basePayload == null
                ? new LinkedHashMap<>() : Json.parseObject(basePayload);
        String scratch = WorkflowEngine.forEachScratchKey(agg);
        if (scratch == null) {
            List<String> armNames = WorkflowEngine.armNames(agg);
            for (Token bt : atBarrier) {
                Map<String, Object> branch = bt.payloadJson == null
                        ? new LinkedHashMap<>() : Json.parseObject(bt.payloadJson);
                Object idx = branch.remove(WorkflowEngine.ARM_IDX);
                branch.remove(WorkflowEngine.LOOP_COUNTS);
                if (idx == null) continue;   // defensive: a token that never carried an arm tag
                staged.put(ScratchKeys.arm(armNames.get(((Number) idx).intValue())), branch);
            }
            return Json.write(staged);
        }
        // forEach: collect each item's FINAL VALUE, ordered by arm index; key by the source map
        // key when the input was a map. The values are exactly what each item's last step returned.
        TreeMap<Long, Map<String, Object>> ordered = new TreeMap<>();
        boolean mapInput = false;
        for (Token bt : atBarrier) {
            Map<String, Object> payload = bt.payloadJson == null
                    ? new LinkedHashMap<>() : Json.parseObject(bt.payloadJson);
            Object idx = payload.get(WorkflowEngine.ARM_IDX);
            if (idx == null) continue;
            mapInput |= payload.containsKey(WorkflowEngine.ITEM_MAP_KEY);
            ordered.put(((Number) idx).longValue(), payload);
        }
        if (mapInput) {
            Map<String, Object> byKey = new LinkedHashMap<>();
            for (Map<String, Object> payload : ordered.values()) {
                byKey.put(String.valueOf(payload.get(WorkflowEngine.ITEM_MAP_KEY)),
                        payload.get(WorkflowEngine.ITEM_VALUE));
            }
            staged.put(scratch, byKey);
        } else {
            List<Object> values = new ArrayList<>(ordered.size());
            for (Map<String, Object> payload : ordered.values()) values.add(payload.get(WorkflowEngine.ITEM_VALUE));
            staged.put(scratch, values);
        }
        return Json.write(staged);
    }
}
