package com.wiggle.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One vertex of the compiled workflow graph. A single record with nullable slots
 * keeps the wire format flat and the engine's switch exhaustive on {@link NodeKind}.
 */
public record Node(String id, NodeKind kind, String name, String activity, String queue,
                   RetryPolicy retry, long sleepMillis, String next, String altNext,
                   List<String> branches, int expected, boolean success, String reason,
                   String itemsKey, String itemKey, int loopBudget, boolean compensable,
                   List<String> armNames, String collectKey) {
    // itemsKey/itemKey are the DYN_FORK fan-out's context keys. armNames/collectKey mark a combine.

    public Node {
        branches = branches == null ? List.of() : List.copyOf(branches);
        armNames = armNames == null ? List.of() : List.copyOf(armNames);
    }

    /** Back-compat canonical shape: a node that is neither kind of combine. */
    public Node(String id, NodeKind kind, String name, String activity, String queue,
                RetryPolicy retry, long sleepMillis, String next, String altNext,
                List<String> branches, int expected, boolean success, String reason,
                String itemsKey, String itemKey, int loopBudget, boolean compensable) {
        this(id, kind, name, activity, queue, retry, sleepMillis, next, altNext, branches, expected,
                success, reason, itemsKey, itemKey, loopBudget, compensable, List.of(), null);
    }

    /**
     * Whether this task node is the mandatory merge after a join. A fork's combine carries
     * {@link #armNames}; a forEach's carries {@link #collectKey}.
     */
    public boolean isCombine() {
        return kind == NodeKind.TASK && (!armNames.isEmpty() || collectKey != null);
    }

    /** The arm names a fork combine keys its staged inputs by, in fork order. */
    public Node withArmNames(List<String> names) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, next, altNext, branches,
                expected, success, reason, itemsKey, itemKey, loopBudget, compensable, names, collectKey);
    }

    /** The scratch key a forEach combine's collected item results are staged under. */
    public Node withCollectKey(String key) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, next, altNext, branches,
                expected, success, reason, itemsKey, itemKey, loopBudget, compensable, armNames, key);
    }

    public static Node task(String id, String name, String activity, String queue, RetryPolicy retry) {
        return new Node(id, NodeKind.TASK, name, activity, queue, retry, 0, null, null, List.of(), 0, false, null, null, null, 0, false);
    }

    public static Node predicate(String id, String name, String activity, String queue, RetryPolicy retry) {
        return new Node(id, NodeKind.PREDICATE, name, activity, queue, retry, 0, null, null, List.of(), 0, false, null, null, null, 0, false);
    }

    public static Node sleep(String id, String name, long millis) {
        return new Node(id, NodeKind.SLEEP, name, null, null, null, millis, null, null, List.of(), 0, false, null, null, null, 0, false);
    }

    public static Node fork(String id, String name) {
        return new Node(id, NodeKind.FORK, name, null, null, null, 0, null, null, List.of(), 0, false, null, null, null, 0, false);
    }

    /**
     * Runtime fan-out over the list found in the context at {@code itemsKey}; each spawned token
     * runs the single branch template with its element injected under {@code itemKey}. The
     * {@code next} edge points at the paired join (used directly when the list is empty).
     */
    public static Node dynFork(String id, String name, String itemsKey, String itemKey) {
        return new Node(id, NodeKind.DYN_FORK, name, null, null, null, 0, null, null, List.of(), 0, false, null,
                itemsKey, itemKey, 0, false);
    }

    /** {@code expected == 0} marks a dynamic join: the width travels in the join group instead. */
    public static Node join(String id, String name, int expected) {
        return new Node(id, NodeKind.JOIN, name, null, null, null, 0, null, null, List.of(), expected, false, null, null, null, 0, false);
    }

    /**
     * Waits for the signal named {@code name}. {@code deadlineMillis == 0} means no deadline
     * (reuses {@code sleepMillis} for the deadline).
     */
    public static Node signal(String id, String name, long deadlineMillis) {
        return new Node(id, NodeKind.SIGNAL, name, null, null, null, deadlineMillis, null, null, List.of(), 0, false, null, null, null, 0, false);
    }

    /** Runs the workflow named {@code workflow} as a child; reuses {@code activity} for its name. */
    public static Node subWorkflow(String id, String name, String workflow) {
        return new Node(id, NodeKind.SUB_WORKFLOW, name, workflow, null, null, 0, null, null, List.of(), 0, false, null, null, null, 0, false);
    }

    public static Node end(String id, boolean success, String reason) {
        return new Node(id, NodeKind.END, "end", null, null, null, 0, null, null, List.of(), 0, success, reason, null, null, 0, false);
    }

    public Node withNext(String n) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, n, altNext, branches, expected, success, reason, itemsKey, itemKey, loopBudget, compensable, armNames, collectKey);
    }

    public Node withAltNext(String n) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, next, n, branches, expected, success, reason, itemsKey, itemKey, loopBudget, compensable, armNames, collectKey);
    }

    public Node withQueue(String q) {
        return new Node(id, kind, name, activity, q, retry, sleepMillis, next, altNext, branches, expected, success, reason, itemsKey, itemKey, loopBudget, compensable, armNames, collectKey);
    }

    public Node withRetry(RetryPolicy r) {
        return new Node(id, kind, name, activity, queue, r, sleepMillis, next, altNext, branches, expected, success, reason, itemsKey, itemKey, loopBudget, compensable, armNames, collectKey);
    }

    public Node withBranches(List<String> b) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, next, altNext, List.copyOf(b), expected, success, reason, itemsKey, itemKey, loopBudget, compensable, armNames, collectKey);
    }

    /** Marks this step compensable: on instance failure its bound Compensable undo runs in the
     *  reverse pass, fed the input/result snapshots captured at completion. */
    public Node withCompensable() {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, next, altNext,
                branches, expected, success, reason, itemsKey, itemKey, loopBudget, true, armNames, collectKey);
    }

    /** doWhile guards only: max true-evaluations before the instance fails (-1 = engine default). */
    public Node withLoopBudget(int budget) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, next, altNext,
                branches, expected, success, reason, itemsKey, itemKey, budget, compensable, armNames, collectKey);
    }

    public boolean isWorkerDispatched() {
        return kind == NodeKind.TASK || kind == NodeKind.PREDICATE;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", id);
        m.put("kind", kind.name());
        m.put("name", name);
        if (activity != null) m.put("activity", activity);
        if (queue != null) m.put("queue", queue);
        if (retry != null) m.put("retry", retry.toJson());
        if (sleepMillis > 0) m.put("sleepMillis", sleepMillis);
        if (next != null) m.put("next", next);
        if (altNext != null) m.put("altNext", altNext);
        if (!branches.isEmpty()) m.put("branches", branches);
        if (expected > 0) m.put("expected", (long) expected);
        if (kind == NodeKind.END) m.put("success", success);
        if (reason != null) m.put("reason", reason);
        if (itemsKey != null) m.put("itemsKey", itemsKey);
        if (itemKey != null) m.put("itemKey", itemKey);
        if (!armNames.isEmpty()) m.put("armNames", armNames);
        if (collectKey != null) m.put("collectKey", collectKey);
        if (loopBudget != 0) m.put("loopBudget", (long) loopBudget);
        if (compensable) m.put("compensable", true);
        return m;
    }

    public static Node fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        List<String> branches = new ArrayList<>();
        for (Object b : Json.asArray(m.get("branches"))) branches.add(String.valueOf(b));
        NodeKind kind = NodeKind.valueOf(Json.reqStr(m, "kind"));
        String itemsKey = Json.str(m, "itemsKey", null);
        List<String> armNames = new ArrayList<>();
        for (Object a : Json.asArray(m.get("armNames"))) armNames.add(String.valueOf(a));
        String collectKey = Json.str(m, "collectKey", null);
        if (kind == NodeKind.TASK && itemsKey != null && armNames.isEmpty() && collectKey == null) {
            // A graph written before combines had typed fields: the task's itemsKey held either a
            // JSON array of arm names or a JSON string naming the collect key. Decoded once, here.
            Object legacy = Json.parse(itemsKey);
            if (legacy instanceof String s) collectKey = s;
            else for (Object a : Json.asArray(legacy)) armNames.add(String.valueOf(a));
            itemsKey = null;
        }
        return new Node(
                Json.reqStr(m, "id"),
                kind,
                Json.str(m, "name", null),
                Json.str(m, "activity", null),
                Json.str(m, "queue", null),
                RetryPolicy.fromJson(m.get("retry")),
                Json.num(m, "sleepMillis", 0),
                Json.str(m, "next", null),
                Json.str(m, "altNext", null),
                List.copyOf(branches),
                (int) Json.num(m, "expected", 0),
                Json.bool(m, "success", false),
                Json.str(m, "reason", null),
                itemsKey,
                Json.str(m, "itemKey", null),
                (int) Json.num(m, "loopBudget", 0),
                Json.bool(m, "compensable", false),
                List.copyOf(armNames),
                collectKey);
    }
}
