package dev.wiggle.core;

import java.util.*;

/**
 * One vertex of the compiled workflow graph. A single record with nullable slots
 * keeps the wire format flat and the engine's switch exhaustive on {@link NodeKind}.
 */
public record Node(String id, NodeKind kind, String name, String activity, String queue,
                   RetryPolicy retry, long sleepMillis, String next, String altNext,
                   List<String> branches, int expected, boolean success, String reason) {

    public static Node task(String id, String name, String activity, String queue, RetryPolicy retry) {
        return new Node(id, NodeKind.TASK, name, activity, queue, retry, 0, null, null, List.of(), 0, false, null);
    }

    public static Node predicate(String id, String name, String activity, String queue, RetryPolicy retry) {
        return new Node(id, NodeKind.PREDICATE, name, activity, queue, retry, 0, null, null, List.of(), 0, false, null);
    }

    public static Node sleep(String id, String name, long millis) {
        return new Node(id, NodeKind.SLEEP, name, null, null, null, millis, null, null, List.of(), 0, false, null);
    }

    public static Node fork(String id, String name) {
        return new Node(id, NodeKind.FORK, name, null, null, null, 0, null, null, List.of(), 0, false, null);
    }

    public static Node join(String id, String name, int expected) {
        return new Node(id, NodeKind.JOIN, name, null, null, null, 0, null, null, List.of(), expected, false, null);
    }

    public static Node end(String id, boolean success, String reason) {
        return new Node(id, NodeKind.END, "end", null, null, null, 0, null, null, List.of(), 0, success, reason);
    }

    public Node withNext(String n) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, n, altNext, branches, expected, success, reason);
    }

    public Node withAltNext(String n) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, next, n, branches, expected, success, reason);
    }

    public Node withBranches(List<String> b) {
        return new Node(id, kind, name, activity, queue, retry, sleepMillis, next, altNext, List.copyOf(b), expected, success, reason);
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
        return m;
    }

    public static Node fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        List<String> branches = new ArrayList<>();
        for (Object b : Json.asArray(m.get("branches"))) branches.add(String.valueOf(b));
        return new Node(
                Json.reqStr(m, "id"),
                NodeKind.valueOf(Json.reqStr(m, "kind")),
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
                Json.str(m, "reason", null));
    }
}
