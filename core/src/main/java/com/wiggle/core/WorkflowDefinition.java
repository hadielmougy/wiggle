package com.wiggle.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * An immutable, compiled workflow graph. The {@code version} is declared by the author and is
 * positive; a published version is immutable, which the engine enforces by comparing
 * {@link #fingerprint()} on re-registration. Running instances always keep executing the exact
 * graph they started on.
 */
public record WorkflowDefinition(String name, int version, String startNode,
                                 Map<String, Node> nodes, Set<String> queues, ExecutionMode executionMode,
                                 Set<String> checkpoints) {

    public WorkflowDefinition {
        if (version <= 0) {
            throw new IllegalArgumentException("workflow '" + name + "' needs a positive version, got " + version);
        }
        executionMode = executionMode == null ? ExecutionMode.DEFAULT : executionMode;
    }

    /** Convenience constructor with no checkpoints. */
    public WorkflowDefinition(String name, int version, String startNode,
                              Map<String, Node> nodes, Set<String> queues, ExecutionMode executionMode) {
        this(name, version, startNode, nodes, queues, executionMode, Set.of());
    }

    /** Legacy constructor defaulting to {@link ExecutionMode#DEFAULT} with no checkpoints. */
    public WorkflowDefinition(String name, int version, String startNode,
                              Map<String, Node> nodes, Set<String> queues) {
        this(name, version, startNode, nodes, queues, ExecutionMode.DEFAULT, Set.of());
    }

    public Node node(String id) {
        Node n = nodes.get(id);
        if (n == null) throw new IllegalStateException("unknown node '" + id + "' in workflow " + name);
        return n;
    }

    /** The queues this graph's worker-dispatched nodes are routed to. */
    public Set<String> workerQueues() {
        Set<String> qs = new LinkedHashSet<>();
        for (Node n : nodes.values()) {
            if (n.isWorkerDispatched() && n.queue() != null) qs.add(n.queue());
        }
        return qs;
    }

    public String key() {
        return name + ":" + version;
    }

    public Map<String, Object> toJson() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("version", (long) version);
        m.put("startNode", startNode);
        List<Object> ns = new ArrayList<>();
        nodes.keySet().stream().sorted().forEach(k -> ns.add(nodes.get(k).toJson()));
        m.put("nodes", ns);
        m.put("queues", new ArrayList<>(new TreeSet<>(queues)));
        m.put("executionMode", executionMode.name());
        if (!checkpoints.isEmpty()) m.put("checkpoints", new ArrayList<>(new TreeSet<>(checkpoints)));
        return m;
    }

    public static WorkflowDefinition fromJson(Object o) {
        Map<String, Object> m = Json.asObject(o);
        Map<String, Node> nodes = new LinkedHashMap<>();
        for (Object n : Json.asArray(m.get("nodes"))) {
            Node node = Node.fromJson(n);
            nodes.put(node.id(), node);
        }
        Set<String> queues = new LinkedHashSet<>();
        for (Object q : Json.asArray(m.get("queues"))) queues.add(String.valueOf(q));
        ExecutionMode mode = ExecutionMode.valueOf(Json.str(m, "executionMode", ExecutionMode.DEFAULT.name()));
        Set<String> checkpoints = new LinkedHashSet<>();
        for (Object c : Json.asArray(m.get("checkpoints"))) checkpoints.add(String.valueOf(c));
        return new WorkflowDefinition(Json.reqStr(m, "name"), (int) Json.num(m, "version", 0),
                Json.reqStr(m, "startNode"), nodes, queues, mode, checkpoints);
    }

    /** The current fingerprint algorithm, stored beside a fingerprint so a later change to the
     *  topology's serialised shape cannot be mistaken for a changed graph. */
    public static final String FINGERPRINT_ALGO = "sha256-canonical-v1";

    /** This definition's {@link #fingerprint(String, String, Collection, ExecutionMode, Set) fingerprint}. */
    public String fingerprint() {
        return fingerprint(name, startNode, nodes.values(), executionMode, checkpoints);
    }

    public int numberOfNodes() {
        return nodes.size();
    }

    /**
     * A stable digest of the topology, ignoring the version field itself. Two definitions with the
     * same fingerprint are the same graph; the engine uses it to tell an idempotent re-registration
     * from an attempt to redefine a version that instances are already running on.
     *
     * <p>It is not the version. The version is declared by the author, so it stays readable and
     * ordered; this only answers "is this the same graph as the one already stored".
     */
    public static String fingerprint(String name, String startNode, Collection<Node> nodes,
                                     ExecutionMode executionMode, Set<String> checkpoints) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("startNode", startNode);
        List<Object> ns = new ArrayList<>();
        nodes.stream().sorted(Comparator.comparing(Node::id)).forEach(n -> ns.add(n.toJson()));
        m.put("nodes", ns);
        m.put("executionMode", (executionMode == null ? ExecutionMode.DEFAULT : executionMode).name());
        m.put("checkpoints", new ArrayList<>(new TreeSet<>(checkpoints == null ? Set.of() : checkpoints)));
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256")
                    .digest(Json.canonical(m).getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        StringBuilder hex = new StringBuilder(64);
        for (byte b : digest) hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        return hex.toString();
    }
}
