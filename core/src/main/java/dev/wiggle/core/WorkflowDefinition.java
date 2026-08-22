package dev.wiggle.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * An immutable, compiled workflow graph. The {@code version} is a content hash, so
 * registering the same topology twice is idempotent and running instances always
 * keep executing the exact graph they started on.
 */
public record WorkflowDefinition(String name, int version, String startNode,
                                 Map<String, Node> nodes, Set<String> queues, ExecutionMode executionMode,
                                 Set<String> checkpoints) {

    public WorkflowDefinition {
        nodes = Map.copyOf(nodes);
        queues = Set.copyOf(queues);
        executionMode = executionMode == null ? ExecutionMode.DEFAULT : executionMode;
        checkpoints = checkpoints == null ? Set.of() : Set.copyOf(checkpoints);
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

    /** Stable positive hash over the topology, ignoring the version field itself. */
    public static int contentVersion(String name, String startNode, Collection<Node> nodes,
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
        int v = ((digest[0] & 0x7f) << 24) | ((digest[1] & 0xff) << 16)
                | ((digest[2] & 0xff) << 8) | (digest[3] & 0xff);
        return v == 0 ? 1 : v;
    }
}
