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
                                 Map<String, Node> nodes, Set<String> queues) {

    public WorkflowDefinition {
        nodes = Map.copyOf(nodes);
        queues = Set.copyOf(queues);
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
        return new WorkflowDefinition(Json.reqStr(m, "name"), (int) Json.num(m, "version", 0),
                Json.reqStr(m, "startNode"), nodes, queues);
    }

    /** Stable positive hash over the topology, ignoring the version field itself. */
    public static int contentVersion(String name, String startNode, Collection<Node> nodes) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("name", name);
        m.put("startNode", startNode);
        List<Object> ns = new ArrayList<>();
        nodes.stream().sorted(Comparator.comparing(Node::id)).forEach(n -> ns.add(n.toJson()));
        m.put("nodes", ns);
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
