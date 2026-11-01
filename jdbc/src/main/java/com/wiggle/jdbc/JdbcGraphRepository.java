package com.wiggle.jdbc;

import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RetryPolicy;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.GraphStore;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** {@link GraphStore} on {@code wf_definition}, {@code wf_graph_node} and {@code wf_graph_edge}. */
final class JdbcGraphRepository extends JdbcRepository implements GraphStore {

    JdbcGraphRepository(JdbcStorage owner) {
        super(owner);
    }

    private record Edge(String to, String condition, int ordinal) { }

    /** Flattens a node's typed successors into ordered edge rows; the inverse of {@link #assemble}. */
    private static List<Edge> edgesOf(Node n) {
        List<Edge> out = new ArrayList<>();
        switch (n.kind()) {
            case PREDICATE -> {
                if (n.next() != null) out.add(new Edge(n.next(), "true", 0));
                if (n.altNext() != null) out.add(new Edge(n.altNext(), "false", 1));
            }
            case FORK -> {
                int i = 0;
                for (String b : n.branches()) out.add(new Edge(b, "branch", i++));
            }
            case DYN_FORK -> {
                out.add(new Edge(n.branches().getFirst(), "branch", 0));
                if (n.next() != null) out.add(new Edge(n.next(), null, 1));
            }
            case SIGNAL -> {
                if (n.next() != null) out.add(new Edge(n.next(), null, 0));
                if (n.altNext() != null) out.add(new Edge(n.altNext(), "escalate", 1));
            }
            default -> {
                if (n.next() != null) out.add(new Edge(n.next(), null, 0));
            }
        }
        return out;
    }

    @Override public void putDefinition(String name, int version, String json,
                                        String fingerprint, String fingerprintAlgo) {
        // insertIgnore rather than DELETE-then-INSERT: it leaves no window in which two nodes
        // registering the same new version collide on the primary key. The isDuplicateKey catch
        // covers a backend whose ignore is not inline.
        try {
            jdbc().update(dialect.insertIgnore("INSERT INTO wf_definition "
                            + "(name,version,body,registered_at,fingerprint,fingerprint_algo) VALUES "
                            + "(:name,:version,:body,:registeredAt,:fingerprint,:algo)"),
                    params()
                            .text("name", name)
                            .add("version", version)
                            .text("body", json)
                            .add("registeredAt", System.currentTimeMillis())
                            .text("fingerprint", fingerprint)
                            .text("algo", fingerprintAlgo));
        } catch (RuntimeException e) {
            if (!isDuplicateKey(e)) throw e;
        }
    }

    @Override public void replaceDefinition(String name, int version, String json,
                                            String fingerprint, String fingerprintAlgo) {
        jdbc().update("UPDATE wf_definition SET body=:body, registered_at=:registeredAt, "
                        + "fingerprint=:fingerprint, fingerprint_algo=:algo "
                        + "WHERE name=:name AND version=:version",
                params()
                        .text("body", json)
                        .add("registeredAt", System.currentTimeMillis())
                        .text("fingerprint", fingerprint)
                        .text("algo", fingerprintAlgo)
                        .text("name", name)
                        .add("version", version));
    }

    @Override public Optional<StoredFingerprint> definitionFingerprint(String name, int version) {
        return first(jdbc().query("SELECT fingerprint, fingerprint_algo FROM wf_definition "
                        + "WHERE name=:name AND version=:version FOR UPDATE",
                params().text("name", name).add("version", version),
                (rs, row) -> new StoredFingerprint(rs.getString("fingerprint"), rs.getString("fingerprint_algo"))));
    }

    @Override public void putGraph(WorkflowDefinition def) {
        // The registry deletes these rows before a replacement, so anything still here is the
        // same graph: a no-op, even when two nodes register it at once.
        if (graphExists(def.name(), def.version())) return;
        List<SqlParameterSource> nodes = new ArrayList<>();
        List<SqlParameterSource> edges = new ArrayList<>();
        for (Node n : def.nodes().values()) {
            nodes.add(params()
                    .text("workflow", def.name())
                    .add("version", def.version())
                    .text("nodeId", n.id())
                    .text("kind", n.kind().name())
                    .text("name", n.name())
                    .text("activity", n.activity())
                    .text("queue", n.queue())
                    .text("retry", n.retry() == null ? null : Json.write(n.retry().toJson()))
                    .add("sleepMillis", n.sleepMillis())
                    .add("expected", n.expected())
                    .add("success", n.success() ? 1 : 0)
                    .text("reason", n.reason())
                    .add("isStart", n.id().equals(def.startNode()) ? 1 : 0)
                    .text("itemsKey", n.itemsKey())
                    .text("itemKey", n.itemKey())
                    .add("loopBudget", n.loopBudget())
                    .add("compensable", n.compensable() ? 1 : 0)
                    .text("armNames", n.armNames().isEmpty() ? null : Json.write(n.armNames()))
                    .text("collectKey", n.collectKey()));
            for (Edge e : edgesOf(n)) {
                edges.add(params()
                        .text("workflow", def.name())
                        .add("version", def.version())
                        .text("from", n.id())
                        .text("to", e.to)
                        .text("cond", e.condition)
                        .add("ordinal", e.ordinal));
            }
        }
        try {
            jdbc().batchUpdate(dialect.insertIgnore("INSERT INTO wf_graph_node "
                    + "(workflow,version,node_id,kind,name,activity,queue,retry_json,sleep_millis,expected,"
                    + "success,reason,is_start,items_key,item_key,loop_budget,compensable,arm_names,collect_key) "
                    + "VALUES (:workflow,:version,:nodeId,:kind,:name,:activity,:queue,:retry,:sleepMillis,"
                    + ":expected,:success,:reason,:isStart,:itemsKey,:itemKey,:loopBudget,:compensable,"
                    + ":armNames,:collectKey)"), nodes.toArray(SqlParameterSource[]::new));
            if (!edges.isEmpty()) {
                jdbc().batchUpdate(dialect.insertIgnore("INSERT INTO wf_graph_edge "
                        + "(workflow,version,from_node,to_node,cond,ordinal) "
                        + "VALUES (:workflow,:version,:from,:to,:cond,:ordinal)"), edges.toArray(SqlParameterSource[]::new));
            }
        } catch (RuntimeException e) {
            if (!isDuplicateKey(e)) throw e;
        }
    }

    private boolean graphExists(String workflow, int version) {
        return !jdbc().queryForList("SELECT 1 FROM wf_graph_node WHERE workflow=:workflow AND version=:version LIMIT 1",
                params().text("workflow", workflow).add("version", version), Integer.class).isEmpty();
    }

    @Override public void deleteGraph(String workflow, int version) {
        deleteGraphRows("wf_graph_edge", workflow, version);
        deleteGraphRows("wf_graph_node", workflow, version);
    }

    /** Edges before nodes: an edge pointing at a node that is gone is worse than neither. */
    private void deleteGraphRows(String table, String workflow, int version) {
        jdbc().update("DELETE FROM " + table + " WHERE workflow=:workflow AND version=:version",
                params().text("workflow", workflow).add("version", version));
    }

    @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
        return first(jdbc().query("SELECT kind,name,activity,queue,retry_json,sleep_millis,expected,success,"
                        + "reason,items_key,item_key,loop_budget,compensable,arm_names,collect_key "
                        + "FROM wf_graph_node WHERE workflow=:workflow AND version=:version "
                        + "AND node_id=:nodeId",
                params().text("workflow", workflow).add("version", version).text("nodeId", nodeId),
                (rs, row) -> {
                    NodeKind kind = NodeKind.valueOf(rs.getString("kind"));
                    String retryJson = rs.getString("retry_json");
                    RetryPolicy retry = retryJson == null ? null : RetryPolicy.fromJson(Json.parse(retryJson));
                    String itemsKey = rs.getString("items_key");
                    Combine combine = Combine.of(kind, itemsKey, rs.getString("arm_names"), rs.getString("collect_key"));
                    return new Row(kind, rs.getString("name"), rs.getString("activity"), rs.getString("queue"), retry,
                            rs.getLong("sleep_millis"), rs.getInt("expected"), rs.getInt("success") != 0,
                            rs.getString("reason"), combine.itemsKey(), rs.getString("item_key"),
                            rs.getInt("loop_budget"),   // NULL -> 0 (not a loop)
                            rs.getInt("compensable") != 0, combine);
                })).map(r -> assemble(workflow, version, nodeId, r));
    }

    /** A node's own row, before its edges are folded in. */
    private record Row(NodeKind kind, String name, String activity, String queue, RetryPolicy retry, long sleep,
                       int expected, boolean success, String reason, String itemsKey, String itemKey, int loopBudget,
                       boolean compensable, Combine combine) { }

    /**
     * A combine node's shape as stored. Rows written before {@code combine-columns} carry it as
     * JSON in {@code items_key}; this is the one place that still decodes that form.
     */
    private record Combine(List<String> armNames, String collectKey, String itemsKey) {
        static Combine of(NodeKind kind, String itemsKey, String armNamesJson, String collectKey) {
            if (armNamesJson != null) {
                return new Combine(Json.asArray(Json.parse(armNamesJson)).stream()
                        .map(String::valueOf).toList(), collectKey, itemsKey);
            }
            if (collectKey != null) return new Combine(List.of(), collectKey, itemsKey);
            if (kind != NodeKind.TASK || itemsKey == null) return new Combine(List.of(), null, itemsKey);
            Object legacy = Json.parse(itemsKey);
            if (legacy instanceof String s) return new Combine(List.of(), s, null);
            return new Combine(Json.asArray(legacy).stream().map(String::valueOf).toList(), null, null);
        }
    }

    /** Reads a node's outgoing edges and folds them back into the node's typed next/altNext/branches. */
    private Node assemble(String workflow, int version, String id, Row r) {
        EdgeTargets targets = new EdgeTargets(r.kind());
        jdbc().query("SELECT to_node,cond FROM wf_graph_edge "
                        + "WHERE workflow=:workflow AND version=:version AND from_node=:from ORDER BY ordinal",
                params().text("workflow", workflow).add("version", version).text("from", id),
                rs -> { targets.absorb(rs.getString("to_node"), rs.getString("cond")); });
        Node n = new Node(id, r.kind(), r.name(), r.activity(), r.queue(), r.retry(), r.sleep(), targets.next,
                targets.altNext, List.copyOf(targets.branches), r.expected(), r.success(), r.reason(), r.itemsKey(),
                r.itemKey(), r.loopBudget(), false, r.combine().armNames(), r.combine().collectKey());
        return r.compensable() ? n.withCompensable() : n;
    }

    /** Folds edge rows back into a node's typed successor slots (the inverse of {@code edgesOf}). */
    private static final class EdgeTargets {
        private final NodeKind kind;
        String next;
        String altNext;
        final List<String> branches = new ArrayList<>();

        EdgeTargets(NodeKind kind) { this.kind = kind; }

        void absorb(String to, String cond) {
            if (kind == NodeKind.FORK || (kind == NodeKind.DYN_FORK && "branch".equals(cond))) {
                branches.add(to);
            } else if (isAltEdge(cond)) {
                altNext = to;
            } else {
                next = to;
            }
        }

        private boolean isAltEdge(String cond) {
            return (kind == NodeKind.PREDICATE && "false".equals(cond))
                    || (kind == NodeKind.SIGNAL && "escalate".equals(cond));
        }
    }

    @Override public Optional<String> graphStartNode(String workflow, int version) {
        return first(jdbc().queryForList("SELECT node_id FROM wf_graph_node "
                        + "WHERE workflow=:workflow AND version=:version AND is_start=1",
                params().text("workflow", workflow).add("version", version), String.class));
    }

    @Override public int graphNodeCount(String workflow, int version) {
        return jdbc().queryForObject("SELECT COUNT(*) FROM wf_graph_node WHERE workflow=:workflow AND version=:version",
                params().text("workflow", workflow).add("version", version), Integer.class);
    }

    @Override public Optional<String> definition(String name, int version) {
        return first(jdbc().queryForList("SELECT body FROM wf_definition WHERE name=:name AND version=:version",
                params().text("name", name).add("version", version), String.class));
    }

    @Override public Optional<Integer> latestVersion(String name) {
        // MAX over no rows is a row holding NULL, not an empty result, so the Optional comes from the value.
        return Optional.ofNullable(jdbc().queryForObject("SELECT MAX(version) FROM wf_definition WHERE name=:name",
                params().text("name", name), Integer.class));
    }

    @Override public List<String> definitionNames() {
        return jdbc().queryForList("SELECT DISTINCT name FROM wf_definition ORDER BY name", params(), String.class);
    }

    @Override public List<Integer> definitionVersions(String name) {
        return jdbc().queryForList("SELECT version FROM wf_definition WHERE name=:name ORDER BY version",
                params().text("name", name), Integer.class);
    }
}
