package com.wiggle.jdbc;

import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.server.store.DynamicNodeRepository;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.List;
import java.util.Optional;

/** {@link DynamicNodeRepository} on {@code wf_dyn_node}. */
final class JdbcDynamicNodeRepository extends JdbcRepository implements DynamicNodeRepository {

    JdbcDynamicNodeRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public void insertDynNodes(String instanceId, List<Node> nodes) {
        if (nodes.isEmpty()) return;
        SqlParameterSource[] rows = nodes.stream()
                .map(n -> params().text("nodeId", n.id()).text("instanceId", instanceId)
                        .text("body", Json.write(n.toJson())))
                .toArray(SqlParameterSource[]::new);
        jdbc().batchUpdate("INSERT INTO wf_dyn_node (node_id,instance_id,body) VALUES (:nodeId,:instanceId,:body)", rows);
    }

    @Override public Optional<Node> dynNode(String nodeId) {
        return first(jdbc().queryForList("SELECT body FROM wf_dyn_node WHERE node_id=:id",
                        params().text("id", nodeId), String.class))
                .map(body -> Node.fromJson(Json.parse(body)));
    }

    @Override public long dynNodeCount(String instanceId) {
        return jdbc().queryForObject("SELECT COUNT(*) FROM wf_dyn_node WHERE instance_id=:id",
                params().text("id", instanceId), Long.class);
    }
}
