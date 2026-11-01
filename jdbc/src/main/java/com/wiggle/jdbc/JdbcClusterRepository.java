package com.wiggle.jdbc;

import com.wiggle.server.store.ClusterRepository;
import com.wiggle.server.store.Rows.ServerNode;

import java.util.List;

/** {@link ClusterRepository} on {@code wf_node}. */
final class JdbcClusterRepository extends JdbcRepository implements ClusterRepository {

    JdbcClusterRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public void upsertNode(ServerNode n) {
        Params p = params()
                .add("id", n.id)
                .add("name", n.name)
                .add("first", n.firstHeartbeat)
                .add("beat", n.lastHeartbeat)
                .add("workers", n.workers)
                .add("gen", n.topologyGeneration);
        int updated = jdbc().update("UPDATE wf_node SET name=:name,last_heartbeat=:beat,"
                + "workers=:workers,topology_generation=:gen WHERE id=:id", p);
        if (updated > 0) return;
        jdbc().update("INSERT INTO wf_node (id,name,first_heartbeat,last_heartbeat,workers,leader,"
                + "topology_generation) VALUES (:id,:name,:first,:beat,:workers,0,:gen)", p);
    }

    @Override public void deleteNodesOlderThan(long before) {
        jdbc().update("DELETE FROM wf_node WHERE last_heartbeat<:before", params().add("before", before));
    }

    @Override public void setLeader(String nodeId, boolean leader) {
        jdbc().update("UPDATE wf_node SET leader=:leader WHERE id=:id",
                params().add("leader", leader ? 1 : 0).add("id", nodeId));
    }

    @Override public List<ServerNode> nodes() {
        return jdbc().query("SELECT * FROM wf_node ORDER BY first_heartbeat, id", (rs, row) -> JdbcStorage.readNode(rs));
    }
}
