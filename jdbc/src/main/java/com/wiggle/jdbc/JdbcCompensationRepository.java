package com.wiggle.jdbc;

import com.wiggle.core.Doc;
import com.wiggle.server.store.CompensationRepository;
import com.wiggle.server.store.Rows;

import java.util.List;

/** {@link CompensationRepository} on {@code wf_comp_log}. */
final class JdbcCompensationRepository extends JdbcRepository implements CompensationRepository {

    JdbcCompensationRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public void appendCompensation(Rows.CompLog e) {
        jdbc().update("INSERT INTO wf_comp_log "
                        + "(instance_id,seq,node_id,activity,queue,input_json,result_json,compensated) "
                        + "VALUES (:instanceId,:seq,:nodeId,:activity,:queue,:input,:result,:compensated)",
                params()
                        .text("instanceId", e.instanceId)
                        .add("seq", e.seq)
                        .text("nodeId", e.nodeId)
                        .text("activity", e.activity)
                        .text("queue", e.queue)
                        .text("input", e.input == null ? null : e.input.json())
                        .text("result", e.result == null ? null : e.result.json())
                        .add("compensated", e.compensated ? 1 : 0));
    }

    @Override public void markCompensated(String instanceId, long seq) {
        jdbc().update("UPDATE wf_comp_log SET compensated=1 WHERE instance_id=:id AND seq=:seq",
                params().text("id", instanceId).add("seq", seq));
    }

    @Override public List<Rows.CompLog> compensationLog(String instanceId) {
        return jdbc().query("SELECT seq,node_id,activity,queue,input_json,result_json,compensated "
                        + "FROM wf_comp_log WHERE instance_id=:id ORDER BY seq",
                params().text("id", instanceId),
                (rs, row) -> {
                    Rows.CompLog e = new Rows.CompLog();
                    e.instanceId = instanceId;
                    e.seq = rs.getLong("seq");
                    e.nodeId = rs.getString("node_id");
                    e.activity = rs.getString("activity");
                    e.queue = rs.getString("queue");
                    e.input = Doc.parse(rs.getString("input_json"));
                    e.result = Doc.parse(rs.getString("result_json"));
                    e.compensated = rs.getInt("compensated") != 0;
                    return e;
                });
    }
}
