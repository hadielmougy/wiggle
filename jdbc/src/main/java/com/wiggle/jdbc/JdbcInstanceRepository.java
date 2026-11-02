package com.wiggle.jdbc;

import com.wiggle.core.InstanceStatus;
import com.wiggle.server.store.InstanceRepository;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Instance;
import com.wiggle.server.store.WriteBuffer;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** {@link InstanceRepository} on {@code wf_instance}, and the instance locks taken through a token. */
final class JdbcInstanceRepository extends JdbcRepository implements InstanceRepository {

    private static final String INSERT_INSTANCE = "INSERT INTO wf_instance "
            + "(id,workflow,version,correlation_id,status,term_reason,error,context,created_at,updated_at,"
            + "revision,parent_token_id) VALUES "
            + "(:id,:workflow,:version,:correlationId,:status,:termReason,:error,:context,:createdAt,"
            + ":updatedAt,:revision,:parentTokenId)";

    private static final String UPDATE_INSTANCE = "UPDATE wf_instance SET status=:status,"
            + "term_reason=:termReason,error=:error,context=:context,updated_at=:updatedAt,"
            + "revision=revision+1 WHERE id=:id";

    /** The token and its instance in one statement, both locked, instance first, so each is read
     *  as it stands under its lock. */
    private static final String LOCK_TASK = "SELECT t.*, "
            + JdbcStorage.INSTANCE_COLUMNS.stream().map(c -> "i." + c + " AS i_" + c).collect(Collectors.joining(","))
            + " FROM wf_instance i JOIN wf_token t ON t.instance_id=i.id WHERE t.id=:id FOR UPDATE OF i, t";

    JdbcInstanceRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public void insertInstance(Instance i) {
        jdbc().update(INSERT_INSTANCE, params()
                .text("id", i.id)
                .text("workflow", i.workflow)
                .add("version", i.version)
                .text("correlationId", i.correlationId)
                .text("status", i.status.name())
                .text("termReason", i.terminationReason)
                .text("error", i.error)
                .text("context", i.context.json())
                .add("createdAt", i.createdAt)
                .add("updatedAt", i.updatedAt)
                .add("revision", i.revision)
                .text("parentTokenId", i.parentTokenId));
    }

    @Override public Optional<Instance> lockInstance(String id) { return loadInstance(id, true); }

    @Override public Optional<Instance> findInstance(String id) { return loadInstance(id, false); }

    /** The instance that owns token {@code tokenId}, locked, in one statement. */
    Optional<Instance> lockInstanceOf(String tokenId) {
        return first(jdbc().query("SELECT * FROM wf_instance WHERE id=(SELECT instance_id FROM wf_token WHERE id=:id)"
                + " FOR UPDATE", params().text("id", tokenId), (rs, row) -> JdbcStorage.readInstance(rs)));
    }

    /** The token and its instance, both locked, in one statement. Only for a dialect that {@link Dialect#supportsJoinedLock}. */
    Optional<Rows.LockedTask> lockTask(String tokenId) {
        return first(jdbc().query(LOCK_TASK, params().text("id", tokenId),
                (rs, row) -> new Rows.LockedTask(JdbcStorage.readInstance(rs, "i_"),
                        owner.recorded(JdbcStorage.readToken(rs)))));
    }

    private Optional<Instance> loadInstance(String id, boolean forUpdate) {
        String sql = forUpdate
                ? "SELECT * FROM wf_instance WHERE id=:id FOR UPDATE"
                : "SELECT * FROM wf_instance WHERE id=:id";
        return first(jdbc().query(sql, params().text("id", id), (rs, row) -> JdbcStorage.readInstance(rs)));
    }

    @Override public void updateInstance(Instance i) {
        WriteBuffer held = owner.buffer();
        if (held != null) {
            held.updateInstance(i);
            return;
        }
        jdbc().update(UPDATE_INSTANCE, instanceUpdate(i));
        i.revision++;
    }

    @Override public List<Instance> lockInstances(List<String> ids) {
        if (ids.isEmpty()) return List.of();
        // ORDER BY id keeps lock acquisition in id order on a btree PK scan; see Tx.lockInstances.
        // SKIP LOCKED where the dialect has it: a batch must never WAIT on an instance another
        // batch holds -- two workers whose batches share one instance (two arms of one fork)
        // would otherwise convoy, each batch serialising behind the other's whole commit. A
        // skipped instance is simply absent from the result; the caller answers its run as
        // retryable and the worker reports it singly.
        return jdbc().query("SELECT * FROM wf_instance WHERE id IN (:ids) ORDER BY id FOR UPDATE"
                        + (dialect.supportsSkipLocked() ? " SKIP LOCKED" : ""),
                params().add("ids", ids), (rs, row) -> JdbcStorage.readInstance(rs));
    }

    @Override public void updateInstances(List<Instance> instances) {
        if (instances.isEmpty()) return;
        SqlParameterSource[] rows = instances.stream().map(JdbcInstanceRepository::instanceUpdate)
                .toArray(SqlParameterSource[]::new);
        requireOneRowEach(jdbc().batchUpdate(UPDATE_INSTANCE, rows), "update wf_instance");
        for (Instance i : instances) i.revision++;
    }

    private static Params instanceUpdate(Instance i) {
        return params()
                .text("status", i.status.name())
                .text("termReason", i.terminationReason)
                .text("error", i.error)
                .text("context", i.context.json())
                .add("updatedAt", i.updatedAt)
                .text("id", i.id);
    }

    @Override public List<Instance> findByCorrelation(String correlationId, int limit) {
        return jdbc().query("SELECT * FROM wf_instance WHERE correlation_id=:cid ORDER BY created_at DESC LIMIT :limit",
                params().text("cid", correlationId).add("limit", limit), (rs, row) -> JdbcStorage.readInstance(rs));
    }

    @Override public List<Instance> listInstances(String workflow, InstanceStatus status, int limit) {
        // Both filters are optional, so the clauses are conditional but the binds are not
        // positional: a name binds where it appears, or nowhere, and cannot slide.
        Params p = params().add("limit", limit);
        if (workflow != null) p.text("workflow", workflow);
        if (status != null) p.text("status", status.name());
        return jdbc().query("SELECT * FROM wf_instance WHERE 1=1"
                + (workflow != null ? " AND workflow=:workflow" : "")
                + (status != null ? " AND status=:status" : "")
                + " ORDER BY created_at DESC LIMIT :limit", p, (rs, row) -> JdbcStorage.readInstance(rs));
    }

    @Override public int countInstances(InstanceStatus status) {
        return jdbc().queryForObject("SELECT COUNT(*) FROM wf_instance WHERE status=:status",
                params().text("status", status.name()), Integer.class);
    }

    @Override public int deleteTerminalInstancesBefore(long updatedBefore, int limit) {
        // ORDER BY is required for SQL Server's OFFSET/FETCH rewrite of LIMIT, and gives every
        // dialect a deterministic "oldest first" deletion order at no cost.
        List<String> ids = new ArrayList<>(jdbc().queryForList("SELECT id FROM wf_instance "
                        + "WHERE status NOT IN ('RUNNING','COMPENSATING') AND updated_at<:before "
                        + "ORDER BY updated_at LIMIT :limit",
                params().add("before", updatedBefore).add("limit", limit), String.class));
        if (ids.isEmpty()) return 0;
        // Children before parents: a token or comp-log row outliving its instance is a leak.
        jdbc().update("DELETE FROM wf_token_attr WHERE token_id IN (SELECT id FROM wf_token WHERE instance_id IN (:ids))",
                params().add("ids", ids));
        for (String table : List.of("wf_token", "wf_comp_log", "wf_dyn_node")) {
            jdbc().update("DELETE FROM " + table + " WHERE instance_id IN (:ids)", params().add("ids", ids));
        }
        jdbc().update("DELETE FROM wf_instance WHERE id IN (:ids)", params().add("ids", ids));
        return ids.size();
    }

    @Override public List<String> childInstanceIds(String parentInstanceId) {
        return jdbc().queryForList("SELECT id FROM wf_instance WHERE parent_token_id IN "
                        + "(SELECT id FROM wf_token WHERE instance_id=:id) ORDER BY id",
                params().text("id", parentInstanceId), String.class);
    }
}
