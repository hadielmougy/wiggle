package com.wiggle.jdbc;

import com.wiggle.core.TokenStatus;
import com.wiggle.core.WorkflowVersion;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.PayloadCodec;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.TokenRepository;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** {@link TokenRepository} on {@code wf_token} and {@code wf_token_attr}: the tokens, their leases and the work queue. */
final class JdbcTokenRepository extends JdbcRepository implements TokenRepository {

    private static final String INSERT_TOKEN = "INSERT INTO wf_token (id,instance_id,workflow,version,"
            + "node_id,kind,status,activity,queue,attempt,available_at,lease_owner,lease_expires,join_stack,"
            + "last_error,created_at,updated_at,payload,comp_seq,started_at,finished_at,inst_created_at,"
            + "correlation_id,step_input,step_output) VALUES "
            + "(:id,:instanceId,:workflow,:version,:nodeId,:kind,:status,:activity,:queue,:attempt,"
            + ":availableAt,:leaseOwner,:leaseExpires,:joinStack,:lastError,:createdAt,:updatedAt,:payload,"
            + ":compSeq,:startedAt,:finishedAt,:instCreatedAt,:correlationId,:stepInput,:stepOutput)";

    // Every column the insert names except the identity ones, so the same parameters serve both.
    private static final String UPDATE_TOKEN = "UPDATE wf_token SET node_id=:nodeId,kind=:kind,"
            + "status=:status,activity=:activity,queue=:queue,attempt=:attempt,"
            + "available_at=:availableAt,lease_owner=:leaseOwner,lease_expires=:leaseExpires,"
            + "join_stack=:joinStack,last_error=:lastError,updated_at=:updatedAt,payload=:payload,"
            + "comp_seq=:compSeq,started_at=:startedAt,finished_at=:finishedAt,"
            + "step_input=:stepInput,step_output=:stepOutput WHERE id=:id";

    /** {@link #UPDATE_TOKEN} minus the payload column, for a row whose payload is unchanged. */
    private static final String UPDATE_TOKEN_KEEP_PAYLOAD = UPDATE_TOKEN.replace("payload=:payload,", "");

    private static final String DUE = "status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=:now";
    private static final String AGE_ORDER = " ORDER BY inst_created_at, available_at, id LIMIT :max";

    /** A token row, stamped with the transaction that read it. */
    private final RowMapper<Token> token;

    JdbcTokenRepository(JdbcStorage owner) {
        super(owner);
        this.token = (rs, row) -> owner.recorded(JdbcStorage.readToken(rs));
    }

    @Override public void insertToken(Token t) {
        jdbc().update(INSERT_TOKEN, tokenRow(t));
        writeAttributes(List.of(t), false);
        owner.recorded(t);
    }

    @Override public void insertTokens(List<Token> tokens) {
        if (tokens.isEmpty()) return;
        SqlParameterSource[] rows = tokens.stream().map(JdbcTokenRepository::tokenRow).toArray(SqlParameterSource[]::new);
        requireOneRowEach(jdbc().batchUpdate(INSERT_TOKEN, rows), "insert wf_token");
        writeAttributes(tokens, false);
        for (Token t : tokens) owner.recorded(t);
    }

    /**
     * The attribute rows of the tokens whose attributes changed, in one batch; none at all for a
     * flow that declares no attributes. {@code replace} drops a token's earlier rows first.
     */
    private void writeAttributes(List<Token> tokens, boolean replace) {
        Map<String, Token> last = new LinkedHashMap<>();
        for (Token t : tokens) if (t.attributesChanged) last.put(t.id, t);
        if (last.isEmpty()) return;
        List<Token> changed = List.copyOf(last.values());
        if (replace) {
            jdbc().update("DELETE FROM wf_token_attr WHERE token_id IN (:ids)", params().add("ids", List.copyOf(last.keySet())));
        }
        for (Token t : tokens) t.attributesChanged = false;
        List<SqlParameterSource> rows = new ArrayList<>();
        for (Token t : changed) {
            if (t.attributes == null) continue;
            for (Map.Entry<String, Object> a : t.attributes.entrySet()) {
                Object v = a.getValue();
                rows.add(params()
                        .text("tokenId", t.id)
                        .text("name", a.getKey())
                        .text("t", v instanceof Boolean ? "B" : v instanceof String ? "S" : "N")
                        .text("s", v instanceof Number ? null : String.valueOf(v))
                        .decimal("n", v instanceof Number n ? attributeNumber(n) : null));
            }
        }
        if (!rows.isEmpty()) {
            jdbc().batchUpdate("INSERT INTO wf_token_attr (token_id,name,t,s_val,n_val) VALUES (:tokenId,:name,:t,:s,:n)",
                    rows.toArray(SqlParameterSource[]::new));
        }
    }

    /** Every wf_token column, by name. The empty join-stack sentinel is applied here, not assumed of the row. */
    private static Params tokenRow(Token t) {
        return updatable(t)
                .text("instanceId", t.instanceId)
                .text("workflow", t.workflow)
                .add("version", t.version)
                .add("createdAt", t.createdAt)
                .add("instCreatedAt", t.instCreatedAt)
                .text("correlationId", t.correlationId)
                .text("payload", PayloadCodec.encode(t.payload));
    }

    /** The columns {@link #UPDATE_TOKEN_KEEP_PAYLOAD} sets, and the id it matches on. */
    private static Params updatable(Token t) {
        return params()
                .text("id", t.id)
                .text("nodeId", t.nodeId)
                .text("kind", t.kind.name())
                .text("status", t.status.name())
                .text("activity", t.activity)
                .text("queue", t.queue)
                .add("attempt", t.attempt)
                .add("availableAt", t.availableAt)
                .text("leaseOwner", t.leaseOwner)
                .add("leaseExpires", t.leaseExpiresAt)
                .text("joinStack", t.joinStack == null ? "" : t.joinStack)
                .text("lastError", t.lastError)
                .add("updatedAt", t.updatedAt)
                .number("compSeq", t.compSeq)
                .number("startedAt", t.startedAt)
                .number("finishedAt", t.finishedAt)
                .text("stepInput", t.stepInput)
                .text("stepOutput", t.stepOutput);
    }

    @Override public Optional<Token> findToken(String id) {
        return first(jdbc().query("SELECT * FROM wf_token WHERE id=:id", params().text("id", id), token));
    }

    @Override public List<Token> findTokens(List<String> ids) {
        if (ids.isEmpty()) return List.of();
        return jdbc().query("SELECT * FROM wf_token WHERE id IN (:ids)", params().add("ids", ids), token);
    }

    @Override public List<Token> tokensOf(String instanceId) {
        return jdbc().query("SELECT * FROM wf_token WHERE instance_id=:id ORDER BY id", params().text("id", instanceId), token);
    }

    @Override public boolean hasActiveTokens(String instanceId) {
        return !jdbc().queryForList("SELECT 1 FROM wf_token WHERE instance_id=:id "
                        + "AND status IN ('READY','RUNNING','WAITING','AWAITING','JOINED') LIMIT 1",
                params().text("id", instanceId), Integer.class).isEmpty();
    }

    private boolean payloadUnchanged(Token t) {
        return t.storedIn == owner.transactionMark() && t.payload == t.storedPayload;
    }

    private static Params tokenUpdate(Token t, boolean keepPayload) {
        return keepPayload ? updatable(t) : tokenRow(t);
    }

    @Override public void updateToken(Token t) {
        boolean keep = payloadUnchanged(t);
        jdbc().update(keep ? UPDATE_TOKEN_KEEP_PAYLOAD : UPDATE_TOKEN, tokenUpdate(t, keep));
        writeAttributes(List.of(t), true);
        owner.recorded(t);
    }

    /** Rows with an unchanged payload go in a batch that leaves the column alone. Splitting
     *  reorders writes across the two batches, so a list naming one row twice keeps the single
     *  full-row batch and its order. */
    @Override public void updateTokens(List<Token> tokens) {
        if (tokens.isEmpty()) return;
        List<Token> keep = new ArrayList<>();
        List<Token> write = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        boolean distinct = true;
        for (Token t : tokens) {
            distinct &= ids.add(t.id);
            (payloadUnchanged(t) ? keep : write).add(t);
        }
        if (!distinct) {
            keep.clear();
            write = tokens;
        }
        updateBatch(write, false);
        updateBatch(keep, true);
        writeAttributes(tokens, true);
        for (Token t : tokens) owner.recorded(t);
    }

    private void updateBatch(List<Token> tokens, boolean keepPayload) {
        if (tokens.isEmpty()) return;
        SqlParameterSource[] rows = tokens.stream().map(t -> tokenUpdate(t, keepPayload)).toArray(SqlParameterSource[]::new);
        requireOneRowEach(jdbc().batchUpdate(keepPayload ? UPDATE_TOKEN_KEEP_PAYLOAD : UPDATE_TOKEN, rows),
                "update wf_token");
    }

    @Override public boolean renewLease(String taskId, String leaseOwner, long until, long now) {
        Params p = params().add("until", until).add("now", now).text("id", taskId);
        if (leaseOwner != null) p.text("owner", leaseOwner);
        return jdbc().update("UPDATE wf_token SET lease_expires=:until,updated_at=:now"
                + " WHERE id=:id AND status='RUNNING'"
                + (leaseOwner == null ? "" : " AND lease_owner=:owner"), p) == 1;
    }

    @Override public List<String> joinStacksAt(String instanceId, String nodeId) {
        return jdbc().queryForList("SELECT join_stack FROM wf_token "
                        + "WHERE instance_id=:id AND node_id=:node AND status='JOINED'",
                params().text("id", instanceId).text("node", nodeId), String.class);
    }

    @Override public List<Token> joinedAt(String instanceId, String nodeId) {
        return jdbc().query("SELECT * FROM wf_token "
                        + "WHERE instance_id=:id AND node_id=:node AND status='JOINED' ORDER BY id",
                params().text("id", instanceId).text("node", nodeId), token);
    }

    @Override public Optional<Token> awaitingSignal(String instanceId, String name) {
        return first(jdbc().query("SELECT * FROM wf_token WHERE instance_id=:id AND status='AWAITING' "
                        + "AND kind='SIGNAL' AND activity=:name ORDER BY id LIMIT 1",
                params().text("id", instanceId).text("name", name), token));
    }

    @Override public List<Token> claimTasks(String workerId, ClaimFilter filter, int max, long now, long leaseUntil) {
        QueueSql queues = QueueSql.of(filter, dialect);
        Set<WorkflowVersion> versions = filter.versions();
        // PostgreSQL claims in one statement; H2 has neither SKIP LOCKED nor RETURNING and
        // falls back to compare-and-set.
        return dialect.supportsSkipLocked() && dialect.supportsReturning()
                ? claimSkipLockedReturning(workerId, queues, versions, max, now, leaseUntil)
                : claimCompareAndSet(workerId, queues, versions, max, now, leaseUntil);
    }

    /**
     * A claim's queues, each with its compiled predicate (null where it has none). The i-th
     * queue binds as {@code :cq<i>} and its predicate's values as {@code :c<i>_<n>}.
     */
    private record QueueSql(List<String> queues, List<CriteriaSql> where) {

        static QueueSql of(ClaimFilter filter, Dialect dialect) {
            List<String> queues = new ArrayList<>(filter.queues().size());
            List<CriteriaSql> where = new ArrayList<>(filter.queues().size());
            for (com.wiggle.core.criteria.QueueCriteria c : filter.queues()) {
                queues.add(c.queue());
                where.add(c.where() == null ? null : CriteriaSql.compile(c.where(), dialect, "c" + where.size() + "_"));
            }
            return new QueueSql(queues, Collections.unmodifiableList(where));
        }

        boolean filtered() {
            return where.stream().anyMatch(Objects::nonNull);
        }

        /** The queue condition: one queue's (with its predicate) or, unfiltered, an IN list. */
        String clause(int i) {
            return "queue=:cq" + i + (where.get(i) == null ? "" : " AND " + where.get(i).sql());
        }

        String clause() {
            if (queues.isEmpty()) return "";
            if (!filtered()) return " AND queue IN (:queues)";
            StringBuilder out = new StringBuilder(" AND (");
            for (int i = 0; i < queues.size(); i++) {
                out.append(i == 0 ? "" : " OR ").append('(').append(clause(i)).append(')');
            }
            return out.append(')').toString();
        }

        void bind(Params p) {
            if (queues.isEmpty()) return;
            if (!filtered()) {
                p.add("queues", queues);
                return;
            }
            for (int i = 0; i < queues.size(); i++) {
                p.text("cq" + i, queues.get(i));
                if (where.get(i) != null) where.get(i).bind(p);
            }
        }
    }

    /** The (workflow, version) filter: one OR-ed pair per version, each named by its position
     *  so the clause and its parameters cannot drift apart. */
    private static String versionsClause(Set<WorkflowVersion> versions) {
        if (versions == null || versions.isEmpty()) return "";
        StringBuilder sql = new StringBuilder(" AND (");
        for (int i = 0; i < versions.size(); i++) {
            if (i > 0) sql.append(" OR ");
            sql.append("(workflow=:wf").append(i).append(" AND version=:ver").append(i).append(")");
        }
        return sql.append(")").toString();
    }

    private static void bindVersions(Params p, Set<WorkflowVersion> versions) {
        if (versions == null || versions.isEmpty()) return;
        int i = 0;
        for (WorkflowVersion v : versions) {
            p.text("wf" + i, v.workflow());
            p.add("ver" + i, v.version());
            i++;
        }
    }

    /** The claim candidate filter, shared by both dialect paths: ready task tokens that are due,
     *  optionally narrowed to the worker's queues and their criteria, and its bound versions.
     *  {@code %s} is the select list -- the ids alone where the update reads them back, whole
     *  rows otherwise. */
    private static String claimFilter(QueueSql queues, Set<WorkflowVersion> versions) {
        return "SELECT %s FROM wf_token WHERE " + DUE + queues.clause() + versionsClause(versions) + AGE_ORDER;
    }

    /**
     * Atomic claim for PostgreSQL: lock up to {@code max} dispatchable rows with
     * SKIP LOCKED -- which steps over rows another worker already holds instead of
     * blocking on them -- and update them in the same statement. Because no
     * transaction ever waits on a row task by another, concurrent claims across
     * many workers and nodes cannot deadlock, and none of them collide on a row.
     */
    private List<Token> claimSkipLockedReturning(String workerId, QueueSql queues, Set<WorkflowVersion> versions,
                                                 int max, long now, long leaseUntil) {
        Params p = params();
        String sql = claimSql("", workerId, queues, versions, max, now, leaseUntil, p);
        return jdbc().query(sql, p, token);
    }

    /** The plan the database chooses for {@link #claimSkipLockedReturning}, as {@code EXPLAIN} prints it. */
    String explainClaim(ClaimFilter filter, int max, long now) {
        Params p = params();
        String sql = claimSql("EXPLAIN ", "explain", QueueSql.of(filter, dialect), filter.versions(), max, now, now, p);
        return String.join("\n", jdbc().query(sql, p, (rs, row) -> rs.getString(1)));
    }

    /** The single-statement claim, after {@code prefix}, with its parameters added to {@code p}. */
    private static String claimSql(String prefix, String workerId, QueueSql queues, Set<WorkflowVersion> versions,
                                   int max, long now, long leaseUntil, Params p) {
        boolean perQueue = queues.queues().size() > 1;
        String pick = perQueue
                ? perQueuePick(queues, versions)
                : claimFilter(queues, versions).formatted("id") + " FOR UPDATE SKIP LOCKED";
        p.text("owner", workerId)
                .add("until", leaseUntil)
                // updated_at, started_at and the due cutoff are all this instant
                .add("now", now)
                .add("max", max);
        if (perQueue && !queues.filtered()) {
            for (int i = 0; i < queues.queues().size(); i++) p.text("q" + i, queues.queues().get(i));
        } else {
            queues.bind(p);
        }
        bindVersions(p, versions);
        return prefix + "UPDATE wf_token SET status='RUNNING',lease_owner=:owner,"
                + "lease_expires=:until,updated_at=:now,started_at=:now,finished_at=NULL"
                + " WHERE id IN (" + pick + ") RETURNING *";
    }

    /**
     * The claim's pick across several queues: each queue's earliest {@code max} dispatchable
     * rows, read in index order and locked SKIP LOCKED, then the earliest {@code max} of
     * those. One filter over all the queues instead would sort every due row in them on
     * every claim. Rows locked here but not picked stay locked, and are skipped by other
     * claimers, until this claim commits. With criteria, each queue's pick is its own
     * subquery with its own predicate, so each is planned for the predicate it has.
     */
    private static String perQueuePick(QueueSql queues, Set<WorkflowVersion> versions) {
        String pickOne = "SELECT id, inst_created_at, available_at FROM wf_token WHERE " + DUE + " AND %s"
                + versionsClause(versions) + AGE_ORDER + " FOR UPDATE SKIP LOCKED";
        String outer = " ORDER BY c.inst_created_at, c.available_at, c.id LIMIT :max";
        if (!queues.filtered()) {
            StringBuilder names = new StringBuilder();
            for (int i = 0; i < queues.queues().size(); i++) {
                names.append(i == 0 ? "" : ",").append("(:q").append(i).append(")");
            }
            return "SELECT c.id FROM (VALUES " + names + ") AS q(name) CROSS JOIN LATERAL ("
                    + pickOne.formatted("queue=q.name") + ") c" + outer;
        }
        StringBuilder arms = new StringBuilder();
        for (int i = 0; i < queues.queues().size(); i++) {
            if (i > 0) arms.append(" UNION ALL ");
            arms.append("SELECT * FROM (").append(pickOne.formatted(queues.clause(i))).append(") a").append(i);
        }
        return "SELECT c.id FROM (" + arms + ") c" + outer;
    }

    /** Portable fallback (H2): over-fetch candidates, then compare-and-set each. */
    private List<Token> claimCompareAndSet(String workerId, QueueSql queues, Set<WorkflowVersion> versions,
                                           int max, long now, long leaseUntil) {
        Params pick = params().add("now", now).add("max", max * 4);   // over-fetch: some candidates will lose the CAS race
        queues.bind(pick);
        bindVersions(pick, versions);
        List<Token> candidates = jdbc().query(claimFilter(queues, versions).formatted("*"), pick, token);

        List<Token> claimed = new ArrayList<>();
        for (Token t : candidates) {
            if (claimed.size() >= max) break;
            int won = jdbc().update("UPDATE wf_token SET status='RUNNING',lease_owner=:owner,"
                            + "lease_expires=:until,updated_at=:now,started_at=:now,finished_at=NULL"
                            + " WHERE id=:id AND status='READY'",
                    params().text("owner", workerId).add("until", leaseUntil).add("now", now).text("id", t.id));
            if (won == 1) {
                t.status = TokenStatus.RUNNING;
                t.leaseOwner = workerId;
                t.leaseExpiresAt = leaseUntil;
                t.startedAt = now;
                t.finishedAt = null;
                t.updatedAt = now;
                claimed.add(t);
            }
        }
        return claimed;
    }

    @Override public List<Token> dueRetries(long now, int max) {
        return sweep("SELECT * FROM wf_token WHERE status='WAITING' AND kind IN ('TASK','PREDICATE') "
                + "AND available_at<=? ORDER BY available_at LIMIT ?", now, max);
    }

    @Override public List<Token> dueTimers(long now, int max) {
        return sweep("SELECT * FROM wf_token WHERE status='WAITING' AND kind='SLEEP' AND available_at<=? "
                + "ORDER BY available_at LIMIT ?", now, max);
    }

    @Override public List<Token> expiredLeases(long now, int max) {
        return sweep("SELECT * FROM wf_token WHERE status='RUNNING' AND lease_expires>0 AND lease_expires<? "
                + "ORDER BY lease_expires LIMIT ?", now, max);
    }

    @Override public List<Token> pendingSignals(int max) {
        return jdbc().query("SELECT * FROM wf_token WHERE status='AWAITING' AND kind='SIGNAL' "
                + "ORDER BY created_at LIMIT :max", params().add("max", max), token);
    }

    @Override public List<Token> dueSignals(long now, int max) {
        return sweep("SELECT * FROM wf_token WHERE status='AWAITING' AND kind='SIGNAL' "
                + "AND available_at>0 AND available_at<=? ORDER BY available_at LIMIT ?", now, max);
    }

    /** The token sweeps: one bound time and a cap, in that order. */
    private List<Token> sweep(String sql, long arg, int limit) {
        return positional().query(sql, token, arg, limit);
    }

    @Override public Rows.QueueDepth queueDepth(long now) {
        return jdbc().queryForObject("SELECT COUNT(*) AS depth, COALESCE(MIN(available_at),0) AS oldest "
                        + "FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE') "
                        + "AND available_at<=:now",
                params().add("now", now), (rs, row) -> new Rows.QueueDepth(rs.getInt("depth"), rs.getLong("oldest")));
    }

    @Override public int countReady(String workflow, int version, String queue,
                                    com.wiggle.core.criteria.Predicate where, long now, int cap) {
        CriteriaSql sql = where == null ? null : CriteriaSql.compile(where, dialect);
        Params p = params().add("now", now).text("queue", queue).text("workflow", workflow)
                .add("version", version).add("cap", cap);
        if (sql != null) sql.bind(p);
        return jdbc().queryForObject("SELECT COUNT(*) FROM (SELECT 1 FROM wf_token WHERE status='READY'"
                + " AND kind IN ('TASK','PREDICATE') AND available_at<=:now AND queue=:queue"
                + " AND workflow=:workflow AND version=:version" + (sql == null ? "" : " AND " + sql.sql())
                + " LIMIT :cap) slice", p, Integer.class);
    }

    @Override public List<Rows.BacklogSlice> backlogByVersion(long now, int max) {
        return jdbc().query("SELECT workflow, version, queue, COUNT(*) AS depth, "
                        + "COALESCE(MIN(available_at),0) AS oldest FROM wf_token "
                        + "WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=:now "
                        + "GROUP BY workflow, version, queue ORDER BY COUNT(*) DESC LIMIT :max",
                params().add("now", now).add("max", max),
                (rs, row) -> new Rows.BacklogSlice(rs.getString("workflow"), rs.getInt("version"),
                        rs.getString("queue"), rs.getInt("depth"), rs.getLong("oldest")));
    }

    @Override public List<Rows.StepDuration> stepDurations(String workflow, int version, long since, int max) {
        return jdbc().query("SELECT node_id, started_at, finished_at, available_at FROM wf_token "
                        + "WHERE workflow=:workflow AND version=:version AND status='DONE' "
                        + "AND finished_at > :since AND started_at IS NOT NULL "
                        + "ORDER BY finished_at DESC LIMIT :max",
                params().text("workflow", workflow).add("version", version).add("since", since).add("max", max),
                (rs, row) -> {
                    long ran = Math.max(0, rs.getLong("finished_at") - rs.getLong("started_at"));
                    long waited = Math.max(0, rs.getLong("started_at") - rs.getLong("available_at"));
                    return new Rows.StepDuration(rs.getString("node_id"), ran, waited);
                });
    }

    @Override public void cancelActiveTokens(String instanceId, long now) {
        jdbc().update("""
                UPDATE wf_token
                   SET status='CANCELLED', lease_owner=NULL, lease_expires=0, updated_at=:now
                 WHERE instance_id=:id
                   AND status IN ('READY','RUNNING','WAITING','AWAITING','JOINED')
                """, params().add("now", now).text("id", instanceId));
    }
}
