package dev.wiggle.server.store;

import dev.wiggle.core.NodeKind;
import dev.wiggle.server.store.Rows.Instance;
import dev.wiggle.server.store.Rows.InstanceStatus;
import dev.wiggle.server.store.Rows.ServerNode;
import dev.wiggle.server.store.Rows.Token;
import dev.wiggle.server.store.Rows.TokenStatus;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;

/**
 * Shared-database store. This is what makes multi-node operation work: instance
 * mutual exclusion comes from SELECT ... FOR UPDATE, task hand-out from a
 * conditional UPDATE (compare-and-set on status), and leader election from the
 * wf_node heartbeat table.
 *
 * Tested against PostgreSQL 14+ and H2 2.x in PostgreSQL compatibility mode.
 */
public final class JdbcStorage implements Storage {

    private final String url, user, password;
    private final ArrayBlockingQueue<Connection> pool;
    private final int poolSize;

    public JdbcStorage(String url, String user, String password, int poolSize) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.poolSize = poolSize;
        this.pool = new ArrayBlockingQueue<>(poolSize);
    }

    private Connection borrow() {
        try {
            Connection c = pool.poll();
            if (c == null || c.isClosed()) {
                c = user == null ? DriverManager.getConnection(url)
                        : DriverManager.getConnection(url, user, password);
                c.setAutoCommit(false);
                c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            }
            return c;
        } catch (SQLException e) {
            throw new StorageException("cannot obtain connection", e);
        }
    }

    private void release(Connection c) {
        if (c == null) return;
        if (!pool.offer(c)) {
            try { c.close(); } catch (SQLException ignored) { }
        }
    }

    @Override public void migrate() {
        String ddl = """
            CREATE TABLE IF NOT EXISTS wf_definition (
              name           VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              body           TEXT         NOT NULL,
              registered_at  BIGINT       NOT NULL,
              PRIMARY KEY (name, version)
            );
            CREATE TABLE IF NOT EXISTS wf_instance (
              id             VARCHAR(64)  PRIMARY KEY,
              workflow       VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              correlation_id VARCHAR(200),
              status         VARCHAR(16)  NOT NULL,
              term_reason    VARCHAR(200),
              error          TEXT,
              context        TEXT         NOT NULL,
              created_at     BIGINT       NOT NULL,
              updated_at     BIGINT       NOT NULL,
              revision       BIGINT       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_instance_status ON wf_instance (status, updated_at);
            CREATE TABLE IF NOT EXISTS wf_token (
              id             VARCHAR(64)  PRIMARY KEY,
              instance_id    VARCHAR(64)  NOT NULL,
              workflow       VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              node_id        VARCHAR(64)  NOT NULL,
              kind           VARCHAR(16)  NOT NULL,
              status         VARCHAR(16)  NOT NULL,
              activity       VARCHAR(300),
              queue          VARCHAR(200),
              attempt        INT          NOT NULL,
              available_at   BIGINT       NOT NULL,
              lease_owner    VARCHAR(120),
              lease_expires  BIGINT       NOT NULL,
              join_stack     VARCHAR(1000) NOT NULL,
              last_error     TEXT,
              created_at     BIGINT       NOT NULL,
              updated_at     BIGINT       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_token_dispatch ON wf_token (status, queue, available_at);
            CREATE INDEX IF NOT EXISTS ix_token_instance ON wf_token (instance_id);
            CREATE INDEX IF NOT EXISTS ix_token_lease ON wf_token (status, lease_expires);
            CREATE TABLE IF NOT EXISTS wf_node (
              id             VARCHAR(64)  PRIMARY KEY,
              name           VARCHAR(200) NOT NULL,
              first_heartbeat BIGINT      NOT NULL,
              last_heartbeat BIGINT       NOT NULL,
              workers        INT          NOT NULL,
              leader         INT          NOT NULL
            );
            """;
        Connection c = borrow();
        try {
            // CREATE TABLE IF NOT EXISTS is not race-safe on PostgreSQL: two nodes can both
            // find a table absent and both try to create it, and the loser fails with a
            // pg_type unique violation. A transaction-scoped advisory lock, keyed by a
            // constant shared across nodes, serialises the whole one-time bootstrap so late
            // arrivals simply find everything already present. H2 has no such catalog race.
            acquireMigrationLock(c);
            try (Statement st = c.createStatement()) {
                for (String stmt : ddl.split(";")) {
                    if (!stmt.isBlank()) st.execute(stmt);
                }
            }
            c.commit(); // also releases the advisory lock
        } catch (SQLException e) {
            rollback(c);
            throw new StorageException("migration failed", e);
        } finally {
            release(c);
        }
    }

    /** Serialises migration across nodes on PostgreSQL; a no-op elsewhere. */
    private static void acquireMigrationLock(Connection c) throws SQLException {
        if (!isPostgres(c)) return;
        try (Statement st = c.createStatement()) {
            st.execute("SELECT pg_advisory_xact_lock(7420398115703004)");
        }
    }

    private static boolean isPostgres(Connection c) throws SQLException {
        String product = c.getMetaData().getDatabaseProductName();
        return product != null && product.toLowerCase(Locale.ROOT).contains("postgresql");
    }

    @Override public <R> R inTx(Function<Tx, R> work) {
        Connection c = borrow();
        try {
            R r = work.apply(new JdbcTx(c));
            c.commit();
            return r;
        } catch (SQLException e) {
            rollback(c);
            throw new StorageException("commit failed", e);
        } catch (RuntimeException e) {
            rollback(c);
            throw e;
        } finally {
            release(c);
        }
    }

    private static void rollback(Connection c) {
        try { c.rollback(); } catch (SQLException ignored) { }
    }

    @Override public void close() {
        List<Connection> all = new ArrayList<>(poolSize);
        pool.drainTo(all);
        for (Connection c : all) {
            try { c.close(); } catch (SQLException ignored) { }
        }
    }

    public static final class StorageException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public StorageException(String m, Throwable c) { super(m, c); }
    }

    // ------------------------------------------------------------------ tx

    private static final class JdbcTx implements Tx {
        private final Connection c;

        JdbcTx(Connection c) { this.c = c; }

        private PreparedStatement ps(String sql) throws SQLException { return c.prepareStatement(sql); }

        private static StorageException wrap(SQLException e) { return new StorageException(e.getMessage(), e); }

        @Override public void putDefinition(String name, int version, String json) {
            // The version is a content hash of the topology, so an existing (name,version) row
            // is byte-for-byte identical and re-registration is a genuine no-op. ON CONFLICT DO
            // NOTHING makes that idempotent atomically -- unlike a DELETE-then-INSERT, it leaves
            // no window in which two nodes registering the same graph collide on the primary key.
            try (PreparedStatement ins = ps("INSERT INTO wf_definition (name,version,body,registered_at) " +
                    "VALUES (?,?,?,?) ON CONFLICT DO NOTHING")) {
                ins.setString(1, name); ins.setInt(2, version); ins.setString(3, json);
                ins.setLong(4, System.currentTimeMillis());
                ins.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public Optional<String> definition(String name, int version) {
            try (PreparedStatement p = ps("SELECT body FROM wf_definition WHERE name=? AND version=?")) {
                p.setString(1, name); p.setInt(2, version);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public Optional<Integer> latestVersion(String name) {
            try (PreparedStatement p = ps(
                    "SELECT version FROM wf_definition WHERE name=? ORDER BY registered_at DESC, version DESC")) {
                p.setString(1, name);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next() ? Optional.of(rs.getInt(1)) : Optional.empty();
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<String> definitionNames() {
            try (PreparedStatement p = ps("SELECT DISTINCT name FROM wf_definition ORDER BY name");
                 ResultSet rs = p.executeQuery()) {
                List<String> out = new ArrayList<>();
                while (rs.next()) out.add(rs.getString(1));
                return out;
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void insertInstance(Instance i) {
            try (PreparedStatement p = ps("INSERT INTO wf_instance " +
                    "(id,workflow,version,correlation_id,status,term_reason,error,context,created_at,updated_at,revision) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)")) {
                p.setString(1, i.id); p.setString(2, i.workflow); p.setInt(3, i.version);
                p.setString(4, i.correlationId); p.setString(5, i.status.name());
                p.setString(6, i.terminationReason); p.setString(7, i.error); p.setString(8, i.contextJson);
                p.setLong(9, i.createdAt); p.setLong(10, i.updatedAt); p.setLong(11, i.revision);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public Optional<Instance> lockInstance(String id) { return loadInstance(id, true); }

        @Override public Optional<Instance> findInstance(String id) { return loadInstance(id, false); }

        private Optional<Instance> loadInstance(String id, boolean forUpdate) {
            String sql = "SELECT * FROM wf_instance WHERE id=?" + (forUpdate ? " FOR UPDATE" : "");
            try (PreparedStatement p = ps(sql)) {
                p.setString(1, id);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next() ? Optional.of(readInstance(rs)) : Optional.empty();
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void updateInstance(Instance i) {
            try (PreparedStatement p = ps("UPDATE wf_instance SET status=?,term_reason=?,error=?,context=?," +
                    "updated_at=?,revision=revision+1 WHERE id=?")) {
                p.setString(1, i.status.name()); p.setString(2, i.terminationReason); p.setString(3, i.error);
                p.setString(4, i.contextJson); p.setLong(5, i.updatedAt); p.setString(6, i.id);
                p.executeUpdate();
                i.revision++;
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Instance> listInstances(String workflow, InstanceStatus status, int limit) {
            StringBuilder sql = new StringBuilder("SELECT * FROM wf_instance WHERE 1=1");
            if (workflow != null) sql.append(" AND workflow=?");
            if (status != null) sql.append(" AND status=?");
            sql.append(" ORDER BY created_at DESC LIMIT ?");
            try (PreparedStatement p = ps(sql.toString())) {
                int idx = 1;
                if (workflow != null) p.setString(idx++, workflow);
                if (status != null) p.setString(idx++, status.name());
                p.setInt(idx, limit);
                try (ResultSet rs = p.executeQuery()) {
                    List<Instance> out = new ArrayList<>();
                    while (rs.next()) out.add(readInstance(rs));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public int countInstances(InstanceStatus status) {
            try (PreparedStatement p = ps("SELECT COUNT(*) FROM wf_instance WHERE status=?")) {
                p.setString(1, status.name());
                try (ResultSet rs = p.executeQuery()) { return rs.next() ? rs.getInt(1) : 0; }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void insertToken(Token t) {
            try (PreparedStatement p = ps("INSERT INTO wf_token (id,instance_id,workflow,version,node_id,kind,status," +
                    "activity,queue,attempt,available_at,lease_owner,lease_expires,join_stack,last_error,created_at,updated_at) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                bindToken(p, t);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        /** Binds parameters 1..17 in wf_token insert column order. */
        private void bindToken(PreparedStatement p, Token t) throws SQLException {
            p.setString(1, t.id);
            p.setString(2, t.instanceId);
            p.setString(3, t.workflow);
            p.setInt(4, t.version);
            p.setString(5, t.nodeId);
            p.setString(6, t.kind.name());
            p.setString(7, t.status.name());
            p.setString(8, t.activity);
            p.setString(9, t.queue);
            p.setInt(10, t.attempt);
            p.setLong(11, t.availableAt);
            p.setString(12, t.leaseOwner);
            p.setLong(13, t.leaseExpiresAt);
            p.setString(14, t.joinStack == null ? "" : t.joinStack);
            p.setString(15, t.lastError);
            p.setLong(16, t.createdAt);
            p.setLong(17, t.updatedAt);
        }

        @Override public Optional<Token> findToken(String id) {
            try (PreparedStatement p = ps("SELECT * FROM wf_token WHERE id=?")) {
                p.setString(1, id);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next() ? Optional.of(readToken(rs)) : Optional.empty();
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Token> tokensOf(String instanceId) {
            try (PreparedStatement p = ps("SELECT * FROM wf_token WHERE instance_id=? ORDER BY id")) {
                p.setString(1, instanceId);
                try (ResultSet rs = p.executeQuery()) {
                    List<Token> out = new ArrayList<>();
                    while (rs.next()) out.add(readToken(rs));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void updateToken(Token t) {
            try (PreparedStatement p = ps("UPDATE wf_token SET node_id=?,kind=?,status=?,activity=?,queue=?," +
                    "attempt=?,available_at=?,lease_owner=?,lease_expires=?,join_stack=?,last_error=?,updated_at=? WHERE id=?")) {
                p.setString(1, t.nodeId); p.setString(2, t.kind.name()); p.setString(3, t.status.name());
                p.setString(4, t.activity); p.setString(5, t.queue); p.setInt(6, t.attempt);
                p.setLong(7, t.availableAt); p.setString(8, t.leaseOwner); p.setLong(9, t.leaseExpiresAt);
                p.setString(10, t.joinStack == null ? "" : t.joinStack); p.setString(11, t.lastError);
                p.setLong(12, t.updatedAt); p.setString(13, t.id);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Token> claimTasks(String workerId, Set<String> queues, int max, long now, long leaseUntil) {
            StringBuilder sql = new StringBuilder(
                    "SELECT * FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=?");
            if (queues != null && !queues.isEmpty()) {
                sql.append(" AND queue IN (").append("?,".repeat(queues.size() - 1)).append("?)");
            }
            sql.append(" ORDER BY available_at LIMIT ?");
            List<Token> candidates = new ArrayList<>();
            try (PreparedStatement p = ps(sql.toString())) {
                int idx = 1;
                p.setLong(idx++, now);
                if (queues != null && !queues.isEmpty()) for (String q : queues) p.setString(idx++, q);
                p.setInt(idx, max * 4); // over-fetch: some candidates will lose the CAS race
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) candidates.add(readToken(rs));
                }
            } catch (SQLException e) { throw wrap(e); }

            List<Token> claimed = new ArrayList<>();
            try (PreparedStatement upd = ps("UPDATE wf_token SET status='RUNNING',lease_owner=?,lease_expires=?," +
                    "updated_at=? WHERE id=? AND status='READY'")) {
                for (Token t : candidates) {
                    if (claimed.size() >= max) break;
                    upd.setString(1, workerId);
                    upd.setLong(2, leaseUntil);
                    upd.setLong(3, now);
                    upd.setString(4, t.id);
                    if (upd.executeUpdate() == 1) {
                        t.status = TokenStatus.RUNNING;
                        t.leaseOwner = workerId;
                        t.leaseExpiresAt = leaseUntil;
                        t.updatedAt = now;
                        claimed.add(t);
                    }
                }
            } catch (SQLException e) { throw wrap(e); }
            return claimed;
        }

        @Override public List<Token> dueTimers(long now, int max) {
            return query("SELECT * FROM wf_token WHERE status='WAITING' AND kind='SLEEP' AND available_at<=? " +
                    "ORDER BY available_at LIMIT ?", now, max);
        }

        @Override public List<Token> expiredLeases(long now, int max) {
            return query("SELECT * FROM wf_token WHERE status='RUNNING' AND lease_expires>0 AND lease_expires<? " +
                    "ORDER BY lease_expires LIMIT ?", now, max);
        }

        private List<Token> query(String sql, long arg, int limit) {
            try (PreparedStatement p = ps(sql)) {
                p.setLong(1, arg);
                p.setInt(2, limit);
                try (ResultSet rs = p.executeQuery()) {
                    List<Token> out = new ArrayList<>();
                    while (rs.next()) out.add(readToken(rs));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void upsertNode(ServerNode n) {
            try (PreparedStatement upd = ps("UPDATE wf_node SET name=?,last_heartbeat=?,workers=? WHERE id=?")) {
                upd.setString(1, n.name); upd.setLong(2, n.lastHeartbeat); upd.setInt(3, n.workers);
                upd.setString(4, n.id);
                if (upd.executeUpdate() == 0) {
                    try (PreparedStatement ins = ps("INSERT INTO wf_node " +
                            "(id,name,first_heartbeat,last_heartbeat,workers,leader) VALUES (?,?,?,?,?,0)")) {
                        ins.setString(1, n.id); ins.setString(2, n.name); ins.setLong(3, n.firstHeartbeat);
                        ins.setLong(4, n.lastHeartbeat); ins.setInt(5, n.workers);
                        ins.executeUpdate();
                    }
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<ServerNode> nodes() {
            try (PreparedStatement p = ps("SELECT * FROM wf_node ORDER BY first_heartbeat, id");
                 ResultSet rs = p.executeQuery()) {
                List<ServerNode> out = new ArrayList<>();
                while (rs.next()) {
                    ServerNode n = new ServerNode();
                    n.id = rs.getString("id");
                    n.name = rs.getString("name");
                    n.firstHeartbeat = rs.getLong("first_heartbeat");
                    n.lastHeartbeat = rs.getLong("last_heartbeat");
                    n.workers = rs.getInt("workers");
                    n.leader = rs.getInt("leader") == 1;
                    out.add(n);
                }
                return out;
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void deleteNodesOlderThan(long before) {
            try (PreparedStatement p = ps("DELETE FROM wf_node WHERE last_heartbeat<?")) {
                p.setLong(1, before);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void setLeader(String nodeId, boolean leader) {
            try (PreparedStatement p = ps("UPDATE wf_node SET leader=? WHERE id=?")) {
                p.setInt(1, leader ? 1 : 0);
                p.setString(2, nodeId);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public int deleteTerminalInstancesBefore(long updatedBefore, int limit) {
            List<String> ids = new ArrayList<>();
            try (PreparedStatement p = ps("SELECT id FROM wf_instance WHERE status<>'RUNNING' AND updated_at<? LIMIT ?")) {
                p.setLong(1, updatedBefore);
                p.setInt(2, limit);
                try (ResultSet rs = p.executeQuery()) { while (rs.next()) ids.add(rs.getString(1)); }
            } catch (SQLException e) { throw wrap(e); }
            if (ids.isEmpty()) return 0;
            try (PreparedStatement dt = ps("DELETE FROM wf_token WHERE instance_id=?");
                 PreparedStatement di = ps("DELETE FROM wf_instance WHERE id=?")) {
                for (String id : ids) {
                    dt.setString(1, id); dt.executeUpdate();
                    di.setString(1, id); di.executeUpdate();
                }
            } catch (SQLException e) { throw wrap(e); }
            return ids.size();
        }

        private static Instance readInstance(ResultSet rs) throws SQLException {
            Instance i = new Instance();
            i.id = rs.getString("id");
            i.workflow = rs.getString("workflow");
            i.version = rs.getInt("version");
            i.correlationId = rs.getString("correlation_id");
            i.status = InstanceStatus.valueOf(rs.getString("status"));
            i.terminationReason = rs.getString("term_reason");
            i.error = rs.getString("error");
            i.contextJson = rs.getString("context");
            i.createdAt = rs.getLong("created_at");
            i.updatedAt = rs.getLong("updated_at");
            i.revision = rs.getLong("revision");
            return i;
        }

        private static Token readToken(ResultSet rs) throws SQLException {
            Token t = new Token();
            t.id = rs.getString("id");
            t.instanceId = rs.getString("instance_id");
            t.workflow = rs.getString("workflow");
            t.version = rs.getInt("version");
            t.nodeId = rs.getString("node_id");
            t.kind = NodeKind.valueOf(rs.getString("kind"));
            t.status = TokenStatus.valueOf(rs.getString("status"));
            t.activity = rs.getString("activity");
            t.queue = rs.getString("queue");
            t.attempt = rs.getInt("attempt");
            t.availableAt = rs.getLong("available_at");
            t.leaseOwner = rs.getString("lease_owner");
            t.leaseExpiresAt = rs.getLong("lease_expires");
            t.joinStack = rs.getString("join_stack");
            t.lastError = rs.getString("last_error");
            t.createdAt = rs.getLong("created_at");
            t.updatedAt = rs.getLong("updated_at");
            return t;
        }
    }
}
