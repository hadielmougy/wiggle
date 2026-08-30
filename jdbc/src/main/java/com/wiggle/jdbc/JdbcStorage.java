package com.wiggle.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.wiggle.core.*;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.CoordinatorStoreProvider;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.*;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.sql.*;
import java.util.*;
import java.util.function.Function;

/**
 * Shared-database store. This is what makes multi-node operation work: instance
 * mutual exclusion comes from SELECT ... FOR UPDATE, task hand-out from a
 * conditional UPDATE (compare-and-set on status), and leader election from the
 * wf_node heartbeat table.
 *
 * <p>The store body is dialect-neutral: it writes canonical, PostgreSQL-flavoured SQL and
 * defers every non-portable fragment to a {@link Dialect}. That single body backs PostgreSQL
 * and H2 (via {@code wiggle-postgres}), MySQL/MariaDB (via {@code wiggle-mysql}) and Oracle
 * (via {@code wiggle-oracle}). Connection pooling is provided by HikariCP.
 */
public final class JdbcStorage implements Storage, CoordinatorStoreProvider {

    private final Dialect dialect;
    private final HikariDataSource ds;
    private final String fingerprint;

    /** Explicit-dialect constructor used by the per-database modules. */
    public JdbcStorage(String url, String user, String password, int poolSize, Dialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.fingerprint = fingerprintOf(dialect.id() + ':' + url);
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        if (user != null) cfg.setUsername(user);
        if (password != null) cfg.setPassword(password);
        cfg.setMaximumPoolSize(Math.max(1, poolSize));
        // The engine drives its own transaction boundaries via inTx(); every borrowed connection
        // stays in manual-commit, read-committed mode.
        cfg.setAutoCommit(false);
        cfg.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        cfg.setPoolName("wiggle-" + dialect.id());
        this.ds = new HikariDataSource(cfg);
    }

    private Connection borrow() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            throw new StorageException("cannot obtain connection", e);
        }
    }

    /** Stable per-database identity: every node pointed at this JDBC URL shares it, distinct URLs differ. */
    @Override public String fingerprint() { return fingerprint; }

    private static String fingerprintOf(String s) {
        try {
            byte[] h = java.security.MessageDigest.getInstance("SHA-256").digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder("db-");
            for (int i = 0; i < 8; i++) sb.append(Character.forDigit((h[i] >> 4) & 0xf, 16)).append(Character.forDigit(h[i] & 0xf, 16));
            return sb.toString();
        } catch (Exception e) {
            return "db-" + Integer.toHexString(s.hashCode());   // never fails routing on a hash quirk
        }
    }

    private static void release(Connection c) {
        if (c == null) return;
        try { c.close(); } catch (SQLException ignored) { }  // returns the connection to the pool
    }

    /** One forward-only schema step. {@code sql} may hold several {@code ;}-separated statements. */
    public record Migration(int version, String name, String sql) { }

    /**
     * Ordered, forward-only schema history. Append new migrations; never edit or reorder an
     * already-released one. V1 is the baseline: it uses {@code IF NOT EXISTS} so a database
     * created before schema versioning existed adopts version tracking without re-creating
     * anything. Later migrations should be backward-compatible (add nullable columns / new
     * tables / new indexes) so a rolling multi-node deploy, where old and new nodes briefly
     * share the database, stays safe.
     */
    public static final List<Migration> MIGRATIONS = List.of(
            new Migration(1, "baseline", """
            CREATE TABLE IF NOT EXISTS wf_definition (
              name           VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              body           TEXT         NOT NULL,
              registered_at  BIGINT       NOT NULL,
              PRIMARY KEY (name, version)
            );
            CREATE TABLE IF NOT EXISTS wf_graph_node (
              workflow       VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              node_id        VARCHAR(64)  NOT NULL,
              kind           VARCHAR(16)  NOT NULL,
              name           VARCHAR(200),
              activity       VARCHAR(300),
              queue          VARCHAR(200),
              retry_json     TEXT,
              sleep_millis   BIGINT       NOT NULL,
              expected       INT          NOT NULL,
              success        INT          NOT NULL,
              reason         VARCHAR(200),
              is_start       INT          NOT NULL,
              PRIMARY KEY (workflow, version, node_id)
            );
            CREATE INDEX IF NOT EXISTS ix_graph_start ON wf_graph_node (workflow, version, is_start);
            CREATE TABLE IF NOT EXISTS wf_graph_edge (
              workflow       VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              from_node      VARCHAR(64)  NOT NULL,
              to_node        VARCHAR(64)  NOT NULL,
              cond           VARCHAR(16),
              ordinal        INT          NOT NULL,
              PRIMARY KEY (workflow, version, from_node, ordinal)
            );
            CREATE INDEX IF NOT EXISTS ix_graph_edge_from ON wf_graph_edge (workflow, version, from_node);
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
            """),
            // Backs QueueLagMonitor's countProcessedSince query (kind IN (...) AND status='DONE'
            // AND updated_at>?), which none of the baseline wf_token indexes cover -- without this,
            // that query is a full table scan that gets slower as DONE tokens accumulate.
            new Migration(2, "index-token-throughput", """
            CREATE INDEX IF NOT EXISTS ix_token_throughput ON wf_token (kind, status, updated_at);
            """),
            // Dynamic fan-out (forkEach): per-token branch payload, and the DYN_FORK node's
            // items/item context keys. All nullable, so the change is rolling-deploy safe.
            new Migration(3, "dynamic-fanout", """
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS payload TEXT;
            ALTER TABLE wf_graph_node ADD COLUMN IF NOT EXISTS items_key VARCHAR(200);
            ALTER TABLE wf_graph_node ADD COLUMN IF NOT EXISTS item_key VARCHAR(200);
            """),
            // Sub-workflows (the parent's waiting token) and recurring schedules.
            new Migration(4, "sub-workflows-and-schedules", """
            ALTER TABLE wf_instance ADD COLUMN IF NOT EXISTS parent_token_id VARCHAR(64);
            CREATE TABLE IF NOT EXISTS wf_schedule (
              id              VARCHAR(64)  PRIMARY KEY,
              workflow        VARCHAR(200) NOT NULL,
              interval_millis BIGINT       NOT NULL,
              context         TEXT         NOT NULL,
              next_fire_at    BIGINT       NOT NULL,
              created_at      BIGINT       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_schedule_due ON wf_schedule (next_fire_at);
            """),
            // Cron cadence for schedules (null = interval-based).
            new Migration(5, "cron-schedules", """
            ALTER TABLE wf_schedule ADD COLUMN IF NOT EXISTS cron VARCHAR(120);
            """),
            // Workflow is a unique key for schedules: concurrent creates for the same workflow
            // collapse onto one row instead of firing the same workflow on N duplicate cadences.
            new Migration(6, "schedule-workflow-unique", """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_schedule_workflow ON wf_schedule (workflow);
            """));

    /**
     * Coordinator schema. A coordinator runs on its <em>own</em> database (separate from any cell), so
     * this is a distinct baseline lineage -- it shares no {@code wf_schema_version} table with the cell
     * set. It carries its own {@code coord_leader} lease table: the coordinator is engine-free and does
     * its own leader election over this database, not via the cell {@code ClusterManager}.
     */
    public static final List<Migration> COORDINATOR_MIGRATIONS = List.of(
            new Migration(1, "coordinator-baseline", """
            CREATE TABLE IF NOT EXISTS coord_leader (
              id         VARCHAR(40)  PRIMARY KEY,
              holder     VARCHAR(200),
              expires_at BIGINT
            );
            CREATE TABLE IF NOT EXISTS coord_policy (
              namespace      VARCHAR(200) PRIMARY KEY,
              current_epoch  BIGINT       NOT NULL,
              epochs         TEXT         NOT NULL,
              revision       BIGINT       NOT NULL
            );
            CREATE TABLE IF NOT EXISTS coord_node (
              id                VARCHAR(64)  PRIMARY KEY,
              namespace         VARCHAR(200) NOT NULL,
              cell_id           VARCHAR(200) NOT NULL,
              endpoint          VARCHAR(300) NOT NULL,
              region            VARCHAR(120),
              engine_version    VARCHAR(60),
              cell_fingerprint  VARCHAR(200),
              config_generation BIGINT       NOT NULL,
              last_heartbeat    BIGINT       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_coord_node_ns ON coord_node (namespace, last_heartbeat);
            CREATE INDEX IF NOT EXISTS ix_coord_node_cell ON coord_node (namespace, cell_id);
            CREATE TABLE IF NOT EXISTS coord_definition (
              namespace      VARCHAR(200) NOT NULL,
              name           VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              hash           VARCHAR(64)  NOT NULL,
              registered_at  BIGINT       NOT NULL,
              PRIMARY KEY (namespace, name)
            );
            CREATE TABLE IF NOT EXISTS coord_namespace (
              namespace   VARCHAR(200) PRIMARY KEY,
              state       VARCHAR(30)  NOT NULL,
              scheme      VARCHAR(30),
              jdbc_url    VARCHAR(500),
              db_user     VARCHAR(120),
              secret_ref  VARCHAR(300),
              pool_size   INT,
              replicas    INT,
              region      VARCHAR(120),
              endpoint    VARCHAR(300),
              error       VARCHAR(1000),
              updated_at  BIGINT       NOT NULL
            );
            """),
            // Cell-identity binding: one fingerprint per (namespace, cell_id). The PRIMARY KEY makes the
            // duplicate-cell-id guard an atomic single-row claim (insert-or-compare), not a roster scan.
            new Migration(2, "coord-cell-binding", """
            CREATE TABLE IF NOT EXISTS coord_cell (
              namespace    VARCHAR(200) NOT NULL,
              cell_id      VARCHAR(200) NOT NULL,
              fingerprint  VARCHAR(200) NOT NULL,
              PRIMARY KEY (namespace, cell_id)
            );
            """));

    /** The coordinator store over this database: migrate the {@code coord_*} schema (idempotent, its own
     *  baseline lineage), then a store sharing this pool. This is the coordinator's business, not the
     *  engine's -- it is reached via {@link CoordinatorStoreProvider}, never via {@code Storage}. */
    @Override public CoordinatorStore coordinatorStore() {
        applyMigrations(COORDINATOR_MIGRATIONS, "coordinator-baseline");
        return new JdbcCoordinatorStore(ds);   // shares this store's pool; does not own/close it
    }

    /** Applies the cell schema. */
    @Override public void migrate() {
        applyMigrations(MIGRATIONS, "baseline");
    }

    private void applyMigrations(List<Migration> migrations, String baseline) {
        Connection c = borrow();
        try {
            runMigrations(c, migrations, dialect, baseline);
            c.commit();   // also releases the migration lock held for the duration
        } catch (SQLException e) {
            rollback(c);
            throw new StorageException("migration failed", e);
        } finally {
            release(c);
        }
    }

    /**
     * Applies every migration newer than the recorded schema version, in order, on the given
     * connection, translating each statement through {@code dialect}. Serialised across nodes by a
     * dialect-supplied migration lock (a transaction-scoped advisory lock on PostgreSQL; a no-op
     * elsewhere, where run-once version tracking is relied on). Runs in the caller's transaction and
     * does <em>not</em> commit -- the caller does, which keeps the lock held until the whole batch
     * lands atomically.
     */
    public static void runMigrations(Connection c, List<Migration> migrations, Dialect dialect) throws SQLException {
        runMigrations(c, migrations, dialect, null);
    }

    /**
     * As {@link #runMigrations(Connection, List, Dialect)}, but if {@code expectedBaseline} is
     * non-null it first verifies the recorded V1 name matches -- so migrating a database whose
     * baseline belongs to the other role (a coordinator pointed at a cell's DB, or vice versa) fails
     * fast instead of silently skipping every migration because the version counter is already ahead.
     */
    public static void runMigrations(Connection c, List<Migration> migrations, Dialect dialect,
                                     String expectedBaseline) throws SQLException {
        dialect.acquireMigrationLock(c);
        try (Statement st = c.createStatement()) {
            execDdl(st, dialect, "CREATE TABLE IF NOT EXISTS wf_schema_version (" +
                    "version INT PRIMARY KEY, name VARCHAR(200) NOT NULL, applied_at BIGINT NOT NULL)");
        }
        if (expectedBaseline != null) {
            try (Statement st = c.createStatement();
                 ResultSet rs = st.executeQuery("SELECT name FROM wf_schema_version WHERE version=1")) {
                if (rs.next()) {
                    String existing = rs.getString(1);
                    if (existing != null && !existing.equals(expectedBaseline)) {
                        throw new SQLException("schema baseline mismatch: this database was initialised as '"
                                + existing + "' but is being migrated as '" + expectedBaseline
                                + "'. A coordinator must use its own database, separate from any cell.");
                    }
                }
            }
        }
        int current = 0;
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT COALESCE(MAX(version),0) FROM wf_schema_version")) {
            if (rs.next()) current = rs.getInt(1);
        }
        for (Migration m : migrations) {
            if (m.version() <= current) continue;
            try (Statement st = c.createStatement()) {
                for (String stmt : m.sql().split(";")) {
                    if (!stmt.isBlank()) execDdl(st, dialect, stmt);
                }
            }
            try (PreparedStatement ins = c.prepareStatement(
                    "INSERT INTO wf_schema_version (version,name,applied_at) VALUES (?,?,?)")) {
                ins.setInt(1, m.version());
                ins.setString(2, m.name());
                ins.setLong(3, System.currentTimeMillis());
                ins.executeUpdate();
            }
        }
    }

    /**
     * Runs one dialect-translated DDL statement, tolerating an "already exists" error the dialect
     * deems benign. Needed for Oracle, which has no {@code IF NOT EXISTS} on older versions and
     * auto-commits DDL, so a restart or a partially-applied migration can re-encounter an object
     * that is already there. Any other error propagates.
     */
    private static void execDdl(Statement st, Dialect dialect, String canonicalSql) throws SQLException {
        try {
            st.execute(dialect.ddl(canonicalSql));
        } catch (SQLException e) {
            if (!dialect.isBenignMigrationError(e)) throw e;
        }
    }

    @Override public <R> R inTx(Function<Tx, R> work) {
        Connection c = borrow();
        try {
            R r = work.apply(new JdbcTx(c, dialect));
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
        ds.close();
    }

    public static final class StorageException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public StorageException(String m, Throwable c) { super(m, c); }
    }

    private static final class JdbcTx implements Tx {
        private final Connection c;
        private final Dialect dialect;

        JdbcTx(Connection c, Dialect dialect) { this.c = c; this.dialect = dialect; }

        private PreparedStatement ps(String sql) throws SQLException { return c.prepareStatement(sql); }

        private static StorageException wrap(SQLException e) { return new StorageException(e.getMessage(), e); }

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

        @Override public void putDefinition(String name, int version, String json) {
            // The version is a content hash of the topology, so an existing (name,version) row
            // is byte-for-byte identical and re-registration is a genuine no-op. insertIgnore makes
            // that idempotent atomically -- unlike a DELETE-then-INSERT, it leaves no window in which
            // two nodes registering the same graph collide on the primary key. Oracle has no inline
            // ignore, so a genuine concurrent duplicate surfaces as a duplicate-key error we swallow.
            try (PreparedStatement ins = ps(dialect.insertIgnore("INSERT INTO wf_definition " +
                    "(name,version,body,registered_at) VALUES (?,?,?,?)", "registered_at"))) {
                ins.setString(1, name); ins.setInt(2, version); ins.setString(3, json);
                ins.setLong(4, System.currentTimeMillis());
                ins.executeUpdate();
            } catch (SQLException e) {
                if (!dialect.isDuplicateKey(e)) throw wrap(e);
            }
        }

        @Override public void putGraph(WorkflowDefinition def) {
            // The (name,version) blob insert already ignored conflicts for idempotency; guard the
            // graph rows the same way so a re-registration of the same content hash is a clean no-op
            // even if two nodes race.
            if (graphExists(def.name(), def.version())) return;
            try (PreparedStatement node = ps(dialect.insertIgnore("INSERT INTO wf_graph_node " +
                    "(workflow,version,node_id,kind,name,activity,queue,retry_json,sleep_millis,expected,success,reason,is_start," +
                    "items_key,item_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", "kind"));
                 PreparedStatement edge = ps(dialect.insertIgnore("INSERT INTO wf_graph_edge " +
                    "(workflow,version,from_node,to_node,cond,ordinal) VALUES (?,?,?,?,?,?)", "to_node"))) {
                for (Node n : def.nodes().values()) {
                    node.setString(1, def.name()); node.setInt(2, def.version()); node.setString(3, n.id());
                    node.setString(4, n.kind().name()); node.setString(5, n.name()); node.setString(6, n.activity());
                    node.setString(7, n.queue());
                    node.setString(8, n.retry() == null ? null : Json.write(n.retry().toJson()));
                    node.setLong(9, n.sleepMillis()); node.setInt(10, n.expected());
                    node.setInt(11, n.success() ? 1 : 0); node.setString(12, n.reason());
                    node.setInt(13, n.id().equals(def.startNode()) ? 1 : 0);
                    node.setString(14, n.itemsKey()); node.setString(15, n.itemKey());
                    node.addBatch();
                    for (Edge e : edgesOf(n)) {
                        edge.setString(1, def.name()); edge.setInt(2, def.version()); edge.setString(3, n.id());
                        edge.setString(4, e.to); edge.setString(5, e.condition); edge.setInt(6, e.ordinal);
                        edge.addBatch();
                    }
                }
                node.executeBatch();
                edge.executeBatch();
            } catch (SQLException e) {
                if (!dialect.isDuplicateKey(e)) throw wrap(e);
            }
        }

        private boolean graphExists(String workflow, int version) {
            try (PreparedStatement p = ps("SELECT 1 FROM wf_graph_node WHERE workflow=? AND version=? " +
                    dialect.firstRow())) {
                p.setString(1, workflow); p.setInt(2, version);
                try (ResultSet rs = p.executeQuery()) { return rs.next(); }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            try (PreparedStatement p = ps("SELECT kind,name,activity,queue,retry_json,sleep_millis,expected,success,reason," +
                    "items_key,item_key FROM wf_graph_node WHERE workflow=? AND version=? AND node_id=?")) {
                p.setString(1, workflow); p.setInt(2, version); p.setString(3, nodeId);
                try (ResultSet rs = p.executeQuery()) {
                    if (!rs.next()) return Optional.empty();
                    NodeKind kind = NodeKind.valueOf(rs.getString(1));
                    String name = rs.getString(2), activity = rs.getString(3), queue = rs.getString(4);
                    String retryJson = rs.getString(5);
                    RetryPolicy retry = retryJson == null ? null : RetryPolicy.fromJson(Json.parse(retryJson));
                    long sleep = rs.getLong(6);
                    int expected = rs.getInt(7);
                    boolean success = rs.getInt(8) != 0;
                    String reason = rs.getString(9);
                    String itemsKey = rs.getString(10);
                    String itemKey = rs.getString(11);
                    return Optional.of(assemble(workflow, version, nodeId, kind, name, activity, queue,
                            retry, sleep, expected, success, reason, itemsKey, itemKey));
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        /** Reads a node's outgoing edges and folds them back into the node's typed next/altNext/branches. */
        private Node assemble(String workflow, int version, String id, NodeKind kind, String name, String activity,
                              String queue, RetryPolicy retry, long sleep, int expected, boolean success, String reason,
                              String itemsKey, String itemKey) {
            EdgeTargets targets = new EdgeTargets(kind);
            try (PreparedStatement p = ps("SELECT to_node,cond FROM wf_graph_edge " +
                    "WHERE workflow=? AND version=? AND from_node=? ORDER BY ordinal")) {
                p.setString(1, workflow); p.setInt(2, version); p.setString(3, id);
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) targets.absorb(rs.getString(1), rs.getString(2));
                }
            } catch (SQLException e) { throw wrap(e); }
            return new Node(id, kind, name, activity, queue, retry, sleep, targets.next, targets.altNext,
                    List.copyOf(targets.branches), expected, success, reason, itemsKey, itemKey);
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
            try (PreparedStatement p = ps("SELECT node_id FROM wf_graph_node " +
                    "WHERE workflow=? AND version=? AND is_start=1")) {
                p.setString(1, workflow); p.setInt(2, version);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
                }
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
                    "(id,workflow,version,correlation_id,status,term_reason,error,context,created_at,updated_at,revision," +
                    "parent_token_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")) {
                p.setString(1, i.id); p.setString(2, i.workflow); p.setInt(3, i.version);
                p.setString(4, i.correlationId); p.setString(5, i.status.name());
                p.setString(6, i.terminationReason); p.setString(7, i.error); p.setString(8, i.contextJson);
                p.setLong(9, i.createdAt); p.setLong(10, i.updatedAt); p.setLong(11, i.revision);
                p.setString(12, i.parentTokenId);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public Optional<Instance> lockInstance(String id) { return loadInstance(id, true); }

        @Override public Optional<Instance> findInstance(String id) { return loadInstance(id, false); }

        private Optional<Instance> loadInstance(String id, boolean forUpdate) {
            String sql;
            if (forUpdate) {
                String hint = dialect.forUpdateHint(), suffix = dialect.forUpdateSuffix();
                sql = "SELECT * FROM wf_instance" + (hint.isEmpty() ? "" : " " + hint) + " WHERE id=?"
                        + (suffix.isEmpty() ? "" : " " + suffix);
            } else {
                sql = "SELECT * FROM wf_instance WHERE id=?";
            }
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
            try (PreparedStatement p = ps(dialect.limit(sql.toString()))) {
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
                    "activity,queue,attempt,available_at,lease_owner,lease_expires,join_stack,last_error,created_at,updated_at," +
                    "payload) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                bindToken(p, t);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        /** Binds parameters 1..18 in wf_token insert column order. */
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
            p.setString(18, t.payloadJson);
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
                    "attempt=?,available_at=?,lease_owner=?,lease_expires=?,join_stack=?,last_error=?,updated_at=?," +
                    "payload=? WHERE id=?")) {
                p.setString(1, t.nodeId); p.setString(2, t.kind.name()); p.setString(3, t.status.name());
                p.setString(4, t.activity); p.setString(5, t.queue); p.setInt(6, t.attempt);
                p.setLong(7, t.availableAt); p.setString(8, t.leaseOwner); p.setLong(9, t.leaseExpiresAt);
                p.setString(10, t.joinStack == null ? "" : t.joinStack); p.setString(11, t.lastError);
                p.setLong(12, t.updatedAt); p.setString(13, t.payloadJson); p.setString(14, t.id);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Token> claimTasks(String workerId, Set<String> queues, int max, long now, long leaseUntil) {
            if (dialect.supportsSkipLocked() && dialect.supportsReturning()) {
                return claimSkipLockedReturning(workerId, queues, max, now, leaseUntil);
            }
            if (dialect.supportsSkipLocked()) {
                return claimSkipLockedSelect(workerId, queues, max, now, leaseUntil);
            }
            return claimCompareAndSet(workerId, queues, max, now, leaseUntil);
        }

        /**
         * Atomic claim for PostgreSQL: lock up to {@code max} dispatchable rows with
         * SKIP LOCKED -- which steps over rows another worker already holds instead of
         * blocking on them -- and update them in the same statement. Because no
         * transaction ever waits on a row locked by another, concurrent claims across
         * many workers and nodes cannot deadlock, and none of them collide on a row.
         */
        private List<Token> claimSkipLockedReturning(String workerId, Set<String> queues, int max, long now, long leaseUntil) {
            StringBuilder pick = new StringBuilder(
                    "SELECT id FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=?");
            if (queues != null && !queues.isEmpty()) {
                pick.append(" AND queue IN (").append("?,".repeat(queues.size() - 1)).append("?)");
            }
            pick.append(" ORDER BY available_at, id LIMIT ? FOR UPDATE SKIP LOCKED");
            String sql = "UPDATE wf_token SET status='RUNNING',lease_owner=?,lease_expires=?,updated_at=? " +
                    "WHERE id IN (" + pick + ") RETURNING *";
            try (PreparedStatement p = ps(sql)) {
                int idx = 1;
                p.setString(idx++, workerId);   // SET lease_owner
                p.setLong(idx++, leaseUntil);   // SET lease_expires
                p.setLong(idx++, now);          // SET updated_at
                p.setLong(idx++, now);          // WHERE available_at<=?
                if (queues != null && !queues.isEmpty()) for (String q : queues) p.setString(idx++, q);
                p.setInt(idx, max);             // LIMIT
                try (ResultSet rs = p.executeQuery()) {
                    List<Token> out = new ArrayList<>();
                    while (rs.next()) out.add(readToken(rs));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        /**
         * Two-step claim for dialects that have SKIP LOCKED but not RETURNING (MySQL): lock the
         * candidate rows with SELECT ... FOR UPDATE SKIP LOCKED, then flip them to RUNNING in the
         * same transaction. The lock the SELECT took guarantees no other worker can claim the same
         * rows before the UPDATE commits.
         */
        private List<Token> claimSkipLockedSelect(String workerId, Set<String> queues, int max, long now, long leaseUntil) {
            StringBuilder sel = new StringBuilder(
                    "SELECT * FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=?");
            if (queues != null && !queues.isEmpty()) {
                sel.append(" AND queue IN (").append("?,".repeat(queues.size() - 1)).append("?)");
            }
            sel.append(" ORDER BY available_at, id LIMIT ? FOR UPDATE SKIP LOCKED");
            List<Token> picked = new ArrayList<>();
            try (PreparedStatement p = ps(dialect.limit(sel.toString()))) {
                int idx = 1;
                p.setLong(idx++, now);
                if (queues != null && !queues.isEmpty()) for (String q : queues) p.setString(idx++, q);
                p.setInt(idx, max);
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) picked.add(readToken(rs));
                }
            } catch (SQLException e) { throw wrap(e); }
            if (picked.isEmpty()) return picked;
            try (PreparedStatement upd = ps("UPDATE wf_token SET status='RUNNING',lease_owner=?,lease_expires=?," +
                    "updated_at=? WHERE id=?")) {
                for (Token t : picked) {
                    upd.setString(1, workerId); upd.setLong(2, leaseUntil); upd.setLong(3, now); upd.setString(4, t.id);
                    upd.addBatch();
                    t.status = TokenStatus.RUNNING;
                    t.leaseOwner = workerId;
                    t.leaseExpiresAt = leaseUntil;
                    t.updatedAt = now;
                }
                upd.executeBatch();
            } catch (SQLException e) { throw wrap(e); }
            return picked;
        }

        /** Portable fallback (H2, Oracle): over-fetch candidates, then compare-and-set each. */
        private List<Token> claimCompareAndSet(String workerId, Set<String> queues, int max, long now, long leaseUntil) {
            StringBuilder sql = new StringBuilder(
                    "SELECT * FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=?");
            if (queues != null && !queues.isEmpty()) {
                sql.append(" AND queue IN (").append("?,".repeat(queues.size() - 1)).append("?)");
            }
            sql.append(" ORDER BY available_at, id LIMIT ?");
            List<Token> candidates = new ArrayList<>();
            try (PreparedStatement p = ps(dialect.limit(sql.toString()))) {
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

        @Override public List<Token> pendingSignals(int max) {
            try (PreparedStatement p = ps(dialect.limit("SELECT * FROM wf_token WHERE status='AWAITING' AND kind='SIGNAL' " +
                    "ORDER BY created_at LIMIT ?"))) {
                p.setInt(1, max);
                try (ResultSet rs = p.executeQuery()) {
                    List<Token> out = new ArrayList<>();
                    while (rs.next()) out.add(readToken(rs));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Token> dueSignals(long now, int max) {
            return query("SELECT * FROM wf_token WHERE status='AWAITING' AND kind='SIGNAL' " +
                    "AND available_at>0 AND available_at<=? ORDER BY available_at LIMIT ?", now, max);
        }

        @Override public List<String> childInstanceIds(String parentInstanceId) {
            try (PreparedStatement p = ps("SELECT id FROM wf_instance WHERE parent_token_id IN " +
                    "(SELECT id FROM wf_token WHERE instance_id=?) ORDER BY id")) {
                p.setString(1, parentInstanceId);
                try (ResultSet rs = p.executeQuery()) {
                    List<String> out = new ArrayList<>();
                    while (rs.next()) out.add(rs.getString(1));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void putSchedule(Rows.Schedule s) {
            // Upsert by id: the engine reuses the existing id when a schedule for the same
            // workflow already exists, so a re-create updates the row rather than duplicating it.
            // The seven bound parameters are identical across dialects; only the SQL text differs.
            try (PreparedStatement p = ps(dialect.scheduleUpsert())) {
                p.setString(1, s.id); p.setString(2, s.workflow); p.setLong(3, s.intervalMillis);
                p.setString(4, s.cron); p.setString(5, s.contextJson);
                p.setLong(6, s.nextFireAt); p.setLong(7, s.createdAt);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public java.util.Optional<Rows.Schedule> scheduleByWorkflow(String workflow) {
            try (PreparedStatement p = ps("SELECT * FROM wf_schedule WHERE workflow=?")) {
                p.setString(1, workflow);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next() ? java.util.Optional.of(readSchedule(rs)) : java.util.Optional.empty();
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void deleteSchedule(String id) {
            try (PreparedStatement p = ps("DELETE FROM wf_schedule WHERE id=?")) {
                p.setString(1, id);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Rows.Schedule> schedules() {
            try (PreparedStatement p = ps("SELECT * FROM wf_schedule ORDER BY id");
                 ResultSet rs = p.executeQuery()) {
                List<Rows.Schedule> out = new ArrayList<>();
                while (rs.next()) out.add(readSchedule(rs));
                return out;
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Rows.Schedule> dueSchedules(long now, int max) {
            try (PreparedStatement p = ps(dialect.limit("SELECT * FROM wf_schedule WHERE next_fire_at<=? " +
                    "ORDER BY next_fire_at LIMIT ?"))) {
                p.setLong(1, now); p.setInt(2, max);
                try (ResultSet rs = p.executeQuery()) {
                    List<Rows.Schedule> out = new ArrayList<>();
                    while (rs.next()) out.add(readSchedule(rs));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public boolean claimSchedule(String id, long expectedFireAt, long nextFireAt) {
            try (PreparedStatement p = ps("UPDATE wf_schedule SET next_fire_at=? WHERE id=? AND next_fire_at=?")) {
                p.setLong(1, nextFireAt); p.setString(2, id); p.setLong(3, expectedFireAt);
                return p.executeUpdate() == 1;
            } catch (SQLException e) { throw wrap(e); }
        }

        private static Rows.Schedule readSchedule(ResultSet rs) throws SQLException {
            Rows.Schedule s = new Rows.Schedule();
            s.id = rs.getString("id");
            s.workflow = rs.getString("workflow");
            s.intervalMillis = rs.getLong("interval_millis");
            s.cron = rs.getString("cron");
            s.contextJson = rs.getString("context");
            s.nextFireAt = rs.getLong("next_fire_at");
            s.createdAt = rs.getLong("created_at");
            return s;
        }

        @Override public Rows.QueueDepth queueDepth(long now) {
            try (PreparedStatement p = ps("SELECT COUNT(*), COALESCE(MIN(available_at),0) FROM wf_token " +
                    "WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=?")) {
                p.setLong(1, now);
                try (ResultSet rs = p.executeQuery()) {
                    rs.next();
                    return new Rows.QueueDepth(rs.getInt(1), rs.getLong(2));
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public int countProcessedSince(long since) {
            try (PreparedStatement p = ps("SELECT COUNT(*) FROM wf_token " +
                    "WHERE kind IN ('TASK','PREDICATE') AND status='DONE' AND updated_at>?")) {
                p.setLong(1, since);
                try (ResultSet rs = p.executeQuery()) {
                    rs.next();
                    return rs.getInt(1);
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        private List<Token> query(String sql, long arg, int limit) {
            try (PreparedStatement p = ps(dialect.limit(sql))) {
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
            // ORDER BY is required for SQL Server's OFFSET/FETCH rewrite of LIMIT, and gives every
            // dialect a deterministic "oldest first" deletion order at no cost.
            try (PreparedStatement p = ps(dialect.limit(
                    "SELECT id FROM wf_instance WHERE status<>'RUNNING' AND updated_at<? ORDER BY updated_at LIMIT ?"))) {
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
            i.parentTokenId = rs.getString("parent_token_id");
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
            // Oracle stores the empty string as NULL, so the NOT-NULL '' sentinel comes back null;
            // normalise it here so the engine always sees a non-null join stack.
            String joinStack = rs.getString("join_stack");
            t.joinStack = joinStack == null ? "" : joinStack;
            t.lastError = rs.getString("last_error");
            t.payloadJson = rs.getString("payload");
            t.createdAt = rs.getLong("created_at");
            t.updatedAt = rs.getLong("updated_at");
            return t;
        }
    }
}
