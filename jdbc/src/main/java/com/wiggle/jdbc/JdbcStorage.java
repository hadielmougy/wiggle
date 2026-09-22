package com.wiggle.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.wiggle.core.*;
import com.wiggle.core.Doc;
import com.wiggle.server.store.PayloadCodec;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.*;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
 * and H2 (both via {@code wiggle-postgres}). Connection pooling is provided by HikariCP.
 */
public final class JdbcStorage implements Storage {

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
            CREATE INDEX IF NOT EXISTS ix_instance_correlation ON wf_instance (correlation_id);
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
            // Dynamic fan-out (forEach): per-token branch payload, and the DYN_FORK node's
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
            """),
            // doWhile loop budgets: max true-evaluations of a loop guard before the instance fails
            // (-1 = engine default). Nullable, so the change is rolling-deploy safe.
            new Migration(7, "loop-budgets", """
            ALTER TABLE wf_graph_node ADD COLUMN IF NOT EXISTS loop_budget INT;
            """),
            // Saga compensation: the compensable marker on a step node, and the per-instance
            // compensation log (input/result snapshots per compensable completion). Nullable /
            // additive, rolling-deploy safe.
            new Migration(8, "compensation", """
            ALTER TABLE wf_graph_node ADD COLUMN IF NOT EXISTS compensable INT;
            CREATE TABLE IF NOT EXISTS wf_comp_log (
              instance_id  VARCHAR(64)  NOT NULL,
              seq          BIGINT       NOT NULL,
              node_id      VARCHAR(64)  NOT NULL,
              activity     VARCHAR(300) NOT NULL,
              queue        VARCHAR(200),
              input_json   TEXT,
              result_json  TEXT,
              compensated  INT          NOT NULL,
              PRIMARY KEY (instance_id, seq)
            );
            """),
            // Instance ids gained an optional cell label ({ns}.c{cell}.e{epoch}.s{shard}.{ulid}), so
            // 64 characters stopped being comfortable: the fixed part is 31 and the rest is split
            // between a namespace and a cell id that operators choose. Widening is additive -- every
            // existing value still fits -- so old and new nodes can share the database through a
            // rolling deploy. Only the three columns that actually hold an INSTANCE id move; node,
            // token and schedule ids are generated and bounded, and widening those would be noise.
            //
            // On a large wf_token this rewrites the table and its indexes under a lock, so run it in
            // a maintenance window (or ahead of the deploy with WIGGLE_MIGRATE_ONLY=true) when the
            // table is big.
            new Migration(9, "wider-instance-ids", """
            ALTER TABLE wf_instance ALTER COLUMN id TYPE VARCHAR(128);
            ALTER TABLE wf_token    ALTER COLUMN instance_id TYPE VARCHAR(128);
            ALTER TABLE wf_comp_log ALTER COLUMN instance_id TYPE VARCHAR(128);
            """),
            // join_stack is one group id (a token id, plus "#width" for a dynamic fan-out) per
            // enclosing fork/forEach, comma-separated -- so its length is proportional to NESTING
            // DEPTH, and VARCHAR(1000) capped nesting at roughly 38 levels with a raw
            // "value too long" from the driver. Nothing else bounds depth: the in-memory store
            // holds a plain String, which is why only JDBC deployments hit it. TEXT, like the
            // other unbounded columns (payload, context, error).
            new Migration(10, "unbounded-join-stack", """
            ALTER TABLE wf_token ALTER COLUMN join_stack TYPE TEXT;
            """),
            // Versions became author-declared rather than a content hash of the topology, so
            // (name, version) no longer implies one graph on its own. The fingerprint is what
            // holds a published version immutable: re-registering it with a different graph is
            // refused. Nullable -- a row written before this migration has no fingerprint and is
            // treated as unknown, not as a mismatch.
            new Migration(11, "definition-fingerprint", """
            ALTER TABLE wf_definition ADD COLUMN IF NOT EXISTS fingerprint VARCHAR(64);
            ALTER TABLE wf_definition ADD COLUMN IF NOT EXISTS fingerprint_algo VARCHAR(32);
            """),
            // Which comp-log entry a compensation token settles. It used to ride inside the token's
            // payload JSON under a reserved key, so telling a compensation token from a forward one
            // meant a substring scan of that JSON on the completion path. Nullable and additive:
            // a token written before this migration has no seq, which is what a forward token has.
            new Migration(12, "compensation-seq-column", """
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS comp_seq BIGINT;
            """),
            // A combine node's shape used to ride inside items_key as JSON -- an array of arm names
            // for a fork, a quoted string for a forEach -- so the engine re-parsed it, and told the
            // two apart by which JSON type came back. Own columns instead; items_key goes back to
            // meaning only what a forEach fan-out reads. Nullable: a graph written before this keeps
            // its items_key, which the reader still decodes.
            new Migration(13, "combine-columns", """
            ALTER TABLE wf_graph_node ADD COLUMN IF NOT EXISTS arm_names TEXT;
            ALTER TABLE wf_graph_node ADD COLUMN IF NOT EXISTS collect_key VARCHAR(200);
            """), new Migration(14, "add-barrier-index", """
                CREATE INDEX IF NOT EXISTS ix_token_barrier ON wf_token (instance_id, node_id, status);
            """),
            // Observed execution. A step's own clock (started_at/finished_at) and the order it was
            // reported in (seq), which the judge uses as the tie-break between steps whose clocks
            // agree; nullable, and duration stats read only rows that have both times. An observed
            // run's settle time on the instance (null on every other instance, so the sweep's
            // index touches only observed runs). The anomaly table records where an observed run
            // departed from its topology.
            new Migration(15, "observed-execution", """
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS started_at BIGINT;
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS finished_at BIGINT;
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS seq BIGINT;
            CREATE INDEX IF NOT EXISTS ix_token_timed ON wf_token (workflow, version, status, finished_at);
            ALTER TABLE wf_instance ADD COLUMN IF NOT EXISTS settle_at BIGINT;
            CREATE INDEX IF NOT EXISTS ix_instance_settle ON wf_instance (status, settle_at);
            CREATE TABLE IF NOT EXISTS wf_anomaly (
              id             VARCHAR(64)  PRIMARY KEY,
              instance_id    VARCHAR(128) NOT NULL,
              workflow       VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              kind           VARCHAR(32)  NOT NULL,
              expected_node  VARCHAR(64),
              reported_node  VARCHAR(64),
              detail         TEXT,
              observed_at    BIGINT       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_anomaly_instance ON wf_anomaly (instance_id);
            CREATE INDEX IF NOT EXISTS ix_anomaly_workflow ON wf_anomaly (workflow, observed_at);
            """),
            // The event log: one append per instance lifecycle transition, in the transaction that
            // made it, so the log never disagrees with wf_instance. Delivery is pull-and-ack: a
            // consumer is a named cursor into the seq order and an ack advances it (cumulative),
            // never touching rows. seq is store-generated, so a reader may see seq N while an
            // earlier, uncommitted transaction still holds N-1: the feed must not serve past the
            // oldest in-flight append (a short visibility grace on created_at suffices).
            // Retention trims below min(acked_seq) with an age cap, leader-only, so an abandoned
            // cursor cannot pin the log forever. payload_ver is the persisted envelope version.
            new Migration(16, "event-log", """
            CREATE TABLE IF NOT EXISTS wf_event (
              seq            BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
              instance_id    VARCHAR(128) NOT NULL,
              workflow       VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              correlation_id VARCHAR(200),
              type           VARCHAR(32)  NOT NULL,
              payload_ver    INT          NOT NULL,
              payload        TEXT,
              created_at     BIGINT       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_event_instance ON wf_event (instance_id);
            CREATE INDEX IF NOT EXISTS ix_event_created ON wf_event (created_at);
            CREATE TABLE IF NOT EXISTS wf_event_cursor (
              consumer       VARCHAR(200) PRIMARY KEY,
              acked_seq      BIGINT       NOT NULL,
              last_seen      BIGINT       NOT NULL,
              created_at     BIGINT       NOT NULL
            );
            """));

    /** How {@link #migrate()} treats pending schema changes. */
    public enum MigrationMode {
        /** Apply any pending migrations (the default). */ APPLY,
        /** Apply nothing; fail if the schema is behind or has drifted — for a DBA/CI-owned schema. */ VERIFY
    }

    /**
     * Applies the cell schema. Honours {@code WIGGLE_SCHEMA_MODE}: {@code apply} (default) migrates,
     * {@code verify} only checks that the schema is current and un-drifted and fails otherwise (for
     * deployments where a DBA or CI pipeline — not the app — owns DDL). Run a one-shot migration
     * with the distribution's {@code WIGGLE_MIGRATE_ONLY=true}, then run the app in {@code verify}.
     */
    @Override public void migrate() {
        applyMigrations(MIGRATIONS, "baseline", modeFromEnv());
    }

    private static MigrationMode modeFromEnv() {
        // A one-shot migration job (WIGGLE_MIGRATE_ONLY) forces APPLY even if the app's env pins verify.
        if (Boolean.getBoolean("wiggle.schema.forceApply")) return MigrationMode.APPLY;
        String m = System.getenv("WIGGLE_SCHEMA_MODE");
        return m != null && m.trim().equalsIgnoreCase("verify") ? MigrationMode.VERIFY : MigrationMode.APPLY;
    }

    private void applyMigrations(List<Migration> migrations, String baseline, MigrationMode mode) {
        Connection c = borrow();
        try {
            runMigrations(c, migrations, dialect, baseline, mode);
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
        runMigrations(c, migrations, dialect, expectedBaseline, MigrationMode.APPLY);
    }

    /**
     * As above, with an explicit {@link MigrationMode}. In {@code VERIFY} mode nothing is applied:
     * it fails if any migration is pending (the schema is behind — a migration step must run first)
     * or if an already-applied migration's checksum no longer matches the code (drift — a released
     * migration was edited). Each applied migration records a checksum of its source, so drift is
     * caught on every subsequent startup, in either mode.
     */
    public static void runMigrations(Connection c, List<Migration> migrations, Dialect dialect,
                                     String expectedBaseline, MigrationMode mode) throws SQLException {
        dialect.acquireMigrationLock(c);
        try (Statement st = c.createStatement()) {
            execDdl(st, "CREATE TABLE IF NOT EXISTS wf_schema_version (" +
                    "version INT PRIMARY KEY, name VARCHAR(200) NOT NULL, applied_at BIGINT NOT NULL, " +
                    "checksum VARCHAR(64))");
        }
        ensureChecksumColumn(c, dialect);
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
        // Load applied versions -> stored checksum.
        Map<Integer, String> applied = new HashMap<>();
        try (Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT version, checksum FROM wf_schema_version")) {
            while (rs.next()) applied.put(rs.getInt(1), rs.getString(2));
        }
        int current = applied.keySet().stream().mapToInt(Integer::intValue).max().orElse(0);

        // Drift check: every already-applied migration still in the code must match its recorded
        // checksum. A null stored checksum is a legacy row (predates this feature) — in APPLY mode we
        // backfill it from the code (adopting the source as truth); in VERIFY mode we leave it be.
        for (Migration m : migrations) {
            if (!applied.containsKey(m.version())) continue;
            String want = checksum(m);
            String have = applied.get(m.version());
            if (have == null) {
                if (mode == MigrationMode.APPLY) {
                    try (PreparedStatement up = c.prepareStatement(
                            "UPDATE wf_schema_version SET checksum = ? WHERE version = ?")) {
                        up.setString(1, want);
                        up.setInt(2, m.version());
                        up.executeUpdate();
                    }
                }
            } else if (!have.equals(want)) {
                throw new SQLException("schema drift: migration V" + m.version() + " ('" + m.name()
                        + "') was applied with a different definition than the current code (checksum "
                        + have + " != " + want + "). Never edit a released migration — add a new one.");
            }
        }

        List<Migration> pending = migrations.stream().filter(m -> m.version() > current).toList();
        if (mode == MigrationMode.VERIFY) {
            if (!pending.isEmpty()) {
                throw new SQLException("schema is behind: " + pending.size() + " migration(s) pending (up to V"
                        + pending.get(pending.size() - 1).version() + "). Running in verify mode (WIGGLE_SCHEMA_MODE"
                        + "=verify) — apply them out of band first (WIGGLE_MIGRATE_ONLY=true).");
            }
            return;
        }
        for (Migration m : pending) {
            try (Statement st = c.createStatement()) {
                for (String stmt : m.sql().split(";")) {
                    if (!stmt.isBlank()) execDdl(st, stmt);
                }
            }
            try (PreparedStatement ins = c.prepareStatement(
                    "INSERT INTO wf_schema_version (version,name,applied_at,checksum) VALUES (?,?,?,?)")) {
                ins.setInt(1, m.version());
                ins.setString(2, m.name());
                ins.setLong(3, System.currentTimeMillis());
                ins.setString(4, checksum(m));
                ins.executeUpdate();
            }
        }
    }

    /** Adds the {@code checksum} column to a pre-existing (legacy) version table that lacks it. */
    private static void ensureChecksumColumn(Connection c, Dialect dialect) throws SQLException {
        DatabaseMetaData md = c.getMetaData();
        for (String table : new String[]{"wf_schema_version", "WF_SCHEMA_VERSION"}) {
            try (ResultSet rs = md.getColumns(null, null, table, "checksum")) { if (rs.next()) return; }
            try (ResultSet rs = md.getColumns(null, null, table, "CHECKSUM")) { if (rs.next()) return; }
        }
        try (Statement st = c.createStatement()) {
            execDdl(st, "ALTER TABLE wf_schema_version ADD checksum VARCHAR(64)");
        }
    }

    /** SHA-256 (hex) of a migration's source — dialect-independent, so it's stable across backends. */
    static String checksum(Migration m) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] h = md.digest((m.version() + "\n" + m.name() + "\n" + m.sql()).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(h.length * 2);
            for (byte b : h) sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e); // never on a standard JRE
        }
    }

    /** Runs one DDL statement. Both dialects take {@code IF NOT EXISTS}, so a re-run is idempotent
     *  and any error here is real. */
    private static void execDdl(Statement st, String sql) throws SQLException {
        st.execute(sql);
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

        @Override public void putDefinition(String name, int version, String json,
                                            String fingerprint, String fingerprintAlgo) {
            // insertIgnore rather than DELETE-then-INSERT: it leaves no window in which two nodes
            // registering the same new version collide on the primary key. The isDuplicateKey catch
            // covers a backend whose ignore is not inline.
            try (PreparedStatement ins = ps(dialect.insertIgnore("INSERT INTO wf_definition " +
                    "(name,version,body,registered_at,fingerprint,fingerprint_algo) VALUES (?,?,?,?,?,?)"))) {
                ins.setString(1, name); ins.setInt(2, version); ins.setString(3, json);
                ins.setLong(4, System.currentTimeMillis());
                ins.setString(5, fingerprint); ins.setString(6, fingerprintAlgo);
                ins.executeUpdate();
            } catch (SQLException e) {
                if (!dialect.isDuplicateKey(e)) throw wrap(e);
            }
        }

        @Override public void replaceDefinition(String name, int version, String json,
                                                String fingerprint, String fingerprintAlgo) {
            try (PreparedStatement p = ps("UPDATE wf_definition SET body=?, registered_at=?, " +
                    "fingerprint=?, fingerprint_algo=? WHERE name=? AND version=?")) {
                p.setString(1, json); p.setLong(2, System.currentTimeMillis());
                p.setString(3, fingerprint); p.setString(4, fingerprintAlgo);
                p.setString(5, name); p.setInt(6, version);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public Optional<StoredFingerprint> definitionFingerprint(String name, int version) {
            try (PreparedStatement p = ps(
                    "SELECT fingerprint, fingerprint_algo FROM wf_definition WHERE name=? AND version=? FOR UPDATE")) {
                p.setString(1, name); p.setInt(2, version);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next()
                            ? Optional.of(new StoredFingerprint(rs.getString(1), rs.getString(2)))
                            : Optional.empty();
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void putGraph(WorkflowDefinition def) {
            // The registry deletes these rows before a replacement, so anything still here is the
            // same graph: a no-op, even when two nodes register it at once.
            if (graphExists(def.name(), def.version())) return;
            try (PreparedStatement node = ps(dialect.insertIgnore("INSERT INTO wf_graph_node " +
                    "(workflow,version,node_id,kind,name,activity,queue,retry_json,sleep_millis,expected,success,reason,is_start," +
                    "items_key,item_key,loop_budget,compensable,arm_names,collect_key) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
                 PreparedStatement edge = ps(dialect.insertIgnore("INSERT INTO wf_graph_edge " +
                    "(workflow,version,from_node,to_node,cond,ordinal) VALUES (?,?,?,?,?,?)"))) {
                for (Node n : def.nodes().values()) {
                    node.setString(1, def.name()); node.setInt(2, def.version()); node.setString(3, n.id());
                    node.setString(4, n.kind().name()); node.setString(5, n.name()); node.setString(6, n.activity());
                    node.setString(7, n.queue());
                    node.setString(8, n.retry() == null ? null : Json.write(n.retry().toJson()));
                    node.setLong(9, n.sleepMillis()); node.setInt(10, n.expected());
                    node.setInt(11, n.success() ? 1 : 0); node.setString(12, n.reason());
                    node.setInt(13, n.id().equals(def.startNode()) ? 1 : 0);
                    node.setString(14, n.itemsKey()); node.setString(15, n.itemKey()); node.setInt(16, n.loopBudget());
                    node.setString(18, n.armNames().isEmpty() ? null : Json.write(n.armNames()));
                    node.setString(19, n.collectKey());
                    node.setInt(17, n.compensable() ? 1 : 0);
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
            try (PreparedStatement p = ps("SELECT 1 FROM wf_graph_node WHERE workflow=? AND version=? LIMIT 1")) {
                p.setString(1, workflow); p.setInt(2, version);
                try (ResultSet rs = p.executeQuery()) { return rs.next(); }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void deleteGraph(String workflow, int version) {
            deleteGraphRows("DELETE FROM wf_graph_edge WHERE workflow=? AND version=?", workflow, version);
            deleteGraphRows("DELETE FROM wf_graph_node WHERE workflow=? AND version=?", workflow, version);
        }

        private void deleteGraphRows(String sql, String workflow, int version) {
            try (PreparedStatement p = ps(sql)) {
                p.setString(1, workflow);
                p.setInt(2, version);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            try (PreparedStatement p = ps("SELECT kind,name,activity,queue,retry_json,sleep_millis,expected,success,reason," +
                    "items_key,item_key,loop_budget,compensable,arm_names,collect_key " +
                    "FROM wf_graph_node WHERE workflow=? AND version=? AND node_id=?")) {
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
                    int loopBudget = rs.getInt(12);   // NULL -> 0 (not a loop)
                    boolean compensable = rs.getInt(13) != 0;
                    Combine combine = Combine.of(kind, itemsKey, rs.getString(14), rs.getString(15));
                    return Optional.of(assemble(workflow, version, nodeId, kind, name, activity, queue,
                            retry, sleep, expected, success, reason, combine.itemsKey(), itemKey,
                            loopBudget, compensable, combine));
                }
            } catch (SQLException e) { throw wrap(e); }
        }

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
        private Node assemble(String workflow, int version, String id, NodeKind kind, String name, String activity,
                              String queue, RetryPolicy retry, long sleep, int expected, boolean success, String reason,
                              String itemsKey, String itemKey, int loopBudget, boolean compensable, Combine combine) {
            EdgeTargets targets = new EdgeTargets(kind);
            try (PreparedStatement p = ps("SELECT to_node,cond FROM wf_graph_edge " +
                    "WHERE workflow=? AND version=? AND from_node=? ORDER BY ordinal")) {
                p.setString(1, workflow); p.setInt(2, version); p.setString(3, id);
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) targets.absorb(rs.getString(1), rs.getString(2));
                }
            } catch (SQLException e) { throw wrap(e); }
            Node n = new Node(id, kind, name, activity, queue, retry, sleep, targets.next, targets.altNext,
                    List.copyOf(targets.branches), expected, success, reason, itemsKey, itemKey, loopBudget, false,
                    combine.armNames(), combine.collectKey());
            return compensable ? n.withCompensable() : n;
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
                    "SELECT MAX(version) FROM wf_definition WHERE name=?")) {
                p.setString(1, name);
                try (ResultSet rs = p.executeQuery()) {
                    if (!rs.next()) return Optional.empty();
                    int v = rs.getInt(1);
                    return rs.wasNull() ? Optional.empty() : Optional.of(v);
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

        private static final String INSERT_INSTANCE = "INSERT INTO wf_instance " +
                "(id,workflow,version,correlation_id,status,term_reason,error,context,created_at,updated_at,revision," +
                "parent_token_id,settle_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

        @Override public void insertInstance(Instance i) {
            try (PreparedStatement p = ps(INSERT_INSTANCE)) {
                bindInstance(p, i);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public boolean insertInstanceIfAbsent(Instance i) {
            try (PreparedStatement p = ps(dialect.insertIgnore(INSERT_INSTANCE))) {
                bindInstance(p, i);
                return p.executeUpdate() > 0;
            } catch (SQLException e) { throw wrap(e); }
        }

        private static void bindInstance(PreparedStatement p, Instance i) throws SQLException {
            p.setString(1, i.id); p.setString(2, i.workflow); p.setInt(3, i.version);
            p.setString(4, i.correlationId); p.setString(5, i.status.name());
            p.setString(6, i.terminationReason); p.setString(7, i.error); p.setString(8, i.context.json());
            p.setLong(9, i.createdAt); p.setLong(10, i.updatedAt); p.setLong(11, i.revision);
            p.setString(12, i.parentTokenId);
            setNullableLong(p, 13, i.settleAt);
        }

        @Override public Optional<Instance> lockInstance(String id) { return loadInstance(id, true); }

        @Override public Optional<Instance> findInstance(String id) { return loadInstance(id, false); }

        private Optional<Instance> loadInstance(String id, boolean forUpdate) {
            String sql = forUpdate
                    ? "SELECT * FROM wf_instance WHERE id=? FOR UPDATE"
                    : "SELECT * FROM wf_instance WHERE id=?";
            try (PreparedStatement p = ps(sql)) {
                p.setString(1, id);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next() ? Optional.of(readInstance(rs)) : Optional.empty();
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        private static final String UPDATE_INSTANCE = "UPDATE wf_instance SET status=?,term_reason=?," +
                "error=?,context=?,updated_at=?,settle_at=?,revision=revision+1 WHERE id=?";

        @Override public void updateInstance(Instance i) {
            try (PreparedStatement p = ps(UPDATE_INSTANCE)) {
                bindInstanceUpdate(p, i);
                p.executeUpdate();
                i.revision++;
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Instance> lockInstances(List<String> ids) {
            if (ids.isEmpty()) return List.of();
            // ORDER BY id keeps lock acquisition in id order on a btree PK scan; see Tx.lockInstances.
            // SKIP LOCKED where the dialect has it: a batch must never WAIT on an instance another
            // batch holds -- two workers whose batches share one instance (two arms of one fork)
            // would otherwise convoy, each batch serialising behind the other's whole commit. A
            // skipped instance is simply absent from the result; the caller answers its run as
            // retryable and the worker reports it singly.
            String sql = "SELECT * FROM wf_instance WHERE id IN (" + placeholders(ids.size())
                    + ") ORDER BY id FOR UPDATE" + (dialect.supportsSkipLocked() ? " SKIP LOCKED" : "");
            try (PreparedStatement p = ps(sql)) {
                for (int i = 0; i < ids.size(); i++) p.setString(i + 1, ids.get(i));
                try (ResultSet rs = p.executeQuery()) {
                    List<Instance> out = new ArrayList<>(ids.size());
                    while (rs.next()) out.add(readInstance(rs));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void updateInstances(List<Instance> instances) {
            if (instances.isEmpty()) return;
            try (PreparedStatement p = ps(UPDATE_INSTANCE)) {
                for (Instance i : instances) { bindInstanceUpdate(p, i); p.addBatch(); }
                requireOneRowEach(p.executeBatch(), "update wf_instance");
                for (Instance i : instances) i.revision++;
            } catch (SQLException e) { throw wrap(e); }
        }

        private static void bindInstanceUpdate(PreparedStatement p, Instance i) throws SQLException {
            p.setString(1, i.status.name()); p.setString(2, i.terminationReason); p.setString(3, i.error);
            p.setString(4, i.context.json()); p.setLong(5, i.updatedAt); setNullableLong(p, 6, i.settleAt);
            p.setString(7, i.id);
        }

        @Override public List<Instance> findByCorrelation(String correlationId, int limit) {
            String sql = "SELECT * FROM wf_instance WHERE correlation_id=? ORDER BY created_at DESC LIMIT ?";
            try (PreparedStatement p = ps(sql)) {
                p.setString(1, correlationId);
                p.setInt(2, limit);
                try (ResultSet rs = p.executeQuery()) {
                    List<Instance> out = new ArrayList<>();
                    while (rs.next()) out.add(readInstance(rs));
                    return out;
                }
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

        private static final String INSERT_TOKEN = "INSERT INTO wf_token (id,instance_id,workflow,version," +
                "node_id,kind,status,activity,queue,attempt,available_at,lease_owner,lease_expires,join_stack," +
                "last_error,created_at,updated_at,payload,comp_seq,started_at,finished_at,seq) " +
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        @Override public void insertToken(Token t) {
            try (PreparedStatement p = ps(INSERT_TOKEN)) {
                bindToken(p, t);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void insertTokens(List<Token> tokens) {
            if (tokens.isEmpty()) return;
            try (PreparedStatement p = ps(INSERT_TOKEN)) {
                for (Token t : tokens) { bindToken(p, t); p.addBatch(); }
                requireOneRowEach(p.executeBatch(), "insert wf_token");
            } catch (SQLException e) { throw wrap(e); }
        }

        /** Binds parameters 1..22 in wf_token insert column order. */
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
            p.setString(18, PayloadCodec.encode(t.payload));
            setNullableLong(p, 19, t.compSeq);
            setNullableLong(p, 20, t.startedAt);
            setNullableLong(p, 21, t.finishedAt);
            setNullableLong(p, 22, t.seq);
        }

        private static String placeholders(int n) {
            return "?,".repeat(n - 1) + "?";
        }

        private static void setNullableLong(PreparedStatement p, int idx, Long v) throws SQLException {
            if (v == null) p.setNull(idx, java.sql.Types.BIGINT);
            else p.setLong(idx, v);
        }

        @Override public Optional<Token> findToken(String id) {
            try (PreparedStatement p = ps("SELECT * FROM wf_token WHERE id=?")) {
                p.setString(1, id);
                try (ResultSet rs = p.executeQuery()) {
                    return rs.next() ? Optional.of(readToken(rs)) : Optional.empty();
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Token> findTokens(List<String> ids) {
            if (ids.isEmpty()) return List.of();
            String sql = "SELECT * FROM wf_token WHERE id IN (" + placeholders(ids.size()) + ")";
            try (PreparedStatement p = ps(sql)) {
                for (int i = 0; i < ids.size(); i++) p.setString(i + 1, ids.get(i));
                try (ResultSet rs = p.executeQuery()) {
                    List<Token> out = new ArrayList<>(ids.size());
                    while (rs.next()) out.add(readToken(rs));
                    return out;
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

        @Override
        public boolean hasActiveTokens(String instanceId) {
            try (PreparedStatement p = ps("SELECT 1 FROM wf_token WHERE instance_id=? AND status IN ('READY','RUNNING','WAITING','AWAITING','JOINED') LIMIT 1")) {
                p.setString(1, instanceId);
                try (ResultSet rs = p.executeQuery()) { return rs.next(); }
            } catch (SQLException e) { throw wrap(e); }
        }

        private static final String UPDATE_TOKEN = "UPDATE wf_token SET node_id=?,kind=?,status=?," +
                "activity=?,queue=?,attempt=?,available_at=?,lease_owner=?,lease_expires=?,join_stack=?," +
                "last_error=?,updated_at=?,payload=?,comp_seq=?,started_at=?,finished_at=?,seq=? WHERE id=?";

        @Override
        public void updateToken(Token t) {
            try (PreparedStatement p = ps(UPDATE_TOKEN)) {
                bindTokenUpdate(p, t);
                p.executeUpdate();
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public void updateTokens(List<Token> tokens) {
            if (tokens.isEmpty()) return;
            try (PreparedStatement p = ps(UPDATE_TOKEN)) {
                for (Token t : tokens) { bindTokenUpdate(p, t); p.addBatch(); }
                requireOneRowEach(p.executeBatch(), "update wf_token");
            } catch (SQLException e) { throw wrap(e); }
        }

        private static void bindTokenUpdate(PreparedStatement p, Token t) throws SQLException {
            p.setString(1, t.nodeId); p.setString(2, t.kind.name()); p.setString(3, t.status.name());
            p.setString(4, t.activity); p.setString(5, t.queue); p.setInt(6, t.attempt);
            p.setLong(7, t.availableAt); p.setString(8, t.leaseOwner); p.setLong(9, t.leaseExpiresAt);
            p.setString(10, t.joinStack == null ? "" : t.joinStack); p.setString(11, t.lastError);
            p.setLong(12, t.updatedAt); p.setString(13, PayloadCodec.encode(t.payload));
            setNullableLong(p, 14, t.compSeq); setNullableLong(p, 15, t.startedAt);
            setNullableLong(p, 16, t.finishedAt); setNullableLong(p, 17, t.seq); p.setString(18, t.id);
        }

        /** A count that is not one row means a buffered write ran out of order (an update flushed
         *  before its insert, say). Refusing turns silent corruption into a rollback the batch
         *  caller replays run by run. */
        private static void requireOneRowEach(int[] counts, String what) {
            for (int c : counts) {
                if (c != 1 && c != PreparedStatement.SUCCESS_NO_INFO) {
                    throw new IllegalStateException(what + ": batched statement touched " + c
                            + " rows where exactly 1 was expected");
                }
            }
        }

        @Override
        public List<String> joinStacksAt(String instanceId, String nodeId) {
            try (PreparedStatement p = ps("SELECT join_stack FROM wf_token WHERE instance_id=? AND node_id=? AND status='JOINED'")) {
                p.setString(1, instanceId);
                p.setString(2, nodeId);
                try (ResultSet rs = p.executeQuery()) {
                    List<String> out = new ArrayList<>();
                    while (rs.next()) out.add(rs.getString(1));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        @Override public List<Token> claimTasks(String workerId, Set<String> queues,
                                                Set<WorkflowVersion> versions, int max, long now,
                                                long leaseUntil) {
            // PostgreSQL claims in one statement; H2 has neither SKIP LOCKED nor RETURNING and
            // falls back to compare-and-set.
            return dialect.supportsSkipLocked() && dialect.supportsReturning()
                    ? claimSkipLockedReturning(workerId, queues, versions, max, now, leaseUntil)
                    : claimCompareAndSet(workerId, queues, versions, max, now, leaseUntil);
        }

        /**
         * The (workflow, version) filter of a version-scoped worker. It only narrows the candidate
         * set the dispatch index already found, so it costs a predicate and no join -- wf_token
         * carries both columns.
         */
        private static void appendVersions(StringBuilder sql, Set<WorkflowVersion> versions) {
            if (versions == null || versions.isEmpty()) return;
            sql.append(" AND (");
            for (int i = 0; i < versions.size(); i++) {
                if (i > 0) sql.append(" OR ");
                sql.append("(workflow=? AND version=?)");
            }
            sql.append(")");
        }

        /** Binds what {@link #appendVersions} appended; same set, so the same iteration order. */
        private static int bindVersions(PreparedStatement p, int idx, Set<WorkflowVersion> versions)
                throws SQLException {
            if (versions == null || versions.isEmpty()) return idx;
            for (WorkflowVersion v : versions) {
                p.setString(idx++, v.workflow());
                p.setInt(idx++, v.version());
            }
            return idx;
        }

        /**
         * Atomic claim for PostgreSQL: lock up to {@code max} dispatchable rows with
         * SKIP LOCKED -- which steps over rows another worker already holds instead of
         * blocking on them -- and update them in the same statement. Because no
         * transaction ever waits on a row task by another, concurrent claims across
         * many workers and nodes cannot deadlock, and none of them collide on a row.
         */
        private List<Token> claimSkipLockedReturning(String workerId, Set<String> queues,
                                                     Set<WorkflowVersion> versions, int max,
                                                     long now, long leaseUntil) {
            StringBuilder pick = new StringBuilder(
                    "SELECT id FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=?");
            if (queues != null && !queues.isEmpty()) {
                pick.append(" AND queue IN (").append("?,".repeat(queues.size() - 1)).append("?)");
            }
            appendVersions(pick, versions);
            pick.append(" ORDER BY available_at, id LIMIT ? FOR UPDATE SKIP LOCKED");
            String sql = "UPDATE wf_token SET status='RUNNING',lease_owner=?,lease_expires=?,updated_at=?," +
                    "started_at=?,finished_at=NULL WHERE id IN (" + pick + ") RETURNING *";
            try (PreparedStatement p = ps(sql)) {
                int idx = 1;
                p.setString(idx++, workerId);   // SET lease_owner
                p.setLong(idx++, leaseUntil);   // SET lease_expires
                p.setLong(idx++, now);          // SET updated_at
                p.setLong(idx++, now);          // SET started_at: the step's clock starts when a worker takes it
                p.setLong(idx++, now);          // WHERE available_at<=?
                if (queues != null && !queues.isEmpty()) for (String q : queues) p.setString(idx++, q);
                idx = bindVersions(p, idx, versions);
                p.setInt(idx, max);             // LIMIT
                try (ResultSet rs = p.executeQuery()) {
                    List<Token> out = new ArrayList<>();
                    while (rs.next()) out.add(readToken(rs));
                    return out;
                }
            } catch (SQLException e) { throw wrap(e); }
        }

        /** Portable fallback (H2): over-fetch candidates, then compare-and-set each. */
        private List<Token> claimCompareAndSet(String workerId, Set<String> queues,
                                               Set<WorkflowVersion> versions, int max,
                                               long now, long leaseUntil) {
            StringBuilder sql = new StringBuilder(
                    "SELECT * FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=?");
            if (queues != null && !queues.isEmpty()) {
                sql.append(" AND queue IN (").append("?,".repeat(queues.size() - 1)).append("?)");
            }
            appendVersions(sql, versions);
            sql.append(" ORDER BY available_at, id LIMIT ?");
            List<Token> candidates = new ArrayList<>();
            try (PreparedStatement p = ps(sql.toString())) {
                int idx = 1;
                p.setLong(idx++, now);
                if (queues != null && !queues.isEmpty()) for (String q : queues) p.setString(idx++, q);
                idx = bindVersions(p, idx, versions);
                p.setInt(idx, max * 4); // over-fetch: some candidates will lose the CAS race
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) candidates.add(readToken(rs));
                }
            } catch (SQLException e) { throw wrap(e); }

            List<Token> claimed = new ArrayList<>();
            try (PreparedStatement upd = ps("UPDATE wf_token SET status='RUNNING',lease_owner=?,lease_expires=?," +
                    "updated_at=?,started_at=?,finished_at=NULL WHERE id=? AND status='READY'")) {
                for (Token t : candidates) {
                    if (claimed.size() >= max) break;
                    upd.setString(1, workerId);
                    upd.setLong(2, leaseUntil);
                    upd.setLong(3, now);
                    upd.setLong(4, now);
                    upd.setString(5, t.id);
                    if (upd.executeUpdate() == 1) {
                        t.status = TokenStatus.RUNNING;
                        t.leaseOwner = workerId;
                        t.leaseExpiresAt = leaseUntil;
                        t.startedAt = now;
                        t.finishedAt = null;
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

        @Override public List<Instance> dueSettle(long now, int max) {
            List<Instance> out = new ArrayList<>();
            try (PreparedStatement p = ps("SELECT * FROM wf_instance WHERE status='RUNNING' AND settle_at IS NOT NULL "
                    + "AND settle_at <= ? ORDER BY settle_at LIMIT ?")) {
                p.setLong(1, now); p.setInt(2, max);
                try (ResultSet rs = p.executeQuery()) { while (rs.next()) out.add(readInstance(rs)); }
            } catch (SQLException e) { throw wrap(e); }
            return out;
        }

        @Override public List<Token> expiredLeases(long now, int max) {
            return query("SELECT * FROM wf_token WHERE status='RUNNING' AND lease_expires>0 AND lease_expires<? " +
                    "ORDER BY lease_expires LIMIT ?", now, max);
        }

        @Override public List<Token> pendingSignals(int max) {
            try (PreparedStatement p = ps("SELECT * FROM wf_token WHERE status='AWAITING' AND kind='SIGNAL' " +
                    "ORDER BY created_at LIMIT ?")) {
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
                p.setString(4, s.cron); p.setString(5, s.context.json());
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
            try (PreparedStatement p = ps("SELECT * FROM wf_schedule WHERE next_fire_at<=? " +
                    "ORDER BY next_fire_at LIMIT ?")) {
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
            s.context = Doc.parse(rs.getString("context"));
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

        @Override public List<Rows.BacklogSlice> backlogByVersion(long now, int max) {
            try (PreparedStatement p = ps("SELECT workflow, version, queue, COUNT(*), COALESCE(MIN(available_at),0) FROM wf_token " +
                    "WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=? " +
                    "GROUP BY workflow, version, queue ORDER BY COUNT(*) DESC LIMIT ?")) {
                p.setLong(1, now);
                p.setInt(2, max);
                try (ResultSet rs = p.executeQuery()) {
                    List<Rows.BacklogSlice> out = new ArrayList<>();
                    while (rs.next()) {
                        out.add(new Rows.BacklogSlice(rs.getString(1), rs.getInt(2), rs.getString(3),
                                rs.getInt(4), rs.getLong(5)));
                    }
                    return out;
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
            // ORDER BY is required for SQL Server's OFFSET/FETCH rewrite of LIMIT, and gives every
            // dialect a deterministic "oldest first" deletion order at no cost.
            try (PreparedStatement p = ps("SELECT id FROM wf_instance WHERE status NOT IN ('RUNNING','COMPENSATING') AND updated_at<? ORDER BY updated_at LIMIT ?")) {
                p.setLong(1, updatedBefore);
                p.setInt(2, limit);
                try (ResultSet rs = p.executeQuery()) { while (rs.next()) ids.add(rs.getString(1)); }
            } catch (SQLException e) { throw wrap(e); }
            if (ids.isEmpty()) return 0;
            try (PreparedStatement dt = ps("DELETE FROM wf_token WHERE instance_id=?");
                 PreparedStatement dc = ps("DELETE FROM wf_comp_log WHERE instance_id=?");
                 PreparedStatement di = ps("DELETE FROM wf_instance WHERE id=?")) {
                for (String id : ids) {
                    dt.setString(1, id); dt.executeUpdate();
                    dc.setString(1, id); dc.executeUpdate();
                    di.setString(1, id); di.executeUpdate();
                }
            } catch (SQLException e) { throw wrap(e); }
            return ids.size();
        }

        @Override public void appendCompensation(Rows.CompLog e) {
            try (PreparedStatement p = ps("INSERT INTO wf_comp_log "
                    + "(instance_id,seq,node_id,activity,queue,input_json,result_json,compensated) "
                    + "VALUES (?,?,?,?,?,?,?,?)")) {
                p.setString(1, e.instanceId); p.setLong(2, e.seq); p.setString(3, e.nodeId);
                p.setString(4, e.activity); p.setString(5, e.queue);
                p.setString(6, e.input == null ? null : e.input.json()); p.setString(7, e.result == null ? null : e.result.json());
                p.setInt(8, e.compensated ? 1 : 0);
                p.executeUpdate();
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public java.util.List<Rows.CompLog> compensationLog(String instanceId) {
            java.util.List<Rows.CompLog> out = new java.util.ArrayList<>();
            try (PreparedStatement p = ps("SELECT seq,node_id,activity,queue,input_json,result_json,compensated "
                    + "FROM wf_comp_log WHERE instance_id=? ORDER BY seq")) {
                p.setString(1, instanceId);
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) {
                        Rows.CompLog e = new Rows.CompLog();
                        e.instanceId = instanceId;
                        e.seq = rs.getLong(1); e.nodeId = rs.getString(2); e.activity = rs.getString(3);
                        e.queue = rs.getString(4); e.input = Doc.parse(rs.getString(5));
                        e.result = Doc.parse(rs.getString(6)); e.compensated = rs.getInt(7) != 0;
                        out.add(e);
                    }
                }
            } catch (SQLException ex) { throw wrap(ex); }
            return out;
        }

        @Override public void insertAnomaly(Rows.Anomaly a) {
            try (PreparedStatement p = ps("INSERT INTO wf_anomaly (id,instance_id,workflow,version,kind,"
                    + "expected_node,reported_node,detail,observed_at) VALUES (?,?,?,?,?,?,?,?,?)")) {
                p.setString(1, a.id()); p.setString(2, a.instanceId()); p.setString(3, a.workflow());
                p.setInt(4, a.version()); p.setString(5, a.kind()); p.setString(6, a.expectedNode());
                p.setString(7, a.reportedNode()); p.setString(8, a.detail()); p.setLong(9, a.at());
                p.executeUpdate();
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public List<Rows.Anomaly> anomalies(String workflow, String instanceId, int limit) {
            StringBuilder sql = new StringBuilder("SELECT id,instance_id,workflow,version,kind,expected_node,"
                    + "reported_node,detail,observed_at FROM wf_anomaly WHERE 1=1");
            if (workflow != null) sql.append(" AND workflow=?");
            if (instanceId != null) sql.append(" AND instance_id=?");
            sql.append(" ORDER BY observed_at DESC, id DESC LIMIT ?");
            List<Rows.Anomaly> out = new ArrayList<>();
            try (PreparedStatement p = ps(sql.toString())) {
                int i = 1;
                if (workflow != null) p.setString(i++, workflow);
                if (instanceId != null) p.setString(i++, instanceId);
                p.setInt(i, limit);
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Rows.Anomaly(rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4),
                                rs.getString(5), rs.getString(6), rs.getString(7), rs.getString(8), rs.getLong(9)));
                    }
                }
            } catch (SQLException ex) { throw wrap(ex); }
            return out;
        }

        @Override public long appendEvent(Rows.Event e) {
            try (PreparedStatement p = c.prepareStatement("INSERT INTO wf_event (instance_id,workflow,version,"
                    + "correlation_id,type,payload_ver,payload,created_at) VALUES (?,?,?,?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS)) {
                p.setString(1, e.instanceId()); p.setString(2, e.workflow()); p.setInt(3, e.version());
                p.setString(4, e.correlationId()); p.setString(5, e.type()); p.setInt(6, e.payloadVer());
                p.setString(7, e.payload()); p.setLong(8, e.createdAt());
                p.executeUpdate();
                try (ResultSet keys = p.getGeneratedKeys()) {
                    if (!keys.next()) throw new StorageException("wf_event insert returned no seq", null);
                    return keys.getLong(1);
                }
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public List<Rows.Event> eventsAfter(long afterSeq, long createdBefore, int max) {
            List<Rows.Event> out = new ArrayList<>();
            try (PreparedStatement p = ps("SELECT seq,instance_id,workflow,version,correlation_id,type,payload_ver,"
                    + "payload,created_at FROM wf_event WHERE seq>? AND created_at<? ORDER BY seq LIMIT ?")) {
                p.setLong(1, afterSeq); p.setLong(2, createdBefore); p.setInt(3, max);
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) {
                        out.add(new Rows.Event(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getInt(4),
                                rs.getString(5), rs.getString(6), rs.getInt(7), rs.getString(8), rs.getLong(9)));
                    }
                }
            } catch (SQLException ex) { throw wrap(ex); }
            return out;
        }

        @Override public long latestEventSeq() {
            try (PreparedStatement p = ps("SELECT COALESCE(MAX(seq),0) FROM wf_event");
                 ResultSet rs = p.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public Rows.EventCursor eventCursor(String consumer) {
            try (PreparedStatement p = ps("SELECT consumer,acked_seq,last_seen,created_at FROM wf_event_cursor WHERE consumer=?")) {
                p.setString(1, consumer);
                try (ResultSet rs = p.executeQuery()) {
                    if (!rs.next()) return null;
                    return new Rows.EventCursor(rs.getString(1), rs.getLong(2), rs.getLong(3), rs.getLong(4));
                }
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public void createEventCursorIfAbsent(Rows.EventCursor c) {
            try (PreparedStatement p = ps(dialect.insertIgnore(
                    "INSERT INTO wf_event_cursor (consumer,acked_seq,last_seen,created_at) VALUES (?,?,?,?)"))) {
                p.setString(1, c.consumer()); p.setLong(2, c.ackedSeq());
                p.setLong(3, c.lastSeen()); p.setLong(4, c.createdAt());
                p.executeUpdate();
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public void advanceEventCursor(String consumer, long ackedSeq, long now) {
            // Update-then-insert rather than an upsert: only PostgreSQL takes ON CONFLICT DO UPDATE,
            // and the two statements are portable. The trailing update covers the race where another
            // ack inserted the row between ours: it folds our seq into the row that won.
            if (moveCursor(consumer, ackedSeq, now) > 0) return;
            createEventCursorIfAbsent(new Rows.EventCursor(consumer, ackedSeq, now, now));
            moveCursor(consumer, ackedSeq, now);
        }

        /** Moves an existing cursor forward (never back) and stamps it; 0 when the consumer has none. */
        private int moveCursor(String consumer, long ackedSeq, long now) {
            try (PreparedStatement p = ps("UPDATE wf_event_cursor SET acked_seq=CASE WHEN acked_seq<? THEN ? "
                    + "ELSE acked_seq END, last_seen=? WHERE consumer=?")) {
                p.setLong(1, ackedSeq); p.setLong(2, ackedSeq); p.setLong(3, now); p.setString(4, consumer);
                return p.executeUpdate();
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public Long oldestAckedSeq() {
            try (PreparedStatement p = ps("SELECT MIN(acked_seq) FROM wf_event_cursor");
                 ResultSet rs = p.executeQuery()) {
                if (!rs.next()) return null;
                long v = rs.getLong(1);
                return rs.wasNull() ? null : v;
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public int deleteEvents(long createdBefore, Long upToSeq, int max) {
            List<Long> seqs = new ArrayList<>();
            String sql = "SELECT seq FROM wf_event WHERE created_at<?" + (upToSeq != null ? " AND seq<=?" : "")
                    + " ORDER BY seq LIMIT ?";
            try (PreparedStatement p = ps(sql)) {
                int i = 1;
                p.setLong(i++, createdBefore);
                if (upToSeq != null) p.setLong(i++, upToSeq);
                p.setInt(i, max);
                try (ResultSet rs = p.executeQuery()) { while (rs.next()) seqs.add(rs.getLong(1)); }
            } catch (SQLException ex) { throw wrap(ex); }
            if (seqs.isEmpty()) return 0;
            try (PreparedStatement d = ps("DELETE FROM wf_event WHERE seq<=? AND created_at<?")) {
                d.setLong(1, seqs.getLast()); d.setLong(2, createdBefore);
                return d.executeUpdate();
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override public List<Rows.StepDuration> stepDurations(String workflow, int version, long since, int max) {
            List<Rows.StepDuration> out = new ArrayList<>();
            try (PreparedStatement p = ps("SELECT node_id, started_at, finished_at, available_at, seq FROM wf_token "
                    + "WHERE workflow=? AND version=? AND status='DONE' AND finished_at > ? AND started_at IS NOT NULL "
                    + "ORDER BY finished_at DESC LIMIT ?")) {
                p.setString(1, workflow); p.setInt(2, version); p.setLong(3, since); p.setInt(4, max);
                try (ResultSet rs = p.executeQuery()) {
                    while (rs.next()) {
                        long seq = rs.getLong(5);
                        boolean observed = !rs.wasNull();
                        out.add(new Rows.StepDuration(rs.getString(1), Math.max(0, rs.getLong(3) - rs.getLong(2)),
                                observed ? 0 : Math.max(0, rs.getLong(2) - rs.getLong(4))));
                    }
                }
            } catch (SQLException ex) { throw wrap(ex); }
            return out;
        }

        @Override public void markCompensated(String instanceId, long seq) {
            try (PreparedStatement p = ps("UPDATE wf_comp_log SET compensated=1 WHERE instance_id=? AND seq=?")) {
                p.setString(1, instanceId); p.setLong(2, seq);
                p.executeUpdate();
            } catch (SQLException ex) { throw wrap(ex); }
        }

        @Override
        public void cancelActiveTokens(String instanceId, long now) {
            try (PreparedStatement p = ps("""
                    UPDATE wf_token
                       SET status='CANCELLED', lease_owner=NULL, lease_expires=0, updated_at=?
                     WHERE instance_id=? AND status IN ('READY','RUNNING','WAITING','AWAITING','JOINED')
                    """)) {
                p.setLong(1, now);
                p.setString(2, instanceId);
                p.executeUpdate();
            } catch (SQLException ex) { throw wrap(ex); }
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
            i.context = Doc.parse(rs.getString("context"));
            i.parentTokenId = rs.getString("parent_token_id");
            i.createdAt = rs.getLong("created_at");
            i.updatedAt = rs.getLong("updated_at");
            i.revision = rs.getLong("revision");
            long settleAt = rs.getLong("settle_at");
            i.settleAt = rs.wasNull() ? null : settleAt;
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
            t.payload = PayloadCodec.decode(rs.getString("payload"));
            long compSeq = rs.getLong("comp_seq");
            t.compSeq = rs.wasNull() ? null : compSeq;
            long startedAt = rs.getLong("started_at");
            t.startedAt = rs.wasNull() ? null : startedAt;
            long finishedAt = rs.getLong("finished_at");
            t.finishedAt = rs.wasNull() ? null : finishedAt;
            long seq = rs.getLong("seq");
            t.seq = rs.wasNull() ? null : seq;
            t.createdAt = rs.getLong("created_at");
            t.updatedAt = rs.getLong("updated_at");
            return t;
        }
    }
}
