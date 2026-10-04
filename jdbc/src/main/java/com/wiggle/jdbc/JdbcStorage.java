package com.wiggle.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.statement.Query;
import org.jdbi.v3.core.statement.SqlStatement;
import org.jdbi.v3.core.statement.Update;
import com.wiggle.core.*;
import com.wiggle.core.Doc;
import com.wiggle.server.store.PayloadCodec;
import com.wiggle.core.InstanceStatus;
import com.wiggle.core.TokenStatus;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.*;
import com.wiggle.server.store.ShardState;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.store.StorageException.Classification;
import com.wiggle.server.store.StorageUnreachableException;
import com.wiggle.server.store.Tx;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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

    private static final System.Logger LOG = System.getLogger(JdbcStorage.class.getName());

    private final Dialect dialect;
    private final HikariDataSource ds;
    private final Jdbi jdbi;
    private final String fingerprint;
    private final boolean readOnly;

    /** Attempts a transaction gets when it rolled back on a momentary failure; 1 disables the replay. */
    private final int txAttempts = (int) envLong("wiggle.jdbc.txAttempts", "WIGGLE_JDBC_TX_ATTEMPTS", 3);

    /** Base pause before a replay; attempt n waits {@code n x} this, so contention spreads out. */
    private final long txRetryDelayMillis =
            envLong("wiggle.jdbc.txRetryDelayMillis", "WIGGLE_JDBC_TX_RETRY_DELAY_MILLIS", 50);

    /** Explicit-dialect constructor used by the per-database modules. */
    public JdbcStorage(String url, String user, String password, int poolSize, Dialect dialect) {
        this(url, user, password, poolSize, dialect, false);
    }

    /**
     * A store over a read replica: its connections are read-only, it is never migrated, and it opens
     * even while the replica is unreachable, so a replica that is down at startup costs reads from it
     * rather than the node.
     */
    public static JdbcStorage readReplica(String url, String user, String password, int poolSize, Dialect dialect) {
        return new JdbcStorage(url, user, password, poolSize, dialect, true);
    }

    private JdbcStorage(String url, String user, String password, int poolSize, Dialect dialect, boolean readOnly) {
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.readOnly = readOnly;
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
        cfg.setPoolName("wiggle-" + dialect.id() + (readOnly ? "-replica" : ""));
        if (readOnly) {
            cfg.setReadOnly(true);
            cfg.setInitializationFailTimeout(-1);
        }
        this.ds = new HikariDataSource(cfg);
        this.jdbi = Jdbi.create(this.ds)
                .registerRowMapper(Instance.class, (rs, ctx) -> readInstance(rs))
                .registerRowMapper(Token.class, (rs, ctx) -> readToken(rs))
                .registerRowMapper(Rows.Event.class, (rs, ctx) -> readEvent(rs))
                .registerRowMapper(Rows.EventCursor.class, (rs, ctx) -> readCursor(rs))
                .registerRowMapper(Rows.Schedule.class, (rs, ctx) -> readSchedule(rs))
                .registerRowMapper(ServerNode.class, (rs, ctx) -> readNode(rs));
    }

    private Connection borrow() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            throw connectionFailure(e);
        }
    }

    /**
     * A failure to hand out a connection. A pool timeout whose cause is a failed connect means the
     * database is unreachable, not merely busy: {@link StorageUnreachableException}.
     */
    private StorageException connectionFailure(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLTransientConnectionException timeout && timeout.getCause() != null) {
                return new StorageUnreachableException("cannot reach the database: " + timeout.getCause().getMessage(), e);
            }
        }
        return new StorageException("cannot obtain connection", e, classify(e));
    }

    /** {@link Classification#TRANSIENT} if the dialect recognises a momentary failure anywhere in the
     *  cause chain, {@link Classification#PERMANENT} otherwise. Never answers {@code AMBIGUOUS}: only
     *  the commit boundary knows that, and {@link #attemptTx} labels it there. */
    private Classification classify(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLException sql && dialect.isTransient(sql)) return Classification.TRANSIENT;
        }
        return Classification.PERMANENT;
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

    /** A system property first, then the environment: the property is how a test shortens a bound. */
    private static long envLong(String prop, String env, long def) {
        String v = System.getProperty(prop);
        if (v == null || v.isBlank()) v = System.getenv(env);
        if (v == null || v.isBlank()) return def;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static void release(Connection c) {
        if (c == null) return;
        try { c.close(); } catch (SQLException ignored) { }  // returns the connection to the pool
    }

    /**
     * One forward-only schema step. {@code sql} may hold several {@code ;}-separated statements.
     * On a dialect {@code appliesTo} rejects, the step is recorded as applied without running.
     */
    public record Migration(int version, String name, String sql, Predicate<Dialect> appliesTo) {
        public Migration(int version, String name, String sql) {
            this(version, name, sql, dialect -> true);
        }
    }

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
              status         VARCHAR(32)  NOT NULL,
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
            """),
            // Which step emitted an event. Null for the engine's own lifecycle entries, and for
            // emitted ones it is the only way a consumer can tell: a retry or a batch makes the
            // step ambiguous from the outside.
            new Migration(17, "event-node", """
            ALTER TABLE wf_event ADD COLUMN IF NOT EXISTS node_id VARCHAR(64);
            """),
            // wf_instance.status was VARCHAR(16), and COMPENSATION_FAILED is nineteen characters:
            // the one status a saga reaches when a compensator runs out of retries could not be
            // written at all, so the update rolled back and the instance stayed COMPENSATING with
            // its undo retrying forever. Only JDBC deployments hit it -- the in-memory store holds
            // the enum, which is why every test passed. 32 leaves room for a longer status later.
            new Migration(18, "instance-status-width", """
            ALTER TABLE wf_instance ALTER COLUMN status TYPE VARCHAR(32);
            """),
            // Every wf_token query that filters on status names exactly one status, so each gets an
            // index over only the rows in that status: ready (claim, queue depth, backlog), waiting
            // (timers), awaiting (signal deadlines), running (expired leases), and done (throughput,
            // step durations). Settled rows are most of the table and no longer sit in the dispatch,
            // timer, signal or lease indexes, and a status move writes an entry only to the indexes
            // whose predicate the new row satisfies. The four full indexes these replace are dropped.
            // Databases without partial indexes (H2) keep the full ones.
            //
            // Building these on a large wf_token blocks writes to it until they finish, so run this
            // in a maintenance window (or ahead of the deploy with WIGGLE_MIGRATE_ONLY=true) when
            // the table is big.
            new Migration(19, "partial-token-indexes", """
            CREATE INDEX IF NOT EXISTS ix_token_ready ON wf_token (queue, available_at, id) WHERE status='READY';
            CREATE INDEX IF NOT EXISTS ix_token_waiting ON wf_token (available_at) WHERE status='WAITING';
            CREATE INDEX IF NOT EXISTS ix_token_awaiting ON wf_token (available_at) WHERE status='AWAITING';
            CREATE INDEX IF NOT EXISTS ix_token_running ON wf_token (lease_expires) WHERE status='RUNNING';
            CREATE INDEX IF NOT EXISTS ix_token_done ON wf_token (updated_at) WHERE status='DONE';
            CREATE INDEX IF NOT EXISTS ix_token_done_timed ON wf_token (workflow, version, finished_at) WHERE status='DONE';
            DROP INDEX IF EXISTS ix_token_dispatch;
            DROP INDEX IF EXISTS ix_token_lease;
            DROP INDEX IF EXISTS ix_token_throughput;
            DROP INDEX IF EXISTS ix_token_timed;
            """, Dialect::supportsPartialIndexes),
            // Dispatch serves the oldest instance's ready work first, so a backlog finishes what it
            // started instead of advancing every instance one step at a time. Rows written before
            // this carry 0 and are served first.
            new Migration(20, "token-instance-age", """
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS inst_created_at BIGINT NOT NULL DEFAULT 0;
            """),
            new Migration(21, "ready-index-by-instance-age", """
            CREATE INDEX IF NOT EXISTS ix_token_ready_age ON wf_token (queue, inst_created_at, available_at, id) WHERE status='READY';
            DROP INDEX IF EXISTS ix_token_ready;
            """, Dialect::supportsPartialIndexes),
            // A step's input and output as JSON, for the console to show. Null on rows written before
            // this, and wherever recording is off.
            new Migration(22, "token-step-io", """
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS step_input TEXT;
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS step_output TEXT;
            """),
            // Sharding: which shard a database was claimed for, the registry of every shard the
            // cluster has used (read on the home shard), and the topology generation each node runs.
            new Migration(23, "shard-identity-and-registry", """
            CREATE TABLE IF NOT EXISTS wf_shard (
              k            VARCHAR(16)  PRIMARY KEY,
              shard_id     INT          NOT NULL
            );
            CREATE TABLE IF NOT EXISTS wf_shard_registry (
              shard_id     INT          PRIMARY KEY,
              state        VARCHAR(16)  NOT NULL,
              first_seen   BIGINT       NOT NULL,
              retired_at   BIGINT
            );
            ALTER TABLE wf_node ADD COLUMN IF NOT EXISTS topology_generation BIGINT;
            """),
            // The replica-lag heartbeat: the leader stamps it on every primary, and each node reads it
            // back from the replicas.
            new Migration(24, "shard-beat", """
            ALTER TABLE wf_shard ADD COLUMN IF NOT EXISTS beat_at BIGINT;
            """),
            // A consumer's event position on each shard other than home (its home position stays in
            // wf_event_cursor.acked_seq). Held on the home shard.
            new Migration(25, "event-cursor-per-shard", """
            CREATE TABLE IF NOT EXISTS wf_event_cursor_shard (
              consumer       VARCHAR(200) NOT NULL,
              shard_id       INT          NOT NULL,
              acked_seq      BIGINT       NOT NULL,
              PRIMARY KEY (consumer, shard_id)
            );
            """),
            // OBSERVED execution was removed. A run still open would stay RUNNING forever with
            // nothing left to settle it, so it is cancelled; then its settle time, the anomaly table
            // and the order its steps were reported in go.
            new Migration(26, "drop-observed-execution", """
            UPDATE wf_instance SET status='CANCELLED',
              term_reason='OBSERVED execution was removed', revision=revision+1
              WHERE settle_at IS NOT NULL AND status='RUNNING';
            DROP INDEX IF EXISTS ix_instance_settle;
            ALTER TABLE wf_instance DROP COLUMN IF EXISTS settle_at;
            DROP INDEX IF EXISTS ix_anomaly_instance;
            DROP INDEX IF EXISTS ix_anomaly_workflow;
            DROP TABLE IF EXISTS wf_anomaly;
            ALTER TABLE wf_token DROP COLUMN IF EXISTS seq;
            """),
            // Portal accounts, roles, sessions, machine credentials and the audit of every change
            // to them. Read and written on the auth shard only.
            new Migration(27, "auth", """
            CREATE TABLE IF NOT EXISTS wf_auth_user (
              name         VARCHAR(64)  PRIMARY KEY,
              hash         VARCHAR(128) NOT NULL,
              salt         VARCHAR(64)  NOT NULL,
              iterations   INT          NOT NULL,
              disabled     INT          NOT NULL DEFAULT 0,
              created_at   BIGINT       NOT NULL,
              updated_at   BIGINT       NOT NULL
            );
            CREATE TABLE IF NOT EXISTS wf_auth_role (
              name         VARCHAR(64)  PRIMARY KEY,
              permissions  TEXT         NOT NULL,
              builtin      INT          NOT NULL DEFAULT 0,
              created_at   BIGINT       NOT NULL,
              updated_at   BIGINT       NOT NULL
            );
            CREATE TABLE IF NOT EXISTS wf_auth_user_role (
              user_name    VARCHAR(64)  NOT NULL,
              role_name    VARCHAR(64)  NOT NULL,
              PRIMARY KEY (user_name, role_name)
            );
            CREATE TABLE IF NOT EXISTS wf_auth_session (
              id_hash      VARCHAR(64)  PRIMARY KEY,
              user_name    VARCHAR(64)  NOT NULL,
              expires_at   BIGINT       NOT NULL,
              created_at   BIGINT       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_auth_session_user ON wf_auth_session (user_name);
            CREATE INDEX IF NOT EXISTS ix_auth_session_expiry ON wf_auth_session (expires_at);
            CREATE TABLE IF NOT EXISTS wf_auth_credential (
              id           VARCHAR(64)  PRIMARY KEY,
              kind         VARCHAR(16)  NOT NULL,
              key_hash     VARCHAR(128),
              subject      VARCHAR(512),
              role_name    VARCHAR(64)  NOT NULL,
              created_at   BIGINT       NOT NULL,
              expires_at   BIGINT
            );
            CREATE TABLE IF NOT EXISTS wf_auth_audit (
              seq          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
              at           BIGINT       NOT NULL,
              actor        VARCHAR(64),
              action       VARCHAR(64)  NOT NULL,
              target       VARCHAR(200),
              detail       TEXT
            );
            """),
            // Machine credentials are looked up by key hash or certificate subject, each unique.
            new Migration(28, "auth-credential-lookup", """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_auth_credential_key ON wf_auth_credential (key_hash);
            CREATE UNIQUE INDEX IF NOT EXISTS ux_auth_credential_subject ON wf_auth_credential (subject);
            """),
            // Search documents, on the search shards: one per instance, derived from its instance shard.
            new Migration(29, "search-doc", """
            CREATE TABLE IF NOT EXISTS wf_search_doc (
              instance_id    VARCHAR(128) PRIMARY KEY,
              workflow       VARCHAR(200) NOT NULL,
              version        INT          NOT NULL,
              status         VARCHAR(32)  NOT NULL,
              correlation_id VARCHAR(200),
              text           TEXT         NOT NULL,
              created_at     BIGINT       NOT NULL,
              updated_at     BIGINT       NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_search_doc_updated ON wf_search_doc (updated_at);
            CREATE INDEX IF NOT EXISTS ix_search_doc_workflow ON wf_search_doc (workflow, updated_at);
            """),
            // Full-text matching where the database has it: a word vector kept by the database itself.
            new Migration(30, "search-doc-fulltext", """
            ALTER TABLE wf_search_doc ADD COLUMN IF NOT EXISTS tsv tsvector
              GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED;
            CREATE INDEX IF NOT EXISTS ix_search_doc_tsv ON wf_search_doc USING GIN (tsv);
            """, Dialect::supportsFullText),
            // Embeddings, one per instance and model, beside their document on a search shard; and on
            // the home shard, the registry of the models being built, served or retired. Where
            // pgvector is installed, migrate() also adds a native vector column (see enableVectors).
            new Migration(31, "search-vectors", """
            CREATE TABLE IF NOT EXISTS wf_search_vec (
              instance_id    VARCHAR(128) NOT NULL,
              model          VARCHAR(200) NOT NULL,
              embedding      BYTEA,
              updated_at     BIGINT       NOT NULL,
              PRIMARY KEY (instance_id, model)
            );
            CREATE INDEX IF NOT EXISTS ix_search_vec_model ON wf_search_vec (model);
            CREATE TABLE IF NOT EXISTS wf_search_model (
              model          VARCHAR(200) PRIMARY KEY,
              dimension      INT          NOT NULL,
              state          VARCHAR(16)  NOT NULL,
              started_at     BIGINT       NOT NULL,
              ready_at       BIGINT
            );
            """),
            // Every token write lands in every index whose predicate the row satisfies, so each one
            // covers only the rows its queries read: the join reads JOINED tokens, throughput counts
            // finished tasks and predicates, step durations read tokens that were claimed. The
            // instance index serves every other read by instance. Same caveat as migration 19.
            new Migration(32, "narrow-token-indexes", """
            CREATE INDEX IF NOT EXISTS ix_token_joined ON wf_token (instance_id, node_id) WHERE status='JOINED';
            CREATE INDEX IF NOT EXISTS ix_token_done_steps ON wf_token (updated_at) WHERE status='DONE' AND kind IN ('TASK','PREDICATE');
            CREATE INDEX IF NOT EXISTS ix_token_step_timed ON wf_token (workflow, version, finished_at) WHERE status='DONE' AND started_at IS NOT NULL;
            DROP INDEX IF EXISTS ix_token_barrier;
            DROP INDEX IF EXISTS ix_token_done;
            DROP INDEX IF EXISTS ix_token_done_timed;
            """, Dialect::supportsPartialIndexes));

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
        if (readOnly) throw new IllegalStateException("a read replica is not migrated; its primary is");
        MigrationMode mode = modeFromEnv();
        applyMigrations(MIGRATIONS, "baseline", mode);
        if (mode == MigrationMode.APPLY && dialect.supportsFullText()) enableVectors();
    }

    /**
     * Adds a native {@code vector} column to {@code wf_search_vec} where the pgvector extension is
     * installed on the server, so nearest-neighbour search runs in the database over an HNSW index.
     * Where it is not, or this user may not create it, vectors stay in {@code embedding} and are
     * compared in Java: correct, but a scan of every candidate. Not a numbered migration because
     * whether it applies depends on the server, not on this schema.
     */
    private void enableVectors() {
        try (Handle h = open()) {
            Connection c = h.getConnection();
            try {
                boolean available = h.createQuery("SELECT COUNT(*) FROM pg_available_extensions WHERE name='vector'")
                        .mapTo(Long.class).one() > 0;
                if (!available) {
                    c.rollback();
                    LOG.log(System.Logger.Level.INFO, "pgvector is not installed on this PostgreSQL; vector search "
                            + "compares embeddings in Java (install pgvector for an HNSW index)");
                    return;
                }
                h.execute("CREATE EXTENSION IF NOT EXISTS vector");
                h.execute("ALTER TABLE wf_search_vec ADD COLUMN IF NOT EXISTS vec vector");
                c.commit();
                pgvector = true;
            } catch (SQLException | RuntimeException e) {
                rollback(c);
                LOG.log(System.Logger.Level.WARNING, () -> "could not enable pgvector (" + e.getMessage()
                        + "); vector search compares embeddings in Java. A superuser can run CREATE EXTENSION vector");
            }
        }
    }

    /** Whether {@code wf_search_vec} has the native vector column; asked of the database once. */
    private volatile Boolean pgvector;

    boolean pgvector(Handle h) {
        Boolean known = pgvector;
        if (known != null) return known;
        boolean has = dialect.supportsFullText() && h.createQuery("SELECT COUNT(*) FROM information_schema.columns "
                        + "WHERE table_name='wf_search_vec' AND column_name='vec'")
                .mapTo(Long.class).one() > 0;
        pgvector = has;
        return has;
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
            throw new StorageException("migration failed", e, classify(e));
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
     * baseline belongs to another schema fails fast instead of silently skipping every migration
     * because the version counter is already ahead.
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
                                + "'. Each schema needs its own database.");
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
            if (m.appliesTo().test(dialect)) {
                try (Statement st = c.createStatement()) {
                    for (String stmt : m.sql().split(";")) {
                        if (!stmt.isBlank()) execDdl(st, stmt);
                    }
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

    /**
     * Runs {@code work} in one transaction, replaying it on a momentary database failure.
     *
     * <p>A replay is only ever offered where the attempt provably applied nothing: the failure came
     * out of a statement, the transaction rolled back, and the dialect calls that failure transient
     * (connection loss, a pool timeout, a deadlock victim, a serialization failure). Those replays
     * are what make a database blip invisible to a caller instead of a failed workflow step. A failed
     * <em>commit</em> is never replayed -- the work may be durable -- and nothing else is either. An
     * unreachable database is not replayed either: the attempt already waited out the pool timeout.
     *
     * <p>{@code work} must therefore be re-runnable against a fresh {@link Tx}: it may read, write
     * and throw, but it must not depend on in-process state it mutated on the previous attempt. Every
     * engine body satisfies this by construction, since each one re-reads the rows it works from.
     * {@code wiggle.jdbc.txAttempts} / {@code WIGGLE_JDBC_TX_ATTEMPTS} bounds the replays (read once
     * per store, at construction); 1 turns them off.
     */
    @Override public <R> R inTx(Function<Tx, R> work) {
        for (int attempt = 1; ; attempt++) {
            try {
                return attemptTx(work);
            } catch (StorageException e) {
                if (!e.repeatable() || e instanceof StorageUnreachableException || attempt >= txAttempts) throw e;
                int a = attempt;
                LOG.log(System.Logger.Level.DEBUG, () -> "transaction rolled back on a transient failure ("
                        + e.getMessage() + "); replaying, attempt " + (a + 1) + " of " + txAttempts);
                pauseBeforeReplay(attempt, e);
            }
        }
    }

    /**
     * One attempt. JDBI borrows the connection and hands it back to the pool when the handle closes;
     * the transaction stays this store's own, as it always was. Nothing calls {@code handle.begin()},
     * so there is one owner of the commit boundary and not two.
     */
    private <R> R attemptTx(Function<Tx, R> work) {
        try (Handle h = open()) {
            Connection c = h.getConnection();
            try {
                R r = work.apply(new JdbcTx(h, dialect, this));
                c.commit();
                return r;
            } catch (SQLException e) {
                // Only the commit above throws a checked SQLException here, and its outcome is
                // unknown: the rollback may be a no-op over work that is already durable.
                rollback(c);
                throw new StorageException("commit failed", e, Classification.AMBIGUOUS);
            } catch (RuntimeException e) {
                rollback(c);
                throw labelled(e);
            }
        }
    }

    /** A handle, with a pool timeout or a dead connection reported as the transient failure it is. */
    private Handle open() {
        try {
            return jdbi.open();
        } catch (RuntimeException e) {
            throw connectionFailure(e);
        }
    }

    /** A transient statement failure, re-raised as a {@link StorageException} the replay above can
     *  recognise. Anything else -- including a {@code StorageException} already classified by the
     *  statement that raised it -- passes through untouched. */
    private RuntimeException labelled(RuntimeException e) {
        if (e instanceof StorageException) return e;
        return classify(e) == Classification.TRANSIENT
                ? new StorageException(e.getMessage(), e, Classification.TRANSIENT) : e;
    }

    /** Waits out a transient failure before replaying. An interrupt abandons the replay and surfaces
     *  the original failure, with the thread's interrupt flag restored. */
    private void pauseBeforeReplay(int attempt, StorageException failure) {
        long delay = txRetryDelayMillis * attempt;
        if (delay <= 0) return;
        try {
            Thread.sleep(delay);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw failure;
        }
    }

    private static void rollback(Connection c) {
        try { c.rollback(); } catch (SQLException ignored) { }
    }

    @Override public void close() {
        ds.close();
    }


    /** Row readers, shared by the registered JDBI mappers and by the statements not yet
     *  converted. Unchanged from when they lived inside the transaction. */
    static Instance readInstance(ResultSet rs) throws SQLException {
        return readInstance(rs, "");
    }

    /** An instance row whose columns are each named {@code prefix} + the column name. */
    static Instance readInstance(ResultSet rs, String prefix) throws SQLException {
        Instance i = new Instance();
        i.id = rs.getString(prefix + "id");
        i.workflow = rs.getString(prefix + "workflow");
        i.version = rs.getInt(prefix + "version");
        i.correlationId = rs.getString(prefix + "correlation_id");
        i.status = InstanceStatus.valueOf(rs.getString(prefix + "status"));
        i.terminationReason = rs.getString(prefix + "term_reason");
        i.error = rs.getString(prefix + "error");
        i.context = Doc.parse(rs.getString(prefix + "context"));
        i.parentTokenId = rs.getString(prefix + "parent_token_id");
        i.createdAt = rs.getLong(prefix + "created_at");
        i.updatedAt = rs.getLong(prefix + "updated_at");
        i.revision = rs.getLong(prefix + "revision");
        return i;
    }

    /** Every column {@link #readInstance} reads. */
    private static final List<String> INSTANCE_COLUMNS = List.of("id", "workflow", "version", "correlation_id",
            "status", "term_reason", "error", "context", "parent_token_id", "created_at", "updated_at",
            "revision");

    static Token readToken(ResultSet rs) throws SQLException {
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
        t.instCreatedAt = rs.getLong("inst_created_at");
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
        t.stepInput = rs.getString("step_input");
        t.stepOutput = rs.getString("step_output");
        t.createdAt = rs.getLong("created_at");
        t.updatedAt = rs.getLong("updated_at");
        return t;
    }

    static ServerNode readNode(ResultSet rs) throws SQLException {
        ServerNode n = new ServerNode();
        n.id = rs.getString("id");
        n.name = rs.getString("name");
        n.firstHeartbeat = rs.getLong("first_heartbeat");
        n.lastHeartbeat = rs.getLong("last_heartbeat");
        n.workers = rs.getInt("workers");
        n.leader = rs.getInt("leader") == 1;
        n.topologyGeneration = rs.getLong("topology_generation");
        return n;
    }

    static Rows.Schedule readSchedule(ResultSet rs) throws SQLException {
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

    static Rows.Event readEvent(ResultSet rs) throws SQLException {
        return new Rows.Event(rs.getLong("seq"), rs.getString("instance_id"), rs.getString("workflow"),
                rs.getInt("version"), rs.getString("correlation_id"), rs.getString("type"),
                rs.getString("node_id"), rs.getInt("payload_ver"), rs.getString("payload"),
                rs.getLong("created_at"));
    }

    static Rows.EventCursor readCursor(ResultSet rs) throws SQLException {
        return new Rows.EventCursor(rs.getString("consumer"), rs.getLong("acked_seq"),
                rs.getLong("last_seen"), rs.getLong("created_at"));
    }

    private static final class JdbcTx implements Tx {
        private final Handle h;
        private final Connection c;
        private final Dialect dialect;
        private final JdbcStorage owner;

        JdbcTx(Handle h, Dialect dialect, JdbcStorage owner) {
            this.h = h;
            this.c = h.getConnection();
            this.dialect = dialect;
            this.owner = owner;
            h.registerRowMapper(Token.class, (rs, ctx) -> recorded(readToken(rs)));
        }

        /** Notes that the row's stored payload is {@code t.payload}, as of this transaction. */
        private Token recorded(Token t) {
            t.storedPayload = t.payload;
            t.storedIn = this;
            return t;
        }

        private PreparedStatement ps(String sql) throws SQLException { return c.prepareStatement(sql); }

        private StorageException wrap(SQLException e) {
            return new StorageException(e.getMessage(), e, dialect.isTransient(e)
                    ? Classification.TRANSIENT : Classification.PERMANENT);
        }

        /** JDBI reports a failed statement as an unchecked exception carrying the driver's
         *  SQLException somewhere in its cause chain; the dialect still decides what counts. */
        private boolean isDuplicateKey(RuntimeException e) {
            for (Throwable t = e; t != null; t = t.getCause()) {
                if (t instanceof SQLException sql && dialect.isDuplicateKey(sql)) return true;
            }
            return false;
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
                h.createUpdate(dialect.insertIgnore("INSERT INTO wf_definition "
                                + "(name,version,body,registered_at,fingerprint,fingerprint_algo) VALUES "
                                + "(:name,:version,:body,:registeredAt,:fingerprint,:algo)"))
                        .bind("name", name)
                        .bind("version", version)
                        .bind("body", json)
                        .bind("registeredAt", System.currentTimeMillis())
                        .bind("fingerprint", fingerprint)
                        .bind("algo", fingerprintAlgo)
                        .execute();
            } catch (RuntimeException e) {
                if (!isDuplicateKey(e)) throw e;
            }
        }

        @Override public void replaceDefinition(String name, int version, String json,
                                                String fingerprint, String fingerprintAlgo) {
            h.createUpdate("UPDATE wf_definition SET body=:body, registered_at=:registeredAt, "
                            + "fingerprint=:fingerprint, fingerprint_algo=:algo "
                            + "WHERE name=:name AND version=:version")
                    .bind("body", json)
                    .bind("registeredAt", System.currentTimeMillis())
                    .bind("fingerprint", fingerprint)
                    .bind("algo", fingerprintAlgo)
                    .bind("name", name)
                    .bind("version", version)
                    .execute();
        }

        @Override public Optional<StoredFingerprint> definitionFingerprint(String name, int version) {
            return h.createQuery("SELECT fingerprint, fingerprint_algo FROM wf_definition "
                            + "WHERE name=:name AND version=:version FOR UPDATE")
                    .bind("name", name)
                    .bind("version", version)
                    .map((rs, ctx) -> new StoredFingerprint(rs.getString("fingerprint"),
                            rs.getString("fingerprint_algo")))
                    .findFirst();
        }

        @Override public void putGraph(WorkflowDefinition def) {
            // The registry deletes these rows before a replacement, so anything still here is the
            // same graph: a no-op, even when two nodes register it at once.
            if (graphExists(def.name(), def.version())) return;
            PreparedBatch nodes = h.prepareBatch(dialect.insertIgnore("INSERT INTO wf_graph_node "
                    + "(workflow,version,node_id,kind,name,activity,queue,retry_json,sleep_millis,expected,"
                    + "success,reason,is_start,items_key,item_key,loop_budget,compensable,arm_names,collect_key) "
                    + "VALUES (:workflow,:version,:nodeId,:kind,:name,:activity,:queue,:retry,:sleepMillis,"
                    + ":expected,:success,:reason,:isStart,:itemsKey,:itemKey,:loopBudget,:compensable,"
                    + ":armNames,:collectKey)"));
            PreparedBatch edges = h.prepareBatch(dialect.insertIgnore("INSERT INTO wf_graph_edge "
                    + "(workflow,version,from_node,to_node,cond,ordinal) "
                    + "VALUES (:workflow,:version,:from,:to,:cond,:ordinal)"));
            for (Node n : def.nodes().values()) {
                nodes.bind("workflow", def.name())
                        .bind("version", def.version())
                        .bind("nodeId", n.id())
                        .bind("kind", n.kind().name())
                        .bind("name", n.name())
                        .bind("activity", n.activity())
                        .bind("queue", n.queue())
                        .bind("retry", n.retry() == null ? null : Json.write(n.retry().toJson()))
                        .bind("sleepMillis", n.sleepMillis())
                        .bind("expected", n.expected())
                        .bind("success", n.success() ? 1 : 0)
                        .bind("reason", n.reason())
                        .bind("isStart", n.id().equals(def.startNode()) ? 1 : 0)
                        .bind("itemsKey", n.itemsKey())
                        .bind("itemKey", n.itemKey())
                        .bind("loopBudget", n.loopBudget())
                        .bind("compensable", n.compensable() ? 1 : 0)
                        .bind("armNames", n.armNames().isEmpty() ? null : Json.write(n.armNames()))
                        .bind("collectKey", n.collectKey())
                        .add();
                for (Edge e : edgesOf(n)) {
                    edges.bind("workflow", def.name())
                            .bind("version", def.version())
                            .bind("from", n.id())
                            .bind("to", e.to)
                            .bind("cond", e.condition)
                            .bind("ordinal", e.ordinal)
                            .add();
                }
            }
            try {
                nodes.execute();
                if (edges.size() > 0) edges.execute();
            } catch (RuntimeException e) {
                if (!isDuplicateKey(e)) throw e;
            }
        }

        private boolean graphExists(String workflow, int version) {
            return h.createQuery("SELECT 1 FROM wf_graph_node "
                            + "WHERE workflow=:workflow AND version=:version LIMIT 1")
                    .bind("workflow", workflow)
                    .bind("version", version)
                    .mapTo(Integer.class)
                    .findFirst()
                    .isPresent();
        }

        @Override public void deleteGraph(String workflow, int version) {
            deleteGraphRows("wf_graph_edge", workflow, version);
            deleteGraphRows("wf_graph_node", workflow, version);
        }

        /** Edges before nodes: an edge pointing at a node that is gone is worse than neither. */
        private void deleteGraphRows(String table, String workflow, int version) {
            h.createUpdate("DELETE FROM " + table + " WHERE workflow=:workflow AND version=:version")
                    .bind("workflow", workflow)
                    .bind("version", version)
                    .execute();
        }

        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            return h.createQuery("SELECT kind,name,activity,queue,retry_json,sleep_millis,expected,success,"
                            + "reason,items_key,item_key,loop_budget,compensable,arm_names,collect_key "
                            + "FROM wf_graph_node WHERE workflow=:workflow AND version=:version "
                            + "AND node_id=:nodeId")
                    .bind("workflow", workflow)
                    .bind("version", version)
                    .bind("nodeId", nodeId)
                    .map((rs, ctx) -> {
                        NodeKind kind = NodeKind.valueOf(rs.getString("kind"));
                        String retryJson = rs.getString("retry_json");
                        RetryPolicy retry = retryJson == null ? null : RetryPolicy.fromJson(Json.parse(retryJson));
                        String itemsKey = rs.getString("items_key");
                        Combine combine = Combine.of(kind, itemsKey, rs.getString("arm_names"),
                                rs.getString("collect_key"));
                        return assemble(workflow, version, nodeId, kind, rs.getString("name"),
                                rs.getString("activity"), rs.getString("queue"), retry,
                                rs.getLong("sleep_millis"), rs.getInt("expected"),
                                rs.getInt("success") != 0, rs.getString("reason"), combine.itemsKey(),
                                rs.getString("item_key"),
                                rs.getInt("loop_budget"),   // NULL -> 0 (not a loop)
                                rs.getInt("compensable") != 0, combine);
                    })
                    .findFirst();
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
            h.createQuery("SELECT to_node,cond FROM wf_graph_edge "
                            + "WHERE workflow=:workflow AND version=:version AND from_node=:from "
                            + "ORDER BY ordinal")
                    .bind("workflow", workflow)
                    .bind("version", version)
                    .bind("from", id)
                    .map((rs, ctx) -> new String[] {rs.getString("to_node"), rs.getString("cond")})
                    .forEach(e -> targets.absorb(e[0], e[1]));
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
            return h.createQuery("SELECT node_id FROM wf_graph_node "
                            + "WHERE workflow=:workflow AND version=:version AND is_start=1")
                    .bind("workflow", workflow)
                    .bind("version", version)
                    .mapTo(String.class)
                    .findFirst();
        }

        @Override public int graphNodeCount(String workflow, int version) {
            return h.createQuery("SELECT COUNT(*) FROM wf_graph_node WHERE workflow=:workflow AND version=:version")
                    .bind("workflow", workflow)
                    .bind("version", version)
                    .mapTo(Integer.class)
                    .one();
        }

        @Override public Optional<String> definition(String name, int version) {
            return h.createQuery("SELECT body FROM wf_definition WHERE name=:name AND version=:version")
                    .bind("name", name)
                    .bind("version", version)
                    .mapTo(String.class)
                    .findFirst();
        }

        @Override public Optional<Integer> latestVersion(String name) {
            // MAX over no rows is a row holding NULL, not an empty result: findFirst would see
            // one row either way, so the Optional has to come from the value.
            return h.createQuery("SELECT MAX(version) FROM wf_definition WHERE name=:name")
                    .bind("name", name)
                    .mapTo(Integer.class)
                    .findOne();
        }

        @Override public List<String> definitionNames() {
            return h.createQuery("SELECT DISTINCT name FROM wf_definition ORDER BY name")
                    .mapTo(String.class)
                    .list();
        }

        @Override public List<Integer> definitionVersions(String name) {
            return h.createQuery("SELECT version FROM wf_definition WHERE name=:name ORDER BY version")
                    .bind("name", name)
                    .mapTo(Integer.class)
                    .list();
        }

        private static final String INSERT_INSTANCE = "INSERT INTO wf_instance "
                + "(id,workflow,version,correlation_id,status,term_reason,error,context,created_at,updated_at,"
                + "revision,parent_token_id) VALUES "
                + "(:id,:workflow,:version,:correlationId,:status,:termReason,:error,:context,:createdAt,"
                + ":updatedAt,:revision,:parentTokenId)";

        @Override public Optional<Rows.AuthUser> findAuthUser(String name) {
            return h.createQuery("SELECT * FROM wf_auth_user WHERE name=:name")
                    .bind("name", name)
                    .map(JdbcTx::authUser)
                    .findOne();
        }

        @Override public List<Rows.AuthUser> authUsers() {
            return h.createQuery("SELECT * FROM wf_auth_user ORDER BY created_at, name")
                    .map(JdbcTx::authUser)
                    .list();
        }

        private static Rows.AuthUser authUser(ResultSet rs, org.jdbi.v3.core.statement.StatementContext ctx)
                throws SQLException {
            return new Rows.AuthUser(rs.getString("name"), rs.getString("hash"), rs.getString("salt"),
                    rs.getInt("iterations"), rs.getInt("disabled") != 0, rs.getLong("created_at"),
                    rs.getLong("updated_at"));
        }

        @Override public List<String> authRolesOf(String user) {
            return h.createQuery("SELECT role_name FROM wf_auth_user_role WHERE user_name=:user ORDER BY role_name")
                    .bind("user", user)
                    .mapTo(String.class)
                    .list();
        }

        @Override public List<Rows.AuthRole> authRoles() {
            return h.createQuery("SELECT * FROM wf_auth_role ORDER BY name")
                    .map((rs, ctx) -> new Rows.AuthRole(rs.getString("name"),
                            permissionSet(rs.getString("permissions")), rs.getInt("builtin") != 0,
                            rs.getLong("created_at"), rs.getLong("updated_at")))
                    .list();
        }

        private static Set<String> permissionSet(String stored) {
            Set<String> out = new TreeSet<>();
            for (String p : stored.split("\\s+")) if (!p.isEmpty()) out.add(p);
            return out;
        }

        @Override public Optional<Rows.AuthSession> findAuthSession(String idHash) {
            return h.createQuery("SELECT * FROM wf_auth_session WHERE id_hash=:id")
                    .bind("id", idHash)
                    .map((rs, ctx) -> new Rows.AuthSession(rs.getString("id_hash"), rs.getString("user_name"),
                            rs.getLong("expires_at"), rs.getLong("created_at")))
                    .findOne();
        }

        @Override public List<Rows.AuthAudit> authAuditAfter(long afterSeq, int max) {
            return h.createQuery("SELECT * FROM wf_auth_audit WHERE seq>:after ORDER BY seq LIMIT :max")
                    .bind("after", afterSeq)
                    .bind("max", max)
                    .map((rs, ctx) -> new Rows.AuthAudit(rs.getLong("seq"), rs.getLong("at"), rs.getString("actor"),
                            rs.getString("action"), rs.getString("target"), rs.getString("detail")))
                    .list();
        }

        @Override public long authAuditHead() {
            return h.createQuery("SELECT COALESCE(MAX(seq),0) FROM wf_auth_audit").mapTo(Long.class).one();
        }

        @Override public boolean authAuditHas(String action) {
            return h.createQuery("SELECT COUNT(*) FROM wf_auth_audit WHERE action=:action")
                    .bind("action", action)
                    .mapTo(Long.class)
                    .one() > 0;
        }

        @Override public void putAuthUser(Rows.AuthUser u) {
            String update = "UPDATE wf_auth_user SET hash=:hash,salt=:salt,iterations=:iterations,"
                    + "disabled=:disabled,created_at=:created,updated_at=:updated WHERE name=:name";
            if (bindAuthUser(h.createUpdate(update), u).execute() > 0) return;
            if (bindAuthUser(h.createUpdate(dialect.insertIgnore("INSERT INTO wf_auth_user "
                    + "(name,hash,salt,iterations,disabled,created_at,updated_at) VALUES "
                    + "(:name,:hash,:salt,:iterations,:disabled,:created,:updated)")), u).execute() > 0) return;
            bindAuthUser(h.createUpdate(update), u).execute();
        }

        private static Update bindAuthUser(Update q, Rows.AuthUser u) {
            return q.bind("name", u.name())
                    .bind("hash", u.hash())
                    .bind("salt", u.salt())
                    .bind("iterations", u.iterations())
                    .bind("disabled", u.disabled() ? 1 : 0)
                    .bind("created", u.createdAt())
                    .bind("updated", u.updatedAt());
        }

        @Override public boolean deleteAuthUser(String name) {
            h.createUpdate("DELETE FROM wf_auth_user_role WHERE user_name=:name").bind("name", name).execute();
            h.createUpdate("DELETE FROM wf_auth_session WHERE user_name=:name").bind("name", name).execute();
            return h.createUpdate("DELETE FROM wf_auth_user WHERE name=:name").bind("name", name).execute() > 0;
        }

        @Override public void setAuthRolesOf(String user, List<String> roles) {
            h.createUpdate("DELETE FROM wf_auth_user_role WHERE user_name=:user").bind("user", user).execute();
            if (roles.isEmpty()) return;
            PreparedBatch b = h.prepareBatch("INSERT INTO wf_auth_user_role (user_name,role_name) VALUES (:user,:role)");
            for (String r : new TreeSet<>(roles)) b.bind("user", user).bind("role", r).add();
            b.execute();
        }

        @Override public void putAuthRole(Rows.AuthRole r) {
            String update = "UPDATE wf_auth_role SET permissions=:permissions,builtin=:builtin,"
                    + "created_at=:created,updated_at=:updated WHERE name=:name";
            if (bindAuthRole(h.createUpdate(update), r).execute() > 0) return;
            if (bindAuthRole(h.createUpdate(dialect.insertIgnore("INSERT INTO wf_auth_role "
                    + "(name,permissions,builtin,created_at,updated_at) VALUES "
                    + "(:name,:permissions,:builtin,:created,:updated)")), r).execute() > 0) return;
            bindAuthRole(h.createUpdate(update), r).execute();
        }

        private static Update bindAuthRole(Update q, Rows.AuthRole r) {
            return q.bind("name", r.name())
                    .bind("permissions", String.join(" ", new TreeSet<>(r.permissions())))
                    .bind("builtin", r.builtin() ? 1 : 0)
                    .bind("created", r.createdAt())
                    .bind("updated", r.updatedAt());
        }

        @Override public boolean deleteAuthRole(String name) {
            h.createUpdate("DELETE FROM wf_auth_user_role WHERE role_name=:name").bind("name", name).execute();
            return h.createUpdate("DELETE FROM wf_auth_role WHERE name=:name").bind("name", name).execute() > 0;
        }

        @Override public List<Rows.AuthCredential> authCredentials() {
            return h.createQuery("SELECT * FROM wf_auth_credential ORDER BY id").map(JdbcTx::authCredential).list();
        }

        @Override public Optional<Rows.AuthCredential> findAuthCredentialByKeyHash(String keyHash) {
            return h.createQuery("SELECT * FROM wf_auth_credential WHERE key_hash=:h")
                    .bind("h", keyHash).map(JdbcTx::authCredential).findOne();
        }

        @Override public Optional<Rows.AuthCredential> findAuthCredentialBySubject(String subject) {
            return h.createQuery("SELECT * FROM wf_auth_credential WHERE subject=:s")
                    .bind("s", subject).map(JdbcTx::authCredential).findOne();
        }

        private static Rows.AuthCredential authCredential(ResultSet rs, org.jdbi.v3.core.statement.StatementContext ctx)
                throws SQLException {
            long expires = rs.getLong("expires_at");
            Long expiresAt = rs.wasNull() ? null : expires;
            return new Rows.AuthCredential(rs.getString("id"), rs.getString("kind"), rs.getString("key_hash"),
                    rs.getString("subject"), rs.getString("role_name"), rs.getLong("created_at"), expiresAt);
        }

        @Override public void insertAuthCredential(Rows.AuthCredential c) {
            h.createUpdate("INSERT INTO wf_auth_credential (id,kind,key_hash,subject,role_name,created_at,expires_at) "
                            + "VALUES (:id,:kind,:keyHash,:subject,:role,:created,:expires)")
                    .bind("id", c.id())
                    .bind("kind", c.kind())
                    .bind("keyHash", c.keyHash())
                    .bind("subject", c.subject())
                    .bind("role", c.role())
                    .bind("created", c.createdAt())
                    .bind("expires", c.expiresAt())
                    .execute();
        }

        @Override public boolean deleteAuthCredential(String id) {
            return h.createUpdate("DELETE FROM wf_auth_credential WHERE id=:id").bind("id", id).execute() > 0;
        }

        @Override public void insertAuthSession(Rows.AuthSession x) {
            h.createUpdate("INSERT INTO wf_auth_session (id_hash,user_name,expires_at,created_at) "
                            + "VALUES (:id,:user,:expires,:created)")
                    .bind("id", x.idHash())
                    .bind("user", x.user())
                    .bind("expires", x.expiresAt())
                    .bind("created", x.createdAt())
                    .execute();
        }

        @Override public void deleteAuthSession(String idHash) {
            h.createUpdate("DELETE FROM wf_auth_session WHERE id_hash=:id").bind("id", idHash).execute();
        }

        @Override public int deleteAuthSessionsOf(String user, String keepIdHash) {
            return h.createUpdate("DELETE FROM wf_auth_session WHERE user_name=:user AND id_hash<>:keep")
                    .bind("user", user)
                    .bind("keep", keepIdHash == null ? "" : keepIdHash)
                    .execute();
        }

        @Override public int deleteExpiredAuthSessions(long now, int max) {
            List<String> ids = h.createQuery("SELECT id_hash FROM wf_auth_session WHERE expires_at<:now "
                            + "ORDER BY expires_at LIMIT :max")
                    .bind("now", now)
                    .bind("max", max)
                    .mapTo(String.class)
                    .list();
            if (ids.isEmpty()) return 0;
            return h.createUpdate("DELETE FROM wf_auth_session WHERE id_hash IN (<ids>)").bindList("ids", ids).execute();
        }

        @Override public long appendAuthAudit(Rows.AuthAudit e) {
            return h.createUpdate("INSERT INTO wf_auth_audit (at,actor,action,target,detail) "
                            + "VALUES (:at,:actor,:action,:target,:detail)")
                    .bind("at", e.at())
                    .bind("actor", e.actor())
                    .bind("action", e.action())
                    .bind("target", e.target())
                    .bind("detail", e.detail())
                    .executeAndReturnGeneratedKeys("seq")
                    .mapTo(Long.class)
                    .findOne()
                    .orElseThrow(() -> new StorageException("wf_auth_audit insert returned no seq", null));
        }

        @Override public boolean upsertSearchDoc(Rows.SearchDoc d) {
            String update = "UPDATE wf_search_doc SET workflow=:workflow,version=:version,status=:status,"
                    + "correlation_id=:correlationId,text=:text,created_at=:created,updated_at=:updated "
                    + "WHERE instance_id=:id AND updated_at<=:updated";
            if (bindSearchDoc(h.createUpdate(update), d).execute() > 0) return true;
            if (bindSearchDoc(h.createUpdate(dialect.insertIgnore("INSERT INTO wf_search_doc "
                    + "(instance_id,workflow,version,status,correlation_id,text,created_at,updated_at) VALUES "
                    + "(:id,:workflow,:version,:status,:correlationId,:text,:created,:updated)")), d).execute() > 0) {
                return true;
            }
            return bindSearchDoc(h.createUpdate(update), d).execute() > 0;   // another writer inserted it first
        }

        private static Update bindSearchDoc(Update u, Rows.SearchDoc d) {
            return u.bind("id", d.instanceId())
                    .bind("workflow", d.workflow())
                    .bind("version", d.version())
                    .bind("status", d.status())
                    .bind("correlationId", d.correlationId())
                    .bind("text", d.text())
                    .bind("created", d.createdAt())
                    .bind("updated", d.updatedAt());
        }

        @Override public void deleteSearchDoc(String instanceId) {
            h.createUpdate("DELETE FROM wf_search_vec WHERE instance_id=:id").bind("id", instanceId).execute();
            h.createUpdate("DELETE FROM wf_search_doc WHERE instance_id=:id").bind("id", instanceId).execute();
        }

        @Override public void upsertSearchVectors(List<Rows.SearchVector> vectors) {
            boolean native_ = owner.pgvector(h);
            String set = native_ ? "vec=CAST(:vec AS vector),embedding=NULL" : "embedding=:embedding";
            for (Rows.SearchVector v : vectors) {
                Update update = bindVector(h.createUpdate("UPDATE wf_search_vec SET " + set + ",updated_at=:updated "
                        + "WHERE instance_id=:id AND model=:model AND updated_at<=:updated"), v, native_);
                if (update.execute() > 0) continue;
                String insert = native_
                        ? "INSERT INTO wf_search_vec (instance_id,model,vec,updated_at) VALUES (:id,:model,CAST(:vec AS vector),:updated)"
                        : "INSERT INTO wf_search_vec (instance_id,model,embedding,updated_at) VALUES (:id,:model,:embedding,:updated)";
                if (bindVector(h.createUpdate(dialect.insertIgnore(insert)), v, native_).execute() > 0) continue;
                bindVector(h.createUpdate("UPDATE wf_search_vec SET " + set + ",updated_at=:updated "
                        + "WHERE instance_id=:id AND model=:model AND updated_at<=:updated"), v, native_).execute();
            }
        }

        private static Update bindVector(Update u, Rows.SearchVector v, boolean native_) {
            u.bind("id", v.instanceId()).bind("model", v.model()).bind("updated", v.updatedAt());
            return native_ ? u.bind("vec", com.wiggle.server.store.Vectors.literal(v.embedding()))
                    : u.bind("embedding", com.wiggle.server.store.Vectors.encode(v.embedding()));
        }

        @Override public int deleteSearchVectors(String model, int max) {
            List<String> ids = h.createQuery("SELECT instance_id FROM wf_search_vec WHERE model=:model LIMIT :max")
                    .bind("model", model).bind("max", max).mapTo(String.class).list();
            if (ids.isEmpty()) return 0;
            return h.createUpdate("DELETE FROM wf_search_vec WHERE model=:model AND instance_id IN (<ids>)")
                    .bind("model", model).bindList("ids", ids).execute();
        }

        /** The name of {@code model}'s HNSW index: a hash, since a model id may hold any character. */
        private static String vectorIndexName(String model) {
            return "ix_search_vec_" + Integer.toHexString(model.hashCode() & 0x7fffffff);
        }

        @Override public void ensureVectorIndex(String model, int dimension) {
            if (!owner.pgvector(h)) return;
            h.execute("CREATE INDEX IF NOT EXISTS " + vectorIndexName(model) + " ON wf_search_vec USING hnsw "
                    + "((vec::vector(" + dimension + ")) vector_cosine_ops) WHERE model=" + sqlString(model));
        }

        private static String sqlString(String value) {
            return "'" + value.replace("'", "''") + "'";
        }

        /** The filters of {@code f} on {@code wf_search_doc} aliased {@code d}, for a WHERE that already has a condition. */
        private static String docFilters(Rows.SearchQuery f) {
            StringBuilder w = new StringBuilder();
            if (f.workflows() != null) w.append(f.workflows().isEmpty() ? " AND 1=0" : " AND d.workflow IN (<workflows>)");
            if (f.status() != null) w.append(" AND d.status=:status");
            if (f.from() != null) w.append(" AND d.updated_at>=:from");
            if (f.to() != null) w.append(" AND d.updated_at<=:to");
            return w.toString();
        }

        private static void bindFilters(Query q, Rows.SearchQuery f) {
            if (f.workflows() != null && !f.workflows().isEmpty()) q.bindList("workflows", List.copyOf(f.workflows()));
            if (f.status() != null) q.bind("status", f.status());
            if (f.from() != null) q.bind("from", f.from());
            if (f.to() != null) q.bind("to", f.to());
        }

        /**
         * With pgvector, the database orders by cosine distance over the model's HNSW index (the
         * expression and predicate match {@link #ensureVectorIndex}), searching wider than the limit
         * so the filters leave enough. Without it, the filtered candidates are compared here.
         */
        @Override public List<Rows.SearchHit> searchVectors(Rows.VectorQuery q) {
            Rows.SearchQuery f = q.filters();
            String docCols = "d.instance_id,d.workflow,d.version,d.status,d.correlation_id,d.text,d.created_at,d.updated_at";
            if (owner.pgvector(h)) {
                int dim = q.vector().length;
                h.execute("SET LOCAL hnsw.ef_search = " + Math.max(40, Math.min(1000, q.limit() * 4)));
                String distance = "(v.vec::vector(" + dim + ") <=> CAST(:q AS vector(" + dim + ")))";
                Query query = h.createQuery("SELECT " + docCols + ", 1 - " + distance + " AS score "
                                + "FROM wf_search_vec v JOIN wf_search_doc d ON d.instance_id=v.instance_id "
                                // The model is a literal, as in the index predicate, so even a generic plan
                                // can prove the partial index applies.
                                + "WHERE v.model=" + sqlString(q.model()) + " AND v.vec IS NOT NULL" + docFilters(f)
                                + " ORDER BY " + distance + " LIMIT :limit")
                        .bind("q", com.wiggle.server.store.Vectors.literal(q.vector()))
                        .bind("limit", q.limit());
                bindFilters(query, f);
                return query.map((rs, ctx) -> new Rows.SearchHit(searchDoc(rs), rs.getDouble("score"))).list();
            }
            Query query = h.createQuery("SELECT " + docCols + ", v.embedding FROM wf_search_vec v "
                    + "JOIN wf_search_doc d ON d.instance_id=v.instance_id WHERE v.model=:model" + docFilters(f))
                    .bind("model", q.model());
            bindFilters(query, f);
            List<Rows.SearchDoc> docs = new ArrayList<>();
            Map<String, float[]> vectors = new HashMap<>();
            query.map((rs, ctx) -> {
                Rows.SearchDoc d = searchDoc(rs);
                byte[] e = rs.getBytes("embedding");
                if (e != null) vectors.put(d.instanceId(), com.wiggle.server.store.Vectors.decode(e));
                return d;
            }).forEach(docs::add);
            return com.wiggle.server.store.Vectors.nearest(docs, vectors, q.vector(), q.limit());
        }

        @Override public List<Rows.SearchDoc> docsNeedingVector(String model, int max) {
            return h.createQuery("SELECT d.instance_id,d.workflow,d.version,d.status,d.correlation_id,d.text,"
                            + "d.created_at,d.updated_at FROM wf_search_doc d LEFT JOIN wf_search_vec v "
                            + "ON v.instance_id=d.instance_id AND v.model=:model "
                            + "WHERE v.instance_id IS NULL OR v.updated_at<d.updated_at"
                            + (owner.pgvector(h) ? " OR v.vec IS NULL" : "") + " ORDER BY d.updated_at LIMIT :max")
                    .bind("model", model).bind("max", max)
                    .map((rs, ctx) -> searchDoc(rs)).list();
        }

        @Override public long countDocsWithoutVector(String model, long updatedBefore) {
            return h.createQuery("SELECT COUNT(*) FROM wf_search_doc d WHERE d.updated_at<:before AND NOT EXISTS "
                            + "(SELECT 1 FROM wf_search_vec v WHERE v.instance_id=d.instance_id AND v.model=:model)")
                    .bind("before", updatedBefore).bind("model", model).mapTo(Long.class).one();
        }

        @Override public List<Rows.SearchVector> searchVectorsOf(List<String> instanceIds) {
            if (instanceIds.isEmpty()) return List.of();
            boolean native_ = owner.pgvector(h);
            return h.createQuery("SELECT instance_id,model,updated_at,embedding" + (native_ ? ",vec::text AS vec" : "")
                            + " FROM wf_search_vec WHERE instance_id IN (<ids>)")
                    .bindList("ids", instanceIds)
                    .map((rs, ctx) -> {
                        String text = native_ ? rs.getString("vec") : null;
                        float[] v = text != null ? com.wiggle.server.store.Vectors.parseLiteral(text)
                                : com.wiggle.server.store.Vectors.decode(rs.getBytes("embedding"));
                        return new Rows.SearchVector(rs.getString("instance_id"), rs.getString("model"), v,
                                rs.getLong("updated_at"));
                    })
                    .list();
        }

        @Override public List<Rows.SearchModel> searchModels() {
            return h.createQuery("SELECT * FROM wf_search_model ORDER BY started_at, model")
                    .map((rs, ctx) -> {
                        long ready = rs.getLong("ready_at");
                        Long readyAt = rs.wasNull() ? null : ready;
                        return new Rows.SearchModel(rs.getString("model"), rs.getInt("dimension"), rs.getString("state"),
                                rs.getLong("started_at"), readyAt);
                    })
                    .list();
        }

        @Override public void putSearchModel(Rows.SearchModel m) {
            String update = "UPDATE wf_search_model SET dimension=:dim,state=:state,started_at=:started,ready_at=:ready "
                    + "WHERE model=:model";
            if (bindModel(h.createUpdate(update), m).execute() > 0) return;
            if (bindModel(h.createUpdate(dialect.insertIgnore("INSERT INTO wf_search_model "
                    + "(model,dimension,state,started_at,ready_at) VALUES (:model,:dim,:state,:started,:ready)")), m)
                    .execute() > 0) return;
            bindModel(h.createUpdate(update), m).execute();
        }

        private static Update bindModel(Update u, Rows.SearchModel m) {
            return u.bind("model", m.model()).bind("dim", m.dimension()).bind("state", m.state())
                    .bind("started", m.startedAt()).bind("ready", m.readyAt());
        }

        @Override public int deleteSearchDocsBefore(long updatedBefore, int max) {
            List<String> ids = h.createQuery("SELECT instance_id FROM wf_search_doc WHERE updated_at<:before "
                            + "ORDER BY updated_at LIMIT :max")
                    .bind("before", updatedBefore)
                    .bind("max", max)
                    .mapTo(String.class)
                    .list();
            if (ids.isEmpty()) return 0;
            h.createUpdate("DELETE FROM wf_search_vec WHERE instance_id IN (<ids>)").bindList("ids", ids).execute();
            return h.createUpdate("DELETE FROM wf_search_doc WHERE instance_id IN (<ids>)").bindList("ids", ids).execute();
        }

        @Override public List<Rows.SearchDoc> searchDocsAfter(String afterId, int max) {
            return h.createQuery("SELECT " + SEARCH_COLUMNS + " FROM wf_search_doc WHERE instance_id>:after "
                            + "ORDER BY instance_id LIMIT :max")
                    .bind("after", afterId == null ? "" : afterId)
                    .bind("max", max)
                    .map((rs, ctx) -> searchDoc(rs))
                    .list();
        }

        private static final String SEARCH_COLUMNS =
                "instance_id,workflow,version,status,correlation_id,text,created_at,updated_at";

        private static Rows.SearchDoc searchDoc(ResultSet rs) throws SQLException {
            return new Rows.SearchDoc(rs.getString("instance_id"), rs.getString("workflow"), rs.getInt("version"),
                    rs.getString("status"), rs.getString("correlation_id"), rs.getString("text"),
                    rs.getLong("created_at"), rs.getLong("updated_at"));
        }

        /**
         * With full-text support, the database matches and ranks ({@code ts_rank} over the
         * {@code simple} configuration: words as written, no stemming). Without it, the filtered
         * documents are matched word by word here, as the in-memory store does.
         */
        @Override public List<Rows.SearchHit> searchDocs(Rows.SearchQuery q) {
            boolean text = q.text() != null && !q.text().isBlank();
            StringBuilder where = new StringBuilder(" WHERE 1=1");
            if (q.workflows() != null) where.append(q.workflows().isEmpty() ? " AND 1=0" : " AND workflow IN (<workflows>)");
            if (q.status() != null) where.append(" AND status=:status");
            if (q.from() != null) where.append(" AND updated_at>=:from");
            if (q.to() != null) where.append(" AND updated_at<=:to");
            Query query;
            if (dialect.supportsFullText()) {
                String sql = text
                        ? "SELECT " + SEARCH_COLUMNS + ", ts_rank(tsv, plainto_tsquery('simple', :text)) AS score "
                          + "FROM wf_search_doc" + where + " AND tsv @@ plainto_tsquery('simple', :text) "
                          + "ORDER BY score DESC, updated_at DESC, instance_id LIMIT :limit"
                        : "SELECT " + SEARCH_COLUMNS + ", 0 AS score FROM wf_search_doc" + where
                          + " ORDER BY updated_at DESC, instance_id LIMIT :limit";
                query = h.createQuery(sql).bind("limit", q.limit());
                if (text) query.bind("text", q.text());
            } else {
                query = h.createQuery("SELECT " + SEARCH_COLUMNS + ", 0 AS score FROM wf_search_doc" + where
                        + " ORDER BY updated_at DESC, instance_id");
            }
            if (q.workflows() != null && !q.workflows().isEmpty()) query.bindList("workflows", List.copyOf(q.workflows()));
            if (q.status() != null) query.bind("status", q.status());
            if (q.from() != null) query.bind("from", q.from());
            if (q.to() != null) query.bind("to", q.to());
            if (dialect.supportsFullText()) {
                return query.map((rs, ctx) -> new Rows.SearchHit(searchDoc(rs), rs.getDouble("score"))).list();
            }
            return com.wiggle.server.store.SearchText.match(query.map((rs, ctx) -> searchDoc(rs)).list(), q.text(), q.limit());
        }

        @Override public void insertInstance(Instance i) {
            bindInstance(h.createUpdate(INSERT_INSTANCE), i).execute();
        }

        private static <S extends SqlStatement<S>> S bindInstance(S s, Instance i) {
            return s.bind("id", i.id)
                    .bind("workflow", i.workflow)
                    .bind("version", i.version)
                    .bind("correlationId", i.correlationId)
                    .bind("status", i.status.name())
                    .bind("termReason", i.terminationReason)
                    .bind("error", i.error)
                    .bind("context", i.context.json())
                    .bind("createdAt", i.createdAt)
                    .bind("updatedAt", i.updatedAt)
                    .bind("revision", i.revision)
                    .bind("parentTokenId", i.parentTokenId);
        }

        @Override public Optional<Instance> lockInstance(String id) { return loadInstance(id, true); }

        @Override public Optional<Instance> findInstance(String id) { return loadInstance(id, false); }

        @Override public Optional<Instance> lockInstanceOf(String tokenId) {
            return h.createQuery("SELECT * FROM wf_instance WHERE id=(SELECT instance_id FROM wf_token WHERE id=:id)"
                            + " FOR UPDATE")
                    .bind("id", tokenId)
                    .mapTo(Instance.class)
                    .findFirst();
        }

        /** The token and its instance in one statement, both locked, instance first, so each is read
         *  as it stands under its lock. */
        private static final String LOCK_TASK = "SELECT t.*, "
                + INSTANCE_COLUMNS.stream().map(c -> "i." + c + " AS i_" + c).collect(Collectors.joining(","))
                + " FROM wf_instance i JOIN wf_token t ON t.instance_id=i.id WHERE t.id=:id FOR UPDATE OF i, t";

        @Override public Optional<Rows.LockedTask> lockTask(String tokenId) {
            if (!dialect.supportsJoinedLock()) return Tx.super.lockTask(tokenId);
            return h.createQuery(LOCK_TASK)
                    .bind("id", tokenId)
                    .map((rs, ctx) -> new Rows.LockedTask(readInstance(rs, "i_"), recorded(readToken(rs))))
                    .findFirst();
        }

        private Optional<Instance> loadInstance(String id, boolean forUpdate) {
            String sql = forUpdate
                    ? "SELECT * FROM wf_instance WHERE id=:id FOR UPDATE"
                    : "SELECT * FROM wf_instance WHERE id=:id";
            return h.createQuery(sql).bind("id", id).mapTo(Instance.class).findFirst();
        }

        private static final String UPDATE_INSTANCE = "UPDATE wf_instance SET status=:status,"
                + "term_reason=:termReason,error=:error,context=:context,updated_at=:updatedAt,"
                + "revision=revision+1 WHERE id=:id";

        @Override public void updateInstance(Instance i) {
            bindInstanceUpdate(h.createUpdate(UPDATE_INSTANCE), i).execute();
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
            return h.createQuery("SELECT * FROM wf_instance WHERE id IN (<ids>) ORDER BY id FOR UPDATE"
                            + (dialect.supportsSkipLocked() ? " SKIP LOCKED" : ""))
                    .bindList("ids", ids)
                    .mapTo(Instance.class)
                    .list();
        }

        @Override public void updateInstances(List<Instance> instances) {
            if (instances.isEmpty()) return;
            PreparedBatch b = h.prepareBatch(UPDATE_INSTANCE);
            for (Instance i : instances) bindInstanceUpdate(b, i).add();
            requireOneRowEach(b.execute(), "update wf_instance");
            for (Instance i : instances) i.revision++;
        }

        private static <S extends SqlStatement<S>> S bindInstanceUpdate(S s, Instance i) {
            return s.bind("status", i.status.name())
                    .bind("termReason", i.terminationReason)
                    .bind("error", i.error)
                    .bind("context", i.context.json())
                    .bind("updatedAt", i.updatedAt)
                    .bind("id", i.id);
        }

        @Override public List<Instance> findByCorrelation(String correlationId, int limit) {
            return h.createQuery("SELECT * FROM wf_instance WHERE correlation_id=:cid "
                            + "ORDER BY created_at DESC LIMIT :limit")
                    .bind("cid", correlationId)
                    .bind("limit", limit)
                    .mapTo(Instance.class)
                    .list();
        }

        @Override public List<Instance> listInstances(String workflow, InstanceStatus status, int limit) {
            // Both filters are optional, so the clauses are conditional but the binds are not
            // positional: a name binds where it appears, or nowhere, and cannot slide.
            Query q = h.createQuery("SELECT * FROM wf_instance WHERE 1=1"
                    + (workflow != null ? " AND workflow=:workflow" : "")
                    + (status != null ? " AND status=:status" : "")
                    + " ORDER BY created_at DESC LIMIT :limit");
            q.bind("limit", limit);
            if (workflow != null) q.bind("workflow", workflow);
            if (status != null) q.bind("status", status.name());
            return q.mapTo(Instance.class).list();
        }

        @Override public int countInstances(InstanceStatus status) {
            return h.createQuery("SELECT COUNT(*) FROM wf_instance WHERE status=:status")
                    .bind("status", status.name())
                    .mapTo(Integer.class)
                    .one();
        }

        private static final String INSERT_TOKEN = "INSERT INTO wf_token (id,instance_id,workflow,version,"
                + "node_id,kind,status,activity,queue,attempt,available_at,lease_owner,lease_expires,join_stack,"
                + "last_error,created_at,updated_at,payload,comp_seq,started_at,finished_at,inst_created_at,"
                + "step_input,step_output) VALUES "
                + "(:id,:instanceId,:workflow,:version,:nodeId,:kind,:status,:activity,:queue,:attempt,"
                + ":availableAt,:leaseOwner,:leaseExpires,:joinStack,:lastError,:createdAt,:updatedAt,:payload,"
                + ":compSeq,:startedAt,:finishedAt,:instCreatedAt,:stepInput,:stepOutput)";

        @Override public void insertToken(Token t) {
            bindToken(h.createUpdate(INSERT_TOKEN), t).execute();
            recorded(t);
        }

        @Override public void insertTokens(List<Token> tokens) {
            if (tokens.isEmpty()) return;
            PreparedBatch b = h.prepareBatch(INSERT_TOKEN);
            for (Token t : tokens) bindToken(b, t).add();
            requireOneRowEach(b.execute(), "insert wf_token");
            for (Token t : tokens) recorded(t);
        }

        /** Every wf_token column, by name. The empty join-stack sentinel is applied here, not
         *  assumed of the row. */
        private static <S extends SqlStatement<S>> S bindToken(S s, Token t) {
            return bindUpdatable(s, t)
                    .bind("instanceId", t.instanceId)
                    .bind("workflow", t.workflow)
                    .bind("version", t.version)
                    .bind("createdAt", t.createdAt)
                    .bind("instCreatedAt", t.instCreatedAt)
                    .bind("payload", PayloadCodec.encode(t.payload));
        }

        /** The columns {@link #UPDATE_TOKEN_KEEP_PAYLOAD} sets, and the id it matches on. */
        private static <S extends SqlStatement<S>> S bindUpdatable(S s, Token t) {
            return s.bind("id", t.id)
                    .bind("nodeId", t.nodeId)
                    .bind("kind", t.kind.name())
                    .bind("status", t.status.name())
                    .bind("activity", t.activity)
                    .bind("queue", t.queue)
                    .bind("attempt", t.attempt)
                    .bind("availableAt", t.availableAt)
                    .bind("leaseOwner", t.leaseOwner)
                    .bind("leaseExpires", t.leaseExpiresAt)
                    .bind("joinStack", t.joinStack == null ? "" : t.joinStack)
                    .bind("lastError", t.lastError)
                    .bind("updatedAt", t.updatedAt)
                    .bindByType("compSeq", t.compSeq, Long.class)
                    .bindByType("startedAt", t.startedAt, Long.class)
                    .bindByType("finishedAt", t.finishedAt, Long.class)
                    .bindByType("stepInput", t.stepInput, String.class)
                    .bindByType("stepOutput", t.stepOutput, String.class);
        }

        @Override public Optional<Token> findToken(String id) {
            return h.createQuery("SELECT * FROM wf_token WHERE id=:id")
                    .bind("id", id)
                    .mapTo(Token.class)
                    .findFirst();
        }

        @Override public List<Token> findTokens(List<String> ids) {
            if (ids.isEmpty()) return List.of();
            return h.createQuery("SELECT * FROM wf_token WHERE id IN (<ids>)")
                    .bindList("ids", ids)
                    .mapTo(Token.class)
                    .list();
        }

        @Override public List<Token> tokensOf(String instanceId) {
            return h.createQuery("SELECT * FROM wf_token WHERE instance_id=:id ORDER BY id")
                    .bind("id", instanceId)
                    .mapTo(Token.class)
                    .list();
        }

        @Override
        public boolean hasActiveTokens(String instanceId) {
            return h.createQuery("SELECT 1 FROM wf_token WHERE instance_id=:id "
                            + "AND status IN ('READY','RUNNING','WAITING','AWAITING','JOINED') LIMIT 1")
                    .bind("id", instanceId)
                    .mapTo(Integer.class)
                    .findFirst()
                    .isPresent();
        }

        // Every column the insert names except the identity ones, so the same binds serve both.
        private static final String UPDATE_TOKEN = "UPDATE wf_token SET node_id=:nodeId,kind=:kind,"
                + "status=:status,activity=:activity,queue=:queue,attempt=:attempt,"
                + "available_at=:availableAt,lease_owner=:leaseOwner,lease_expires=:leaseExpires,"
                + "join_stack=:joinStack,last_error=:lastError,updated_at=:updatedAt,payload=:payload,"
                + "comp_seq=:compSeq,started_at=:startedAt,finished_at=:finishedAt,"
                + "step_input=:stepInput,step_output=:stepOutput WHERE id=:id";

        /** {@link #UPDATE_TOKEN} minus the payload column, for a row whose payload is unchanged. */
        private static final String UPDATE_TOKEN_KEEP_PAYLOAD = UPDATE_TOKEN.replace("payload=:payload,", "");

        private boolean payloadUnchanged(Token t) {
            return t.storedIn == this && t.payload == t.storedPayload;
        }

        private static <S extends SqlStatement<S>> S bindTokenUpdate(S s, Token t, boolean keepPayload) {
            return keepPayload ? bindUpdatable(s, t) : bindToken(s, t);
        }

        @Override
        public void updateToken(Token t) {
            boolean keep = payloadUnchanged(t);
            bindTokenUpdate(h.createUpdate(keep ? UPDATE_TOKEN_KEEP_PAYLOAD : UPDATE_TOKEN), t, keep).execute();
            recorded(t);
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
            for (Token t : tokens) recorded(t);
        }

        private void updateBatch(List<Token> tokens, boolean keepPayload) {
            if (tokens.isEmpty()) return;
            PreparedBatch b = h.prepareBatch(keepPayload ? UPDATE_TOKEN_KEEP_PAYLOAD : UPDATE_TOKEN);
            for (Token t : tokens) bindTokenUpdate(b, t, keepPayload).add();
            requireOneRowEach(b.execute(), "update wf_token");
        }

        @Override
        public boolean renewLease(String taskId, String leaseOwner, long until, long now) {
            Update u = h.createUpdate("UPDATE wf_token SET lease_expires=:until,updated_at=:now"
                            + " WHERE id=:id AND status='RUNNING'"
                            + (leaseOwner == null ? "" : " AND lease_owner=:owner"))
                    .bind("until", until)
                    .bind("now", now)
                    .bind("id", taskId);
            if (leaseOwner != null) u.bind("owner", leaseOwner);
            return u.execute() == 1;
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
            return h.createQuery("SELECT join_stack FROM wf_token "
                            + "WHERE instance_id=:id AND node_id=:node AND status='JOINED'")
                    .bind("id", instanceId)
                    .bind("node", nodeId)
                    .mapTo(String.class)
                    .list();
        }

        @Override
        public List<Token> joinedAt(String instanceId, String nodeId) {
            return h.createQuery("SELECT * FROM wf_token "
                            + "WHERE instance_id=:id AND node_id=:node AND status='JOINED' ORDER BY id")
                    .bind("id", instanceId)
                    .bind("node", nodeId)
                    .mapTo(Token.class)
                    .list();
        }

        @Override
        public Optional<Token> awaitingSignal(String instanceId, String name) {
            return h.createQuery("SELECT * FROM wf_token WHERE instance_id=:id AND status='AWAITING' "
                            + "AND kind='SIGNAL' AND activity=:name ORDER BY id LIMIT 1")
                    .bind("id", instanceId)
                    .bind("name", name)
                    .mapTo(Token.class)
                    .findFirst();
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
        /** The (workflow, version) filter: one OR-ed pair per version, each named by its position
         *  so the clause and its binds cannot drift apart. */
        private static String versionsClause(Set<WorkflowVersion> versions) {
            if (versions == null || versions.isEmpty()) return "";
            StringBuilder sql = new StringBuilder(" AND (");
            for (int i = 0; i < versions.size(); i++) {
                if (i > 0) sql.append(" OR ");
                sql.append("(workflow=:wf").append(i).append(" AND version=:ver").append(i).append(")");
            }
            return sql.append(")").toString();
        }

        private static void bindVersions(Query q, Set<WorkflowVersion> versions) {
            if (versions == null || versions.isEmpty()) return;
            int i = 0;
            for (WorkflowVersion v : versions) {
                q.bind("wf" + i, v.workflow());
                q.bind("ver" + i, v.version());
                i++;
            }
        }

        /** The claim candidate filter, shared by both dialect paths: ready task tokens that are due,
         *  optionally narrowed to the worker's queues and its bound versions. {@code %s} is the
         *  select list -- the ids alone where the update reads them back, whole rows otherwise. */
        private static String claimFilter(Set<String> queues, Set<WorkflowVersion> versions) {
            return "SELECT %s FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE')"
                    + " AND available_at<=:now"
                    + (queues != null && !queues.isEmpty() ? " AND queue IN (<queues>)" : "")
                    + versionsClause(versions)
                    + " ORDER BY inst_created_at, available_at, id LIMIT :max";
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
            boolean perQueue = queues != null && queues.size() > 1;
            String pick = perQueue
                    ? perQueuePick(queues.size(), versions)
                    : claimFilter(queues, versions).formatted("id") + " FOR UPDATE SKIP LOCKED";
            Query q = h.createQuery("UPDATE wf_token SET status='RUNNING',lease_owner=:owner,"
                    + "lease_expires=:until,updated_at=:now,started_at=:now,finished_at=NULL"
                    + " WHERE id IN (" + pick + ") RETURNING *");
            q.bind("owner", workerId)
                    .bind("until", leaseUntil)
                    // updated_at, started_at and the due cutoff are all this instant
                    .bind("now", now)
                    .bind("max", max);
            if (perQueue) {
                int i = 0;
                for (String queue : queues) q.bind("q" + i++, queue);
            } else if (queues != null && !queues.isEmpty()) {
                q.bindList("queues", List.copyOf(queues));
            }
            bindVersions(q, versions);
            return q.mapTo(Token.class).list();
        }

        /**
         * The claim's pick across several queues: each queue's earliest {@code max} dispatchable
         * rows, read in index order and locked SKIP LOCKED, then the earliest {@code max} of
         * those. One filter over all the queues instead would sort every due row in them on
         * every claim. Rows locked here but not picked stay locked, and are skipped by other
         * claimers, until this claim commits.
         */
        private static String perQueuePick(int queueCount, Set<WorkflowVersion> versions) {
            StringBuilder names = new StringBuilder();
            for (int i = 0; i < queueCount; i++) names.append(i == 0 ? "" : ",").append("(:q").append(i).append(")");
            return "SELECT c.id FROM (VALUES " + names + ") AS q(name) CROSS JOIN LATERAL ("
                    + "SELECT id, inst_created_at, available_at FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE')"
                    + " AND available_at<=:now AND queue=q.name" + versionsClause(versions)
                    + " ORDER BY inst_created_at, available_at, id LIMIT :max FOR UPDATE SKIP LOCKED) c"
                    + " ORDER BY c.inst_created_at, c.available_at, c.id LIMIT :max";
        }

        /** Portable fallback (H2): over-fetch candidates, then compare-and-set each. */
        private List<Token> claimCompareAndSet(String workerId, Set<String> queues,
                                               Set<WorkflowVersion> versions, int max,
                                               long now, long leaseUntil) {
            Query pick = h.createQuery(claimFilter(queues, versions).formatted("*"));
            pick.bind("now", now)
                    .bind("max", max * 4);   // over-fetch: some candidates will lose the CAS race
            if (queues != null && !queues.isEmpty()) pick.bindList("queues", List.copyOf(queues));
            bindVersions(pick, versions);
            List<Token> candidates = pick.mapTo(Token.class).list();

            List<Token> claimed = new ArrayList<>();
            for (Token t : candidates) {
                if (claimed.size() >= max) break;
                int won = h.createUpdate("UPDATE wf_token SET status='RUNNING',lease_owner=:owner,"
                                + "lease_expires=:until,updated_at=:now,started_at=:now,finished_at=NULL"
                                + " WHERE id=:id AND status='READY'")
                        .bind("owner", workerId)
                        .bind("until", leaseUntil)
                        .bind("now", now)
                        .bind("id", t.id)
                        .execute();
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
            return query("SELECT * FROM wf_token WHERE status='WAITING' AND kind IN ('TASK','PREDICATE') " +
                    "AND available_at<=? ORDER BY available_at LIMIT ?", now, max);
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
            return h.createQuery("SELECT * FROM wf_token WHERE status='AWAITING' AND kind='SIGNAL' "
                            + "ORDER BY created_at LIMIT :max")
                    .bind("max", max)
                    .mapTo(Token.class)
                    .list();
        }

        @Override public List<Token> dueSignals(long now, int max) {
            return query("SELECT * FROM wf_token WHERE status='AWAITING' AND kind='SIGNAL' " +
                    "AND available_at>0 AND available_at<=? ORDER BY available_at LIMIT ?", now, max);
        }

        @Override public List<String> childInstanceIds(String parentInstanceId) {
            return h.createQuery("SELECT id FROM wf_instance WHERE parent_token_id IN "
                            + "(SELECT id FROM wf_token WHERE instance_id=:id) ORDER BY id")
                    .bind("id", parentInstanceId)
                    .mapTo(String.class)
                    .list();
        }

        @Override public void putSchedule(Rows.Schedule s) {
            // Upsert by id: the engine reuses the existing id when a schedule for the same
            // workflow already exists, so a re-create updates the row rather than duplicating it.
            // The seven bound parameters are identical across dialects; only the SQL text differs.
            // The dialect supplies the SQL with positional parameters in insert-column order, so
            // this one stays positional: the statement text is the dialect's to spell, not ours.
            h.createUpdate(dialect.scheduleUpsert())
                    .bind(0, s.id)
                    .bind(1, s.workflow)
                    .bind(2, s.intervalMillis)
                    .bind(3, s.cron)
                    .bind(4, s.context.json())
                    .bind(5, s.nextFireAt)
                    .bind(6, s.createdAt)
                    .execute();
        }

        @Override public java.util.Optional<Rows.Schedule> scheduleByWorkflow(String workflow) {
            return h.createQuery("SELECT * FROM wf_schedule WHERE workflow=:workflow")
                    .bind("workflow", workflow)
                    .mapTo(Rows.Schedule.class)
                    .findFirst();
        }

        @Override public void deleteSchedule(String id) {
            h.createUpdate("DELETE FROM wf_schedule WHERE id=:id").bind("id", id).execute();
        }

        @Override public List<Rows.Schedule> schedules() {
            return h.createQuery("SELECT * FROM wf_schedule ORDER BY id")
                    .mapTo(Rows.Schedule.class)
                    .list();
        }

        @Override public List<Rows.Schedule> dueSchedules(long now, int max) {
            return h.createQuery("SELECT * FROM wf_schedule WHERE next_fire_at<=:now "
                            + "ORDER BY next_fire_at LIMIT :max")
                    .bind("now", now)
                    .bind("max", max)
                    .mapTo(Rows.Schedule.class)
                    .list();
        }

        @Override public boolean claimSchedule(String id, long expectedFireAt, long nextFireAt) {
            // Compare-and-set on the fire time: exactly one node moves a schedule forward.
            return h.createUpdate("UPDATE wf_schedule SET next_fire_at=:next "
                            + "WHERE id=:id AND next_fire_at=:expected")
                    .bind("next", nextFireAt)
                    .bind("id", id)
                    .bind("expected", expectedFireAt)
                    .execute() == 1;
        }


        @Override public Rows.QueueDepth queueDepth(long now) {
            return h.createQuery("SELECT COUNT(*) AS depth, COALESCE(MIN(available_at),0) AS oldest "
                            + "FROM wf_token WHERE status='READY' AND kind IN ('TASK','PREDICATE') "
                            + "AND available_at<=:now")
                    .bind("now", now)
                    .map((rs, ctx) -> new Rows.QueueDepth(rs.getInt("depth"), rs.getLong("oldest")))
                    .one();
        }

        @Override public List<Rows.BacklogSlice> backlogByVersion(long now, int max) {
            return h.createQuery("SELECT workflow, version, queue, COUNT(*) AS depth, "
                            + "COALESCE(MIN(available_at),0) AS oldest FROM wf_token "
                            + "WHERE status='READY' AND kind IN ('TASK','PREDICATE') AND available_at<=:now "
                            + "GROUP BY workflow, version, queue ORDER BY COUNT(*) DESC LIMIT :max")
                    .bind("now", now)
                    .bind("max", max)
                    .map((rs, ctx) -> new Rows.BacklogSlice(rs.getString("workflow"), rs.getInt("version"),
                            rs.getString("queue"), rs.getInt("depth"), rs.getLong("oldest")))
                    .list();
        }

        @Override public int countProcessedSince(long since) {
            return h.createQuery("SELECT COUNT(*) FROM wf_token "
                            + "WHERE kind IN ('TASK','PREDICATE') AND status='DONE' AND updated_at>:since")
                    .bind("since", since)
                    .mapTo(Integer.class)
                    .one();
        }

        /** The token sweeps: one bound time and a cap, in that order. */
        private List<Token> query(String sql, long arg, int limit) {
            return h.createQuery(sql)
                    .bind(0, arg)
                    .bind(1, limit)
                    .mapTo(Token.class)
                    .list();
        }

        @Override public void upsertNode(ServerNode n) {
            int updated = h.createUpdate("UPDATE wf_node SET name=:name,last_heartbeat=:beat,"
                            + "workers=:workers,topology_generation=:gen WHERE id=:id")
                    .bind("name", n.name)
                    .bind("beat", n.lastHeartbeat)
                    .bind("workers", n.workers)
                    .bind("gen", n.topologyGeneration)
                    .bind("id", n.id)
                    .execute();
            if (updated > 0) return;
            h.createUpdate("INSERT INTO wf_node (id,name,first_heartbeat,last_heartbeat,workers,leader,"
                            + "topology_generation) VALUES (:id,:name,:first,:beat,:workers,0,:gen)")
                    .bind("id", n.id)
                    .bind("name", n.name)
                    .bind("first", n.firstHeartbeat)
                    .bind("beat", n.lastHeartbeat)
                    .bind("workers", n.workers)
                    .bind("gen", n.topologyGeneration)
                    .execute();
        }

        @Override public List<ServerNode> nodes() {
            return h.createQuery("SELECT * FROM wf_node ORDER BY first_heartbeat, id")
                    .mapTo(ServerNode.class)
                    .list();
        }

        @Override public void deleteNodesOlderThan(long before) {
            h.createUpdate("DELETE FROM wf_node WHERE last_heartbeat<:before")
                    .bind("before", before)
                    .execute();
        }

        @Override public OptionalInt shardIdentity() {
            return h.createQuery("SELECT shard_id FROM wf_shard WHERE k='self'")
                    .mapTo(Integer.class)
                    .findOne()
                    .map(OptionalInt::of)
                    .orElse(OptionalInt.empty());
        }

        @Override public void claimShardIdentity(int shardId) {
            h.createUpdate(dialect.insertIgnore("INSERT INTO wf_shard (k,shard_id) VALUES ('self',:shard)"))
                    .bind("shard", shardId)
                    .execute();
        }

        @Override public OptionalLong shardBeat() {
            List<Long> beats = h.createQuery("SELECT beat_at FROM wf_shard WHERE k='self' AND beat_at IS NOT NULL")
                    .mapTo(Long.class)
                    .list();
            return beats.isEmpty() ? OptionalLong.empty() : OptionalLong.of(beats.getFirst());
        }

        @Override public void writeShardBeat(long now) {
            h.createUpdate("UPDATE wf_shard SET beat_at=:now WHERE k='self'").bind("now", now).execute();
        }

        @Override public List<Rows.ShardRecord> shardRegistry() {
            return h.createQuery("SELECT shard_id,state,first_seen,retired_at FROM wf_shard_registry "
                            + "ORDER BY shard_id")
                    .map((rs, ctx) -> {
                        long retired = rs.getLong("retired_at");
                        Long retiredAt = rs.wasNull() ? null : retired;
                        return new Rows.ShardRecord(rs.getInt("shard_id"),
                                ShardState.valueOf(rs.getString("state")), rs.getLong("first_seen"), retiredAt);
                    })
                    .list();
        }

        @Override public void putShardRecord(Rows.ShardRecord r) {
            String update = "UPDATE wf_shard_registry SET state=:state,first_seen=:first,retired_at=:retired "
                    + "WHERE shard_id=:shard";
            if (bindShard(h.createUpdate(update), r).execute() > 0) return;
            if (bindShard(h.createUpdate(dialect.insertIgnore("INSERT INTO wf_shard_registry "
                    + "(shard_id,state,first_seen,retired_at) VALUES (:shard,:state,:first,:retired)")), r)
                    .execute() > 0) return;
            bindShard(h.createUpdate(update), r).execute();   // another node inserted it first
        }

        private static Update bindShard(Update u, Rows.ShardRecord r) {
            return u.bind("shard", r.shardId())
                    .bind("state", r.state().name())
                    .bind("first", r.firstSeen())
                    .bind("retired", r.retiredAt());
        }

        @Override public void setLeader(String nodeId, boolean leader) {
            h.createUpdate("UPDATE wf_node SET leader=:leader WHERE id=:id")
                    .bind("leader", leader ? 1 : 0)
                    .bind("id", nodeId)
                    .execute();
        }

        @Override public int deleteTerminalInstancesBefore(long updatedBefore, int limit) {
            List<String> ids = new ArrayList<>();
            // ORDER BY is required for SQL Server's OFFSET/FETCH rewrite of LIMIT, and gives every
            // dialect a deterministic "oldest first" deletion order at no cost.
            ids.addAll(h.createQuery("SELECT id FROM wf_instance "
                            + "WHERE status NOT IN ('RUNNING','COMPENSATING') AND updated_at<:before "
                            + "ORDER BY updated_at LIMIT :limit")
                    .bind("before", updatedBefore)
                    .bind("limit", limit)
                    .mapTo(String.class)
                    .list());
            if (ids.isEmpty()) return 0;
            // Children before parents: a token or comp-log row outliving its instance is a leak.
            for (String table : List.of("wf_token", "wf_comp_log")) {
                h.createUpdate("DELETE FROM " + table + " WHERE instance_id IN (<ids>)")
                        .bindList("ids", ids)
                        .execute();
            }
            h.createUpdate("DELETE FROM wf_instance WHERE id IN (<ids>)").bindList("ids", ids).execute();
            return ids.size();
        }

        @Override public void appendCompensation(Rows.CompLog e) {
            h.createUpdate("INSERT INTO wf_comp_log "
                            + "(instance_id,seq,node_id,activity,queue,input_json,result_json,compensated) "
                            + "VALUES (:instanceId,:seq,:nodeId,:activity,:queue,:input,:result,:compensated)")
                    .bind("instanceId", e.instanceId)
                    .bind("seq", e.seq)
                    .bind("nodeId", e.nodeId)
                    .bind("activity", e.activity)
                    .bind("queue", e.queue)
                    .bind("input", e.input == null ? null : e.input.json())
                    .bind("result", e.result == null ? null : e.result.json())
                    .bind("compensated", e.compensated ? 1 : 0)
                    .execute();
        }

        @Override public java.util.List<Rows.CompLog> compensationLog(String instanceId) {
            return h.createQuery("SELECT seq,node_id,activity,queue,input_json,result_json,compensated "
                            + "FROM wf_comp_log WHERE instance_id=:id ORDER BY seq")
                    .bind("id", instanceId)
                    .map((rs, ctx) -> {
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
                    })
                    .list();
        }

        @Override public long appendEvent(Rows.Event e) {
            return h.createUpdate("INSERT INTO wf_event (instance_id,workflow,version,correlation_id,type,"
                            + "node_id,payload_ver,payload,created_at) VALUES "
                            + "(:instanceId,:workflow,:version,:correlationId,:type,:nodeId,:payloadVer,"
                            + ":payload,:createdAt)")
                    .bind("instanceId", e.instanceId())
                    .bind("workflow", e.workflow())
                    .bind("version", e.version())
                    .bind("correlationId", e.correlationId())
                    .bind("type", e.type())
                    .bind("nodeId", e.nodeId())
                    .bind("payloadVer", e.payloadVer())
                    .bind("payload", e.payload())
                    .bind("createdAt", e.createdAt())
                    .executeAndReturnGeneratedKeys("seq")
                    .mapTo(Long.class)
                    .findOne()
                    .orElseThrow(() -> new StorageException("wf_event insert returned no seq", null));
        }

        @Override public List<Rows.Event> eventsAfter(long afterSeq, long createdBefore, int max) {
            return h.createQuery("SELECT seq,instance_id,workflow,version,correlation_id,type,node_id,"
                            + "payload_ver,payload,created_at FROM wf_event "
                            + "WHERE seq>:after AND created_at<:before ORDER BY seq LIMIT :max")
                    .bind("after", afterSeq)
                    .bind("before", createdBefore)
                    .bind("max", max)
                    .mapTo(Rows.Event.class)
                    .list();
        }

        @Override public long latestEventSeq() {
            return h.createQuery("SELECT COALESCE(MAX(seq),0) FROM wf_event").mapTo(Long.class).one();
        }

        @Override public Rows.EventCursor eventCursor(String consumer) {
            return h.createQuery("SELECT consumer,acked_seq,last_seen,created_at FROM wf_event_cursor "
                            + "WHERE consumer=:consumer")
                    .bind("consumer", consumer)
                    .mapTo(Rows.EventCursor.class)
                    .findFirst()
                    .orElse(null);
        }

        @Override public void createEventCursorIfAbsent(Rows.EventCursor cursor) {
            h.createUpdate(dialect.insertIgnore("INSERT INTO wf_event_cursor "
                            + "(consumer,acked_seq,last_seen,created_at) VALUES "
                            + "(:consumer,:acked,:lastSeen,:createdAt)"))
                    .bind("consumer", cursor.consumer())
                    .bind("acked", cursor.ackedSeq())
                    .bind("lastSeen", cursor.lastSeen())
                    .bind("createdAt", cursor.createdAt())
                    .execute();
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
        /** An ack never moves a cursor back, so the new seq appears twice in the CASE. One name,
         *  bound once, rather than two positions that have to hold the same value. */
        private int moveCursor(String consumer, long ackedSeq, long now) {
            return h.createUpdate("UPDATE wf_event_cursor SET acked_seq=CASE WHEN acked_seq<:acked "
                            + "THEN :acked ELSE acked_seq END, last_seen=:now WHERE consumer=:consumer")
                    .bind("acked", ackedSeq)
                    .bind("now", now)
                    .bind("consumer", consumer)
                    .execute();
        }

        @Override public Map<Integer, Long> eventPositions(String consumer) {
            Map<Integer, Long> out = new HashMap<>();
            h.createQuery("SELECT shard_id, acked_seq FROM wf_event_cursor_shard WHERE consumer=:consumer")
                    .bind("consumer", consumer)
                    .map((rs, ctx) -> Map.entry(rs.getInt("shard_id"), rs.getLong("acked_seq")))
                    .forEach(e -> out.put(e.getKey(), e.getValue()));
            return out;
        }

        @Override public Long oldestEventPosition(int shard) {
            // MIN over no cursors is NULL; a consumer with no row for the shard holds 0.
            return h.createQuery("SELECT MIN(COALESCE(p.acked_seq, 0)) FROM wf_event_cursor c "
                            + "LEFT JOIN wf_event_cursor_shard p ON p.consumer=c.consumer AND p.shard_id=:shard")
                    .bind("shard", shard)
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(null);
        }

        @Override public void advanceEventPosition(String consumer, int shard, long ackedSeq) {
            String move = "UPDATE wf_event_cursor_shard SET acked_seq=CASE WHEN acked_seq<:acked THEN :acked "
                    + "ELSE acked_seq END WHERE consumer=:consumer AND shard_id=:shard";
            if (h.createUpdate(move).bind("acked", ackedSeq).bind("consumer", consumer).bind("shard", shard)
                    .execute() > 0) return;
            if (h.createUpdate(dialect.insertIgnore("INSERT INTO wf_event_cursor_shard (consumer,shard_id,acked_seq) "
                    + "VALUES (:consumer,:shard,:acked)")).bind("consumer", consumer).bind("shard", shard)
                    .bind("acked", ackedSeq).execute() > 0) return;
            h.createUpdate(move).bind("acked", ackedSeq).bind("consumer", consumer).bind("shard", shard).execute();
        }

        @Override public Long oldestAckedSeq() {
            // MIN over no cursors is a row holding NULL, so the absence comes from the value.
            return h.createQuery("SELECT MIN(acked_seq) FROM wf_event_cursor")
                    .mapTo(Long.class)
                    .findOne()
                    .orElse(null);
        }

        @Override public int deleteEvents(long createdBefore, Long upToSeq, int max) {
            Query pick = h.createQuery("SELECT seq FROM wf_event WHERE created_at<:before"
                    + (upToSeq != null ? " AND seq<=:upTo" : "")
                    + " ORDER BY seq LIMIT :max");
            pick.bind("before", createdBefore).bind("max", max);
            if (upToSeq != null) pick.bind("upTo", upToSeq);
            List<Long> seqs = pick.mapTo(Long.class).list();
            if (seqs.isEmpty()) return 0;
            return h.createUpdate("DELETE FROM wf_event WHERE seq<=:upTo AND created_at<:before")
                    .bind("upTo", seqs.getLast())
                    .bind("before", createdBefore)
                    .execute();
        }

        @Override public List<Rows.StepDuration> stepDurations(String workflow, int version, long since, int max) {
            return h.createQuery("SELECT node_id, started_at, finished_at, available_at FROM wf_token "
                            + "WHERE workflow=:workflow AND version=:version AND status='DONE' "
                            + "AND finished_at > :since AND started_at IS NOT NULL "
                            + "ORDER BY finished_at DESC LIMIT :max")
                    .bind("workflow", workflow)
                    .bind("version", version)
                    .bind("since", since)
                    .bind("max", max)
                    .map((rs, ctx) -> {
                        long ran = Math.max(0, rs.getLong("finished_at") - rs.getLong("started_at"));
                        long waited = Math.max(0, rs.getLong("started_at") - rs.getLong("available_at"));
                        return new Rows.StepDuration(rs.getString("node_id"), ran, waited);
                    })
                    .list();
        }

        @Override public void markCompensated(String instanceId, long seq) {
            h.createUpdate("UPDATE wf_comp_log SET compensated=1 WHERE instance_id=:id AND seq=:seq")
                    .bind("id", instanceId)
                    .bind("seq", seq)
                    .execute();
        }

        @Override
        public void cancelActiveTokens(String instanceId, long now) {
            h.createUpdate("""
                            UPDATE wf_token
                               SET status='CANCELLED', lease_owner=NULL, lease_expires=0, updated_at=:now
                             WHERE instance_id=:id
                               AND status IN ('READY','RUNNING','WAITING','AWAITING','JOINED')
                            """)
                    .bind("now", now)
                    .bind("id", instanceId)
                    .execute();
        }


    }
}
