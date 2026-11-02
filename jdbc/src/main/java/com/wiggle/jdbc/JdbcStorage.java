package com.wiggle.jdbc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.jdbc.support.SQLStateSQLExceptionTranslator;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.ResourceHolderSupport;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import com.wiggle.core.*;
import com.wiggle.core.Doc;
import com.wiggle.server.store.ClaimFilter;
import com.wiggle.server.store.PayloadCodec;
import com.wiggle.core.InstanceStatus;
import com.wiggle.core.TokenStatus;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Rows.*;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.store.StorageException.Classification;
import com.wiggle.server.store.StorageUnreachableException;
import com.wiggle.server.store.Tx;
import com.wiggle.server.store.WriteBuffer;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Shared-database store. This is what makes multi-node operation work: instance
 * mutual exclusion comes from SELECT ... FOR UPDATE, task hand-out from a
 * conditional UPDATE (compare-and-set on status), and leader election from the
 * wf_node heartbeat table.
 *
 * <p>The store body is dialect-neutral: it writes canonical, PostgreSQL-flavoured SQL and
 * defers every non-portable fragment to a {@link Dialect}. That single body backs PostgreSQL
 * and H2 (both via {@code wiggle-postgres}). Connection pooling is provided by HikariCP, unless the
 * caller supplies its own {@link DataSource}. Every transaction runs through Spring's
 * {@link DataSourceTransactionManager}, in a connection of its own.
 */
public final class JdbcStorage implements Storage {

    private static final System.Logger LOG = System.getLogger(JdbcStorage.class.getName());

    private final Dialect dialect;
    /** The pool, closed with this store only when the store created it. */
    private final DataSource pool;
    private final boolean ownsPool;
    /** {@link #pool}, with each connection reporting to the {@link #observe} listeners while any are attached. */
    private final DataSource observed;
    private final TransactionTemplate transactions;
    /** Statements on the connection of the transaction open on the calling thread. */
    private final NamedParameterJdbcTemplate named;
    /** The store's operations; each runs on the connection of the transaction open on the calling thread. */
    private final JdbcTx tx;
    private final String fingerprint;
    private final boolean readOnly;
    private final List<StatementListener> listeners = new java.util.concurrent.CopyOnWriteArrayList<>();

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
        this(pool(url, user, password, poolSize, dialect, readOnly), true, url, dialect, readOnly);
    }

    /**
     * A store over {@code dataSource}, which the caller owns and closes. Its connections must start in
     * manual-commit, read-committed mode, as the pool this store builds for a URL does. {@code url}
     * identifies the database: every node pointed at the same one must pass the same value.
     */
    public JdbcStorage(DataSource dataSource, String url, Dialect dialect) {
        this(dataSource, false, url, dialect, false);
    }

    /** As {@link #readReplica(String, String, String, int, Dialect)}, over a caller-owned {@code dataSource}. */
    public static JdbcStorage readReplica(DataSource dataSource, String url, Dialect dialect) {
        return new JdbcStorage(dataSource, false, url, dialect, true);
    }

    private JdbcStorage(DataSource pool, boolean ownsPool, String url, Dialect dialect, boolean readOnly) {
        this.dialect = Objects.requireNonNull(dialect, "dialect");
        this.readOnly = readOnly;
        this.fingerprint = fingerprintOf(dialect.id() + ':' + url);
        this.pool = Objects.requireNonNull(pool, "dataSource");
        this.ownsPool = ownsPool;
        this.observed = new DelegatingDataSource(pool) {
            @Override public Connection getConnection() throws SQLException {
                Connection c = super.getConnection();
                return listeners.isEmpty() ? c : ObservedConnections.wrap(c, listeners);
            }
        };
        this.transactions = new TransactionTemplate(new DataSourceTransactionManager(observed));
        this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        JdbcTemplate template = new JdbcTemplate(observed);
        template.setExceptionTranslator(new SQLStateSQLExceptionTranslator());
        this.named = new NamedParameterJdbcTemplate(template);
        this.tx = new JdbcTx(dialect, this);
    }

    /** The HikariCP pool a URL-configured store runs on. */
    private static HikariDataSource pool(String url, String user, String password, int poolSize, Dialect dialect,
                                         boolean readOnly) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        if (user != null) cfg.setUsername(user);
        if (password != null) cfg.setPassword(password);
        cfg.setMaximumPoolSize(Math.max(1, poolSize));
        // Every transaction is bounded by inTx(); a borrowed connection stays in manual-commit,
        // read-committed mode.
        cfg.setAutoCommit(false);
        cfg.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        if (dialect.connectionInitSql() != null) cfg.setConnectionInitSql(dialect.connectionInitSql());
        cfg.setPoolName("wiggle-" + dialect.id() + (readOnly ? "-replica" : ""));
        if (readOnly) {
            cfg.setReadOnly(true);
            cfg.setInitializationFailTimeout(-1);
        }
        return new HikariDataSource(cfg);
    }

    private Connection borrow() {
        try {
            return connection();
        } catch (SQLException e) {
            throw connectionFailure(e);
        }
    }

    /** A pooled connection; one that reports to the {@link #observe} listeners while any are attached. */
    private Connection connection() throws SQLException {
        return observed.getConnection();
    }

    /**
     * Reports every statement, commit and rollback this store issues to {@code listener} until the
     * returned handle is closed. Connections borrowed while no listener is attached are not observed.
     */
    public Observation observe(StatementListener listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
        return () -> listeners.remove(listener);
    }

    /** A listener's attachment to a store; closing it detaches the listener. */
    public interface Observation extends AutoCloseable {
        @Override void close();
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
            // "value too long" from the driver. Nothing else bounded depth: the in-memory store
            // held a plain String, which is why only JDBC deployments hit it. TEXT, like the
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
            // its undo retrying forever. Only JDBC deployments hit it -- the in-memory store held
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
            // the home shard, the registry of the models being built, served or retired. Dropped by 38.
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
            """, Dialect::supportsPartialIndexes),
            // Nodes a step creates at run time (Step.create): one instance's own graph fragment,
            // written once and read by id like a graph node. Keyed by node id alone -- a created
            // node's id is minted unique -- with the instance id for the purge.
            new Migration(33, "dyn-nodes", """
            CREATE TABLE IF NOT EXISTS wf_dyn_node (
              node_id      VARCHAR(64)  PRIMARY KEY,
              instance_id  VARCHAR(128) NOT NULL,
              body         TEXT         NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_dyn_node_instance ON wf_dyn_node (instance_id);
            """),
            // The instance's correlation id on each token, for queue criteria to test without a join.
            // Null on rows written before this.
            new Migration(34, "token-correlation-id", """
            ALTER TABLE wf_token ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(200);
            """),
            // A flow's declared attributes as each step's context had them when it became ready, for
            // queue criteria. t is S (text in s_val), B ('true'/'false' in s_val) or N (n_val). The
            // value indexes let a claim whose predicate is rare start from the matching attributes
            // rather than read every row of the table.
            new Migration(35, "token-attributes", """
            CREATE TABLE IF NOT EXISTS wf_token_attr (
              token_id     VARCHAR(128)   NOT NULL,
              name         VARCHAR(63)    NOT NULL,
              t            CHAR(1)        NOT NULL,
              s_val        VARCHAR(256),
              n_val        DECIMAL(38,10),
              PRIMARY KEY (token_id, name)
            );
            CREATE INDEX IF NOT EXISTS ix_token_attr_s ON wf_token_attr (name, s_val);
            CREATE INDEX IF NOT EXISTS ix_token_attr_n ON wf_token_attr (name, n_val);
            """),
            // Instance listings, newest first, with or without a workflow; and the audit read by
            // action. Each otherwise read its whole table.
            new Migration(36, "listing-indexes", """
            CREATE INDEX IF NOT EXISTS ix_instance_created ON wf_instance (created_at);
            CREATE INDEX IF NOT EXISTS ix_instance_workflow ON wf_instance (workflow, created_at);
            CREATE INDEX IF NOT EXISTS ix_auth_audit_action ON wf_auth_audit (action);
            """),
            // The finished-steps index, full (migration 2) or partial (32): nothing reads it.
            new Migration(37, "drop-token-throughput", """
            DROP INDEX IF EXISTS ix_token_throughput;
            DROP INDEX IF EXISTS ix_token_done_steps;
            """),
            // Vector search is removed: its embeddings (and the pgvector column and HNSW indexes on
            // them, which go with the table) and its model registry.
            new Migration(38, "drop-search-vectors", """
            DROP TABLE IF EXISTS wf_search_vec;
            DROP TABLE IF EXISTS wf_search_model;
            """),
            // A flow's declared search attributes as the instance's context had them when it was last
            // indexed, on the search shard beside the document. t is S (text in s_val), B ('true'/'false'
            // in s_val) or N (n_val). The workflow is repeated here so a filter reads one range of the
            // value index, not every flow's rows of that name.
            new Migration(39, "search-attributes", """
            CREATE TABLE IF NOT EXISTS wf_search_attr (
              instance_id  VARCHAR(128)   NOT NULL,
              workflow     VARCHAR(200)   NOT NULL,
              name         VARCHAR(63)    NOT NULL,
              t            CHAR(1)        NOT NULL,
              s_val        VARCHAR(256),
              n_val        DECIMAL(38,10),
              PRIMARY KEY (instance_id, name)
            );
            CREATE INDEX IF NOT EXISTS ix_search_attr_s ON wf_search_attr (workflow, name, s_val);
            CREATE INDEX IF NOT EXISTS ix_search_attr_n ON wf_search_attr (workflow, name, n_val);
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
        if (readOnly) throw new IllegalStateException("a read replica is not migrated; its primary is");
        MigrationMode mode = modeFromEnv();
        applyMigrations(MIGRATIONS, "baseline", mode);
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
    /**
     * The plan PostgreSQL chooses for a claim of up to {@code max} tasks matching {@code filter} at
     * {@code now}, as {@code EXPLAIN} prints it, one line per plan line. Nothing is claimed. Only for
     * a dialect that claims in one statement.
     */
    public String explainClaim(ClaimFilter filter, int max, long now) {
        if (!dialect.supportsSkipLocked() || !dialect.supportsReturning()) {
            throw new UnsupportedOperationException(dialect.id() + " claims by compare-and-set, not in one statement");
        }
        return attemptTx(tx -> ((JdbcTx) tx).explainClaim(filter, max, now));
    }

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
     * One attempt, in a transaction of its own on a connection of its own. Spring owns the commit
     * boundary, and the repositories work on the connection it bound to the thread.
     */
    private <R> R attemptTx(Function<Tx, R> work) {
        try {
            return transactions.execute(status -> work.apply(tx));
        } catch (CannotCreateTransactionException e) {
            throw connectionFailure(e);
        } catch (TransactionSystemException e) {
            // The rollback after a failed body failed too: the body's failure is the one to report.
            if (e.getApplicationException() instanceof RuntimeException body) throw labelled(body);
            // The commit failed, and its outcome is unknown: the work may already be durable.
            throw new StorageException("commit failed", e, Classification.AMBIGUOUS);
        } catch (RuntimeException e) {
            throw labelled(e);
        }
    }

    /**
     * What this store keeps for the transaction open on it on the calling thread, created at first use
     * and released when the transaction completes. A transaction suspended by a nested one keeps its
     * scope out of the nested one's sight.
     */
    private TxScope scope() {
        if (TransactionSynchronizationManager.getResource(this) instanceof TxScope scope) return scope;
        requireTransaction();
        TxScope scope = new TxScope();
        TransactionSynchronizationManager.bindResource(this, scope);
        TransactionSynchronizationManager.registerSynchronization(
                new ResourceHolderSynchronization<TxScope, JdbcStorage>(scope, this) { });
        return scope;
    }

    /** Identifies the transaction open on this store on the calling thread. */
    Object transactionMark() {
        return scope();
    }

    /** Notes that the row's stored payload is {@code t.payload}, as of the current transaction. */
    Token recorded(Token t) {
        t.storedPayload = t.payload;
        t.storedIn = transactionMark();
        return t;
    }

    /** Refuses to go on unless a transaction is open on this store on the calling thread. */
    void requireTransaction() {
        if (!TransactionSynchronizationManager.hasResource(observed)) {
            throw new IllegalStateException("no transaction is open on this store; run the call inside inTx");
        }
    }

    NamedParameterJdbcTemplate named() { return named; }

    Dialect dialect() { return dialect; }

    /**
     * The transaction open on this store, as a Spring transaction resource: its identity, for the
     * token stamps, and the writes it holds back once {@link #bufferWrites} is asked for.
     */
    private static final class TxScope extends ResourceHolderSupport {
        WriteBuffer buffer;
        boolean flushing;
    }

    /** Holds the current transaction's token and instance writes back until it commits; see {@link WriteBuffer}. */
    void bufferWrites() {
        TxScope scope = scope();
        if (scope.buffer != null) return;
        scope.buffer = new WriteBuffer();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void beforeCommit(boolean readOnly) {
                flushPending();
            }
        });
    }

    /** The current transaction's write buffer, or null when its writes are not held back. */
    WriteBuffer buffer() {
        TxScope scope = scope();
        return scope.flushing ? null : scope.buffer;
    }

    /** Writes what the current transaction holds back, before any other statement of it runs and before it commits. */
    void flushPending() {
        if (!(TransactionSynchronizationManager.getResource(this) instanceof TxScope scope)) return;
        if (scope.buffer == null || scope.flushing || scope.buffer.isEmpty()) return;
        scope.flushing = true;
        try {
            scope.buffer.flushTo(tx.tokens(), tx.instances());
        } finally {
            scope.flushing = false;
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
        if (ownsPool && pool instanceof HikariDataSource hikari) hikari.close();
    }

    /** Row readers, shared by the repositories. */
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
    static final List<String> INSTANCE_COLUMNS = List.of("id", "workflow", "version", "correlation_id",
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
        t.correlationId = rs.getString("correlation_id");
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

    

    /**
     * {@link Tx} over the store's repositories, one per area, until the engine calls them directly.
     * Every operation runs on the connection of the transaction open on the calling thread.
     */
    private static final class JdbcTx implements Tx {
        private final Dialect dialect;
        private final JdbcStorage owner;
        private final JdbcShardRepository shards;
        private final JdbcClusterRepository cluster;
        private final JdbcScheduleRepository schedules;
        private final JdbcAuthRepository auth;
        private final JdbcSearchRepository search;
        private final JdbcCompensationRepository compensation;
        private final JdbcDynamicNodeRepository dynamicNodes;
        private final JdbcEventRepository events;
        private final JdbcGraphRepository graph;
        private final JdbcInstanceRepository instances;
        private final JdbcTokenRepository tokens;

        JdbcTx(Dialect dialect, JdbcStorage owner) {
            this.dialect = dialect;
            this.owner = owner;
            this.shards = new JdbcShardRepository(owner);
            this.cluster = new JdbcClusterRepository(owner);
            this.schedules = new JdbcScheduleRepository(owner);
            this.auth = new JdbcAuthRepository(owner);
            this.search = new JdbcSearchRepository(owner);
            this.compensation = new JdbcCompensationRepository(owner);
            this.dynamicNodes = new JdbcDynamicNodeRepository(owner);
            this.events = new JdbcEventRepository(owner);
            this.graph = new JdbcGraphRepository(owner);
            this.instances = new JdbcInstanceRepository(owner);
            this.tokens = new JdbcTokenRepository(owner);
        }

        @Override public void insertToken(Token t) { tokens.insertToken(t); }
        @Override public void insertTokens(List<Token> tokens) { this.tokens.insertTokens(tokens); }
        @Override public Optional<Token> findToken(String id) { return tokens.findToken(id); }
        @Override public List<Token> findTokens(List<String> ids) { return tokens.findTokens(ids); }
        @Override public List<Token> tokensOf(String instanceId) { return tokens.tokensOf(instanceId); }
        @Override public boolean hasActiveTokens(String instanceId) { return tokens.hasActiveTokens(instanceId); }
        @Override public void updateToken(Token t) { tokens.updateToken(t); }
        @Override public void updateTokens(List<Token> tokens) { this.tokens.updateTokens(tokens); }
        @Override public boolean renewLease(String taskId, String leaseOwner, long until, long now) {
            return tokens.renewLease(taskId, leaseOwner, until, now);
        }
        @Override public List<String> joinStacksAt(String instanceId, String nodeId) {
            return tokens.joinStacksAt(instanceId, nodeId);
        }
        @Override public List<Token> joinedAt(String instanceId, String nodeId) {
            return tokens.joinedAt(instanceId, nodeId);
        }
        @Override public Optional<Token> awaitingSignal(String instanceId, String name) {
            return tokens.awaitingSignal(instanceId, name);
        }
        @Override public List<Token> claimTasks(String workerId, ClaimFilter filter, int max, long now,
                                                long leaseUntil) {
            return tokens.claimTasks(workerId, filter, max, now, leaseUntil);
        }
        @Override public List<Token> dueRetries(long now, int max) { return tokens.dueRetries(now, max); }
        @Override public List<Token> dueTimers(long now, int max) { return tokens.dueTimers(now, max); }
        @Override public List<Token> expiredLeases(long now, int max) { return tokens.expiredLeases(now, max); }
        @Override public List<Token> pendingSignals(int max) { return tokens.pendingSignals(max); }
        @Override public List<Token> dueSignals(long now, int max) { return tokens.dueSignals(now, max); }
        @Override public Rows.QueueDepth queueDepth(long now) { return tokens.queueDepth(now); }
        @Override public int countReady(String workflow, int version, String queue,
                                        com.wiggle.core.criteria.Predicate where, long now, int cap) {
            return tokens.countReady(workflow, version, queue, where, now, cap);
        }
        @Override public List<Rows.BacklogSlice> backlogByVersion(long now, int max) {
            return tokens.backlogByVersion(now, max);
        }
        @Override public List<Rows.StepDuration> stepDurations(String workflow, int version, long since, int max) {
            return tokens.stepDurations(workflow, version, since, max);
        }
        @Override public void cancelActiveTokens(String instanceId, long now) {
            tokens.cancelActiveTokens(instanceId, now);
        }
        String explainClaim(ClaimFilter filter, int max, long now) { return tokens.explainClaim(filter, max, now); }

        @Override public void insertInstance(Instance i) { instances.insertInstance(i); }
        @Override public Optional<Instance> lockInstance(String id) { return instances.lockInstance(id); }
        @Override public Optional<Instance> findInstance(String id) { return instances.findInstance(id); }
        @Override public Optional<Instance> lockInstanceOf(String tokenId) { return instances.lockInstanceOf(tokenId); }
        @Override public void updateInstance(Instance i) { instances.updateInstance(i); }
        @Override public List<Instance> lockInstances(List<String> ids) { return instances.lockInstances(ids); }
        @Override public void updateInstances(List<Instance> instances) { this.instances.updateInstances(instances); }
        @Override public List<Instance> findByCorrelation(String correlationId, int limit) {
            return instances.findByCorrelation(correlationId, limit);
        }
        @Override public List<Instance> listInstances(String workflow, InstanceStatus status, int limit) {
            return instances.listInstances(workflow, status, limit);
        }
        @Override public int countInstances(InstanceStatus status) { return instances.countInstances(status); }
        @Override public int deleteTerminalInstancesBefore(long updatedBefore, int limit) {
            return instances.deleteTerminalInstancesBefore(updatedBefore, limit);
        }
        @Override public List<String> childInstanceIds(String parentInstanceId) {
            return instances.childInstanceIds(parentInstanceId);
        }

        @Override public void putDefinition(String name, int version, String json,
                                            String fingerprint, String fingerprintAlgo) {
            graph.putDefinition(name, version, json, fingerprint, fingerprintAlgo);
        }
        @Override public void replaceDefinition(String name, int version, String json,
                                                String fingerprint, String fingerprintAlgo) {
            graph.replaceDefinition(name, version, json, fingerprint, fingerprintAlgo);
        }
        @Override public Optional<StoredFingerprint> definitionFingerprint(String name, int version) {
            return graph.definitionFingerprint(name, version);
        }
        @Override public void putGraph(WorkflowDefinition def) { graph.putGraph(def); }
        @Override public void deleteGraph(String workflow, int version) { graph.deleteGraph(workflow, version); }
        @Override public Optional<Node> graphNode(String workflow, int version, String nodeId) {
            return graph.graphNode(workflow, version, nodeId);
        }
        @Override public Optional<String> graphStartNode(String workflow, int version) {
            return graph.graphStartNode(workflow, version);
        }
        @Override public int graphNodeCount(String workflow, int version) {
            return graph.graphNodeCount(workflow, version);
        }
        @Override public Optional<String> definition(String name, int version) {
            return graph.definition(name, version);
        }
        @Override public Optional<Integer> latestVersion(String name) { return graph.latestVersion(name); }
        @Override public List<String> definitionNames() { return graph.definitionNames(); }
        @Override public List<Integer> definitionVersions(String name) { return graph.definitionVersions(name); }

        @Override public void advanceEventPosition(String consumer, int shard, long ackedSeq) {
            events.advanceEventPosition(consumer, shard, ackedSeq);
        }
        @Override public long appendEvent(Rows.Event e) { return events.appendEvent(e); }
        @Override public void createEventCursorIfAbsent(Rows.EventCursor cursor) {
            events.createEventCursorIfAbsent(cursor);
        }
        @Override public void advanceEventCursor(String consumer, long ackedSeq, long now) {
            events.advanceEventCursor(consumer, ackedSeq, now);
        }
        @Override public int deleteEvents(long createdBefore, Long upToSeq, int max) {
            return events.deleteEvents(createdBefore, upToSeq, max);
        }
        @Override public Map<Integer, Long> eventPositions(String consumer) { return events.eventPositions(consumer); }
        @Override public Long oldestEventPosition(int shard) { return events.oldestEventPosition(shard); }
        @Override public List<Rows.Event> eventsAfter(long afterSeq, long createdBefore, int max) {
            return events.eventsAfter(afterSeq, createdBefore, max);
        }
        @Override public long latestEventSeq() { return events.latestEventSeq(); }
        @Override public Rows.EventCursor eventCursor(String consumer) { return events.eventCursor(consumer); }
        @Override public Long oldestAckedSeq() { return events.oldestAckedSeq(); }

        @Override public void insertDynNodes(String instanceId, java.util.List<Node> nodes) {
            dynamicNodes.insertDynNodes(instanceId, nodes);
        }
        @Override public Optional<Node> dynNode(String nodeId) { return dynamicNodes.dynNode(nodeId); }
        @Override public long dynNodeCount(String instanceId) { return dynamicNodes.dynNodeCount(instanceId); }

        @Override public void appendCompensation(Rows.CompLog e) { compensation.appendCompensation(e); }
        @Override public void markCompensated(String instanceId, long seq) {
            compensation.markCompensated(instanceId, seq);
        }
        @Override public java.util.List<Rows.CompLog> compensationLog(String instanceId) {
            return compensation.compensationLog(instanceId);
        }

        @Override public boolean upsertSearchDoc(Rows.SearchDoc d) { return search.upsertSearchDoc(d); }
        @Override public void deleteSearchDoc(String instanceId) { search.deleteSearchDoc(instanceId); }
        @Override public int deleteSearchDocsBefore(long updatedBefore, int max) {
            return search.deleteSearchDocsBefore(updatedBefore, max);
        }
        @Override public List<Rows.SearchDoc> searchDocsAfter(String afterId, int max) {
            return search.searchDocsAfter(afterId, max);
        }
        @Override public List<Rows.SearchHit> searchDocs(Rows.SearchQuery q) { return search.searchDocs(q); }

        @Override public Optional<Rows.AuthUser> findAuthUser(String name) { return auth.findAuthUser(name); }
        @Override public List<Rows.AuthUser> authUsers() { return auth.authUsers(); }
        @Override public List<String> authRolesOf(String user) { return auth.authRolesOf(user); }
        @Override public List<Rows.AuthRole> authRoles() { return auth.authRoles(); }
        @Override public Optional<Rows.AuthSession> findAuthSession(String idHash) {
            return auth.findAuthSession(idHash);
        }
        @Override public List<Rows.AuthAudit> authAuditAfter(long afterSeq, int max) {
            return auth.authAuditAfter(afterSeq, max);
        }
        @Override public long authAuditHead() { return auth.authAuditHead(); }
        @Override public boolean authAuditHas(String action) { return auth.authAuditHas(action); }
        @Override public void putAuthUser(Rows.AuthUser u) { auth.putAuthUser(u); }
        @Override public boolean deleteAuthUser(String name) { return auth.deleteAuthUser(name); }
        @Override public void setAuthRolesOf(String user, List<String> roles) { auth.setAuthRolesOf(user, roles); }
        @Override public void putAuthRole(Rows.AuthRole r) { auth.putAuthRole(r); }
        @Override public boolean deleteAuthRole(String name) { return auth.deleteAuthRole(name); }
        @Override public List<Rows.AuthCredential> authCredentials() { return auth.authCredentials(); }
        @Override public Optional<Rows.AuthCredential> findAuthCredentialByKeyHash(String keyHash) {
            return auth.findAuthCredentialByKeyHash(keyHash);
        }
        @Override public Optional<Rows.AuthCredential> findAuthCredentialBySubject(String subject) {
            return auth.findAuthCredentialBySubject(subject);
        }
        @Override public void insertAuthCredential(Rows.AuthCredential c) { auth.insertAuthCredential(c); }
        @Override public boolean deleteAuthCredential(String id) { return auth.deleteAuthCredential(id); }
        @Override public void insertAuthSession(Rows.AuthSession x) { auth.insertAuthSession(x); }
        @Override public void deleteAuthSession(String idHash) { auth.deleteAuthSession(idHash); }
        @Override public int deleteAuthSessionsOf(String user, String keepIdHash) {
            return auth.deleteAuthSessionsOf(user, keepIdHash);
        }
        @Override public int deleteExpiredAuthSessions(long now, int max) {
            return auth.deleteExpiredAuthSessions(now, max);
        }
        @Override public long appendAuthAudit(Rows.AuthAudit e) { return auth.appendAuthAudit(e); }

        @Override public void putSchedule(Rows.Schedule s) { schedules.putSchedule(s); }
        @Override public void deleteSchedule(String id) { schedules.deleteSchedule(id); }
        @Override public List<Rows.Schedule> dueSchedules(long now, int max) {
            return schedules.dueSchedules(now, max);
        }
        @Override public boolean claimSchedule(String id, long expectedFireAt, long nextFireAt) {
            return schedules.claimSchedule(id, expectedFireAt, nextFireAt);
        }
        @Override public List<Rows.Schedule> schedules() { return schedules.schedules(); }
        @Override public java.util.Optional<Rows.Schedule> scheduleByWorkflow(String workflow) {
            return schedules.scheduleByWorkflow(workflow);
        }

        @Override public void upsertNode(ServerNode n) { cluster.upsertNode(n); }
        @Override public void deleteNodesOlderThan(long before) { cluster.deleteNodesOlderThan(before); }
        @Override public void setLeader(String nodeId, boolean leader) { cluster.setLeader(nodeId, leader); }
        @Override public List<ServerNode> nodes() { return cluster.nodes(); }

        @Override public void claimShardIdentity(int shardId) { shards.claimShardIdentity(shardId); }
        @Override public void writeShardBeat(long now) { shards.writeShardBeat(now); }
        @Override public void putShardRecord(Rows.ShardRecord r) { shards.putShardRecord(r); }
        @Override public OptionalInt shardIdentity() { return shards.shardIdentity(); }
        @Override public OptionalLong shardBeat() { return shards.shardBeat(); }
        @Override public List<Rows.ShardRecord> shardRegistry() { return shards.shardRegistry(); }

        @Override public void bufferWrites() { owner.bufferWrites(); }

        JdbcTokenRepository tokens() { return tokens; }

        JdbcInstanceRepository instances() { return instances; }

        @Override public Optional<Rows.LockedTask> lockTask(String tokenId) {
            return dialect.supportsJoinedLock() ? instances.lockTask(tokenId) : Tx.super.lockTask(tokenId);
        }
    }
}
