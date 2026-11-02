package com.wiggle.tests;

import com.wiggle.core.Doc;
import com.wiggle.core.InstanceStatus;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.jdbc.StatementListener;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.server.store.Repositories;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.store.StorageException.Classification;
import com.wiggle.server.store.StorageUnreachableException;
import com.wiggle.server.store.Tx;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store's transaction boundary, run through Spring's transaction manager: over a pool the caller
 * owns, with a commit whose outcome is unknown, nested, outside a transaction, with its writes held
 * back, and with no connection to be had.
 */
class JdbcTransactionsTest {

    private static HikariDataSource pool(String label) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl("jdbc:h2:mem:" + label + "-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
        cfg.setAutoCommit(false);
        cfg.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        cfg.setMaximumPoolSize(4);
        return new HikariDataSource(cfg);
    }

    private static JdbcStorage store(DataSource ds) {
        JdbcStorage storage = new JdbcStorage(ds, "jdbc:h2:mem:test", new H2Dialect());
        storage.migrate();
        return storage;
    }

    @Test @DisplayName("a store over a caller-owned pool commits through it, and closing the store leaves the pool open")
    void callerOwnedPool() {
        try (HikariDataSource ds = pool("owned")) {
            try (JdbcStorage storage = store(ds)) {
                TestTx.inHome(storage, () -> Repositories.graph().putDefinition("owned", 1, "{}", "fp", "sha-256"));
                assertTrue(TestTx.inHome(storage, () -> Repositories.graph().definition("owned", 1)).isPresent());
            }
            assertFalse(ds.isClosed(), "the caller closes its own pool");
        }
    }

    @Test @DisplayName("a commit that fails is AMBIGUOUS and is not replayed, whatever its SQL state says")
    void failedCommitIsAmbiguous() {
        AtomicBoolean failCommit = new AtomicBoolean();
        try (HikariDataSource ds = pool("commit")) {
            DataSource failing = connections(ds, c -> proxy(c, failCommit));
            try (JdbcStorage storage = store(failing)) {
                failCommit.set(true);
                AtomicInteger attempts = new AtomicInteger();
                StorageException e = assertThrows(StorageException.class, () -> TestTx.inHome(storage, () -> {
                    attempts.incrementAndGet();
                    Repositories.graph().putDefinition("maybe", 1, "{}", "fp", "sha-256");
                }));
                assertEquals(Classification.AMBIGUOUS, e.classification());
                assertFalse(e.repeatable(), "work that may be durable must not be re-applied");
                assertEquals(1, attempts.get(), "a transient SQL state on the commit does not earn a replay");
            }
        }
    }

    @Test @DisplayName("a nested transaction runs on a connection of its own and commits even when the outer rolls back")
    void nestedTransactionIsIndependent() {
        try (HikariDataSource ds = pool("nested"); JdbcStorage storage = store(ds)) {
            RuntimeException outerFailure = new IllegalStateException("outer fails");
            RuntimeException thrown = assertThrows(RuntimeException.class, () -> storage.inTxVoid(outer -> {
                outer.putDefinition("outer", 1, "{}", "fp", "sha-256");
                storage.inTxVoid(inner -> inner.putDefinition("inner", 1, "{}", "fp", "sha-256"));
                throw outerFailure;
            }));
            assertEquals(outerFailure, thrown);
            assertTrue(TestTx.inHome(storage, () -> Repositories.graph().definition("inner", 1)).isPresent(), "the nested transaction committed");
            assertFalse(TestTx.inHome(storage, () -> Repositories.graph().definition("outer", 1)).isPresent(), "the outer one rolled back");
        }
    }

    @Test @DisplayName("store operations run only inside a transaction: a Tx kept past its transaction refuses to run")
    void operationsNeedATransaction() {
        try (HikariDataSource ds = pool("escaped"); JdbcStorage storage = store(ds)) {
            Tx escaped = storage.inTx(tx -> tx);
            IllegalStateException e = assertThrows(IllegalStateException.class, () -> escaped.definition("x", 1));
            assertTrue(e.getMessage().contains("no transaction"), e.getMessage());
        }
    }

    @Test @DisplayName("held-back writes reach the database before the next statement, and at commit")
    void heldBackWritesReachTheDatabase() {
        try (HikariDataSource ds = pool("held"); JdbcStorage storage = store(ds)) {
            Rows.Instance i = instance("wfi-held");
            Rows.Token t = token(i, "tok-held");
            List<String> statements = new ArrayList<>();
            try (JdbcStorage.Observation watch = storage.observe(listener(statements))) {
                TestTx.inHome(storage, () -> {
                    Repositories.instances().insertInstance(i);
                    Repositories.bufferWrites();
                    Repositories.tokens().insertToken(t);
                    t.status = TokenStatus.RUNNING;
                    Repositories.tokens().updateToken(t);
                    assertTrue(statements.stream().noneMatch(sql -> sql.contains("wf_token")), "nothing written yet");
                    assertEquals(TokenStatus.RUNNING, Repositories.tokens().findToken(t.id).orElseThrow().status,
                            "a read in the transaction sees the held-back writes");
                });
            }
            assertEquals(1, statements.stream().filter(sql -> sql.startsWith("INSERT INTO wf_token ")).count(),
                    "a token inserted and moved on is one insert");
            assertEquals(0, statements.stream().filter(sql -> sql.startsWith("UPDATE wf_token ")).count());

            Rows.Token late = token(i, "tok-late");
            TestTx.inHome(storage, () -> {
                Repositories.bufferWrites();
                Repositories.tokens().insertToken(late);
            });
            assertTrue(TestTx.inHome(storage, () -> Repositories.tokens().findToken(late.id)).isPresent(), "written at commit, with no read before it");
        }
    }

    @Test @DisplayName("held-back writes of a transaction that throws are never written")
    void heldBackWritesRollBack() {
        try (HikariDataSource ds = pool("held-rollback"); JdbcStorage storage = store(ds)) {
            Rows.Instance i = instance("wfi-gone");
            Rows.Token t = token(i, "tok-gone");
            assertThrows(IllegalStateException.class, () -> TestTx.inHome(storage, () -> {
                Repositories.instances().insertInstance(i);
                Repositories.bufferWrites();
                Repositories.tokens().insertToken(t);
                throw new IllegalStateException("abandoned");
            }));
            assertFalse(TestTx.inHome(storage, () -> Repositories.instances().findInstance(i.id)).isPresent());
            assertFalse(TestTx.inHome(storage, () -> Repositories.tokens().findToken(t.id)).isPresent());
        }
    }

    private static StatementListener listener(List<String> statements) {
        return new StatementListener() {
            @Override public void executed(String sql) { statements.add(sql.strip()); }
            @Override public void committed() { }
        };
    }

    private static Rows.Instance instance(String id) {
        Rows.Instance i = new Rows.Instance();
        i.id = id;
        i.workflow = "w";
        i.version = 1;
        i.context = Doc.EMPTY;
        i.status = InstanceStatus.RUNNING;
        return i;
    }

    private static Rows.Token token(Rows.Instance of, String id) {
        Rows.Token t = new Rows.Token();
        t.id = id;
        t.instanceId = of.id;
        t.workflow = of.workflow;
        t.version = of.version;
        t.nodeId = "n1";
        t.kind = NodeKind.TASK;
        t.status = TokenStatus.READY;
        t.activity = "w#step";
        return t;
    }

    @Test @DisplayName("a pool that cannot reach the database fails the transaction as unreachable")
    void unreachableDatabase() {
        SQLTransientConnectionException timeout = new SQLTransientConnectionException(
                "Connection is not available, request timed out", "08001", new SQLException("Connection refused"));
        DataSource dead = connections(null, c -> { throw timeout; });
        try (JdbcStorage storage = new JdbcStorage(dead, "jdbc:h2:mem:dead", new H2Dialect())) {
            StorageException e = assertThrows(StorageException.class, () -> TestTx.inHome(storage, () -> Repositories.graph().definition("x", 1)));
            assertInstanceOf(StorageUnreachableException.class, e);
        }
    }

    private interface ConnectionMapper {
        Connection map(Connection c) throws SQLException;
    }

    /** A connection whose commit fails, with a transient SQL state, while {@code fail} is set. */
    private static Connection proxy(Connection c, AtomicBoolean fail) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (p, m, args) -> {
                    if (m.getName().equals("commit") && fail.get()) {
                        throw new SQLException("connection lost during commit", "40001");
                    }
                    try {
                        return m.invoke(c, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    /** {@code ds} with each connection passed through {@code mapper}; {@code ds} may be null if the mapper never reads it. */
    private static DataSource connections(DataSource ds, ConnectionMapper mapper) {
        return new DataSource() {
            @Override public Connection getConnection() throws SQLException {
                return mapper.map(ds == null ? null : ds.getConnection());
            }
            @Override public Connection getConnection(String u, String p) throws SQLException { return getConnection(); }
            @Override public PrintWriter getLogWriter() { return null; }
            @Override public void setLogWriter(PrintWriter out) { }
            @Override public void setLoginTimeout(int seconds) { }
            @Override public int getLoginTimeout() { return 0; }
            @Override public Logger getParentLogger() { return Logger.getGlobal(); }
            @Override public <T> T unwrap(Class<T> iface) throws SQLException { throw new SQLException("not a wrapper"); }
            @Override public boolean isWrapperFor(Class<?> iface) { return false; }
        };
    }
}
