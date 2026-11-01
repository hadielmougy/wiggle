package com.wiggle.tests;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.store.StorageException.Classification;
import com.wiggle.server.store.StorageUnreachableException;
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
 * owns, with a commit whose outcome is unknown, nested, and with no connection to be had.
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
                storage.inTxVoid(tx -> tx.putDefinition("owned", 1, "{}", "fp", "sha-256"));
                assertTrue(storage.inTx(tx -> tx.definition("owned", 1)).isPresent());
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
                StorageException e = assertThrows(StorageException.class, () -> storage.inTxVoid(tx -> {
                    attempts.incrementAndGet();
                    tx.putDefinition("maybe", 1, "{}", "fp", "sha-256");
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
            assertTrue(storage.inTx(tx -> tx.definition("inner", 1)).isPresent(), "the nested transaction committed");
            assertFalse(storage.inTx(tx -> tx.definition("outer", 1)).isPresent(), "the outer one rolled back");
        }
    }

    @Test @DisplayName("a pool that cannot reach the database fails the transaction as unreachable")
    void unreachableDatabase() {
        SQLTransientConnectionException timeout = new SQLTransientConnectionException(
                "Connection is not available, request timed out", "08001", new SQLException("Connection refused"));
        DataSource dead = connections(null, c -> { throw timeout; });
        try (JdbcStorage storage = new JdbcStorage(dead, "jdbc:h2:mem:dead", new H2Dialect())) {
            StorageException e = assertThrows(StorageException.class, () -> storage.inTx(tx -> tx.definition("x", 1)));
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
