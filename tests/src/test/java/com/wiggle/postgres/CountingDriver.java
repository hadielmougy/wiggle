package com.wiggle.postgres;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

/**
 * A JDBC driver that counts the round trips made through it and hands everything to the real
 * driver. {@code jdbc:counting:postgresql://...} opens {@code jdbc:postgresql://...}; each statement
 * execution (a batch counts once) is tallied under its SQL's leading words, and each commit under
 * {@code COMMIT}. Counts are process-wide; {@link #reset} and {@link #snapshot} bracket a phase.
 */
final class CountingDriver implements Driver {

    static final String PREFIX = "jdbc:counting:";
    static final String COMMIT = "COMMIT";

    private static final Map<String, LongAdder> COUNTS = new ConcurrentHashMap<>();
    private static final CountingDriver INSTANCE = new CountingDriver();

    static {
        try {
            DriverManager.registerDriver(INSTANCE);
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** {@code url} rewritten to open through this driver. */
    static String wrap(String url) {
        return PREFIX + url.substring("jdbc:".length());
    }

    static void reset() {
        COUNTS.clear();
    }

    /** The counts since the last {@link #reset}, by statement shape. */
    static Map<String, Long> snapshot() {
        Map<String, Long> out = new TreeMap<>();
        COUNTS.forEach((k, v) -> out.put(k, v.sum()));
        return out;
    }

    /** The first and last words of {@code sql}, enough to tell one statement from another. */
    static String shape(String sql) {
        String flat = sql.replaceAll("\\s+", " ").trim();
        return flat.length() <= 110 ? flat : flat.substring(0, 70) + " ... " + flat.substring(flat.length() - 35);
    }

    private static void count(String key) {
        COUNTS.computeIfAbsent(key, k -> new LongAdder()).increment();
    }

    @Override public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        Connection real = DriverManager.getConnection("jdbc:" + url.substring(PREFIX.length()), info);
        return proxy(Connection.class, real, (target, m, args) -> {
            Object out = invoke(target, m, args);
            switch (m.getName()) {
                case "commit" -> count(COMMIT);
                case "prepareStatement", "prepareCall" -> {
                    String sql = (String) args[0];
                    Class<? extends Statement> type = m.getName().equals("prepareCall")
                            ? CallableStatement.class : PreparedStatement.class;
                    return proxy(type, out, (st, sm, sargs) -> {
                        if (sm.getName().startsWith("execute")) count(shape(sql));
                        return invoke(st, sm, sargs);
                    });
                }
                case "createStatement" -> {
                    return proxy(Statement.class, out, (st, sm, sargs) -> {
                        if (sm.getName().startsWith("execute") && sargs != null && sargs.length > 0
                                && sargs[0] instanceof String sql) count(shape(sql));
                        else if (sm.getName().equals("executeBatch")) count("(statement batch)");
                        return invoke(st, sm, sargs);
                    });
                }
                default -> { }
            }
            return out;
        });
    }

    private interface Handler {
        Object handle(Object target, java.lang.reflect.Method m, Object[] args) throws Throwable;
    }

    private static <T> T proxy(Class<T> type, Object target, Handler handler) {
        InvocationHandler h = (p, m, args) -> handler.handle(target, m, args);
        return type.cast(Proxy.newProxyInstance(CountingDriver.class.getClassLoader(), new Class<?>[]{type}, h));
    }

    private static Object invoke(Object target, java.lang.reflect.Method m, Object[] args) throws Throwable {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    @Override public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override public int getMajorVersion() { return 1; }

    @Override public int getMinorVersion() { return 0; }

    @Override public boolean jdbcCompliant() { return false; }

    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
