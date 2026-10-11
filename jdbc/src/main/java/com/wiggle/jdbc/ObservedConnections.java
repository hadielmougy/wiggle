package com.wiggle.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

/** Connections and statements that tell {@link StatementListener}s of each execution and commit. */
final class ObservedConnections {

    private ObservedConnections() {}

    @FunctionalInterface
    private interface Handler {
        Object handle(Method m, Object[] args) throws Throwable;
    }

    /** {@code real}, reporting to whatever {@code listeners} holds at each event. */
    static Connection wrap(Connection real, List<StatementListener> listeners) {
        return proxy(Connection.class, (m, args) -> {
            Object out = invoke(real, m, args);
            switch (m.getName()) {
                case "commit" -> listeners.forEach(StatementListener::committed);
                case "rollback" -> { if (args == null) listeners.forEach(StatementListener::rolledBack); }
                case "prepareStatement" -> {
                    return prepared(PreparedStatement.class, (PreparedStatement) out, (String) args[0], listeners);
                }
                case "prepareCall" -> {
                    return prepared(CallableStatement.class, (CallableStatement) out, (String) args[0], listeners);
                }
                case "createStatement" -> {
                    return plain((Statement) out, listeners);
                }
                default -> { }
            }
            return out;
        });
    }

    private static <S extends PreparedStatement> S prepared(Class<S> type, S real, String sql,
                                                           List<StatementListener> listeners) {
        return proxy(type, (m, args) -> {
            if (m.getName().startsWith("execute")) listeners.forEach(l -> l.executed(sql));
            return invoke(real, m, args);
        });
    }

    private static Statement plain(Statement real, List<StatementListener> listeners) {
        List<String> batch = new java.util.ArrayList<>();
        return proxy(Statement.class, (m, args) -> {
            switch (m.getName()) {
                case "addBatch" -> batch.add((String) args[0]);
                case "clearBatch" -> batch.clear();
                case "executeBatch", "executeLargeBatch" -> {
                    String sql = batch.isEmpty() ? "(batch)" : batch.get(0);
                    batch.clear();
                    listeners.forEach(l -> l.executed(sql));
                }
                default -> {
                    if (m.getName().startsWith("execute") && args != null && args.length > 0
                            && args[0] instanceof String sql) {
                        listeners.forEach(l -> l.executed(sql));
                    }
                }
            }
            return invoke(real, m, args);
        });
    }

    private static <T> T proxy(Class<T> type, Handler handler) {
        return type.cast(Proxy.newProxyInstance(ObservedConnections.class.getClassLoader(),
                new Class<?>[]{type}, (p, m, args) -> handler.handle(m, args)));
    }

    private static Object invoke(Object target, Method m, Object[] args) throws Throwable {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
