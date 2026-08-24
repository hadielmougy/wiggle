package dev.wiggle.sqlserver;

import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageProvider;

/**
 * Contributes the Microsoft SQL Server store via {@link java.util.ServiceLoader}. Add this module
 * (and the mssql-jdbc driver it bundles) to the classpath and point {@code WIGGLE_JDBC_URL} at a
 * {@code jdbc:sqlserver:} URL; the server detects it from the URL and clusters on it exactly as it
 * does on PostgreSQL.
 */
public final class SqlServerStorageProvider implements StorageProvider {

    @Override public boolean supports(String jdbcUrl) {
        return jdbcUrl != null && jdbcUrl.startsWith("jdbc:sqlserver:");
    }

    @Override public Storage create(ServerConfig config) {
        return new JdbcStorage(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(),
                config.jdbcPoolSize(), new SqlServerDialect());
    }
}
