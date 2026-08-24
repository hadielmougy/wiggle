package dev.wiggle.mysql;

import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageProvider;

/**
 * Contributes the MySQL/MariaDB store via {@link java.util.ServiceLoader}. Add this module (and the
 * MySQL driver it bundles) to the classpath and point {@code WIGGLE_JDBC_URL} at a
 * {@code jdbc:mysql:} or {@code jdbc:mariadb:} URL; the server detects it from the URL and clusters
 * on it exactly as it does on PostgreSQL.
 */
public final class MySqlStorageProvider implements StorageProvider {

    @Override public boolean supports(String jdbcUrl) {
        return jdbcUrl != null
                && (jdbcUrl.startsWith("jdbc:mysql:") || jdbcUrl.startsWith("jdbc:mariadb:"));
    }

    @Override public Storage create(ServerConfig config) {
        return new JdbcStorage(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(),
                config.jdbcPoolSize(), new MySqlDialect());
    }
}
