package dev.wiggle.oracle;

import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageProvider;

/**
 * Contributes the Oracle Database store via {@link java.util.ServiceLoader}. Add this module (and
 * the ojdbc driver it bundles) to the classpath and point {@code WIGGLE_JDBC_URL} at a
 * {@code jdbc:oracle:} URL; the server detects it from the URL and clusters on it exactly as it
 * does on PostgreSQL.
 */
public final class OracleStorageProvider implements StorageProvider {

    @Override public boolean supports(String jdbcUrl) {
        return jdbcUrl != null && jdbcUrl.startsWith("jdbc:oracle:");
    }

    @Override public Storage create(ServerConfig config) {
        return new JdbcStorage(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(),
                config.jdbcPoolSize(), new OracleDialect());
    }
}
