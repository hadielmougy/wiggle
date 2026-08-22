package dev.wiggle.postgres;

import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageProvider;

/**
 * Contributes the JDBC-backed store to the server via {@link java.util.ServiceLoader}. Supports
 * PostgreSQL (production) and H2 in PostgreSQL-compatibility mode (development and tests).
 */
public final class JdbcStorageProvider implements StorageProvider {

    @Override public boolean supports(String jdbcUrl) {
        return jdbcUrl != null
                && (jdbcUrl.startsWith("jdbc:postgresql:") || jdbcUrl.startsWith("jdbc:h2:"));
    }

    @Override public Storage create(ServerConfig config) {
        return new JdbcStorage(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(), config.jdbcPoolSize());
    }
}
