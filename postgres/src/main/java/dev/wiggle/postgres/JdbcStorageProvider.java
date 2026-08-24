package dev.wiggle.postgres;

import dev.wiggle.jdbc.H2Dialect;
import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.jdbc.PostgresDialect;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageProvider;

/**
 * Contributes the JDBC-backed store to the server via {@link java.util.ServiceLoader}, for
 * PostgreSQL (production) and H2 in PostgreSQL-compatibility mode (development and tests). The
 * concrete database is detected from the JDBC URL and drives the {@link dev.wiggle.jdbc.Dialect}
 * the store runs against. MySQL and Oracle are supplied by the {@code wiggle-mysql} and
 * {@code wiggle-oracle} modules.
 */
public final class JdbcStorageProvider implements StorageProvider {

    @Override public boolean supports(String jdbcUrl) {
        return jdbcUrl != null
                && (jdbcUrl.startsWith("jdbc:postgresql:") || jdbcUrl.startsWith("jdbc:h2:"));
    }

    @Override public Storage create(ServerConfig config) {
        var dialect = config.jdbcUrl().startsWith("jdbc:h2:") ? new H2Dialect() : new PostgresDialect();
        return new JdbcStorage(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(),
                config.jdbcPoolSize(), dialect);
    }
}
