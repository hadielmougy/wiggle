package com.wiggle.dist;

import com.wiggle.jdbc.Dialect;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageFactory;

/**
 * The standalone server's storage selection: an explicit switch on the URL scheme, wired at compile
 * time. No {@code ServiceLoader}, no {@code META-INF/services} -- the mapping is right here and the
 * compiler checks it.
 *
 * <p>PostgreSQL is the supported backend. H2 (in PostgreSQL mode) is here for tests and local runs --
 * it exercises the JDBC store, its migrations and the generic claim path with nothing to install, but
 * it cannot run the {@code FOR UPDATE SKIP LOCKED} claim a real cluster depends on, so it is not a
 * deployment target. With no URL at all the server keeps its state in memory.
 */
public final class WiggleStorageFactory implements StorageFactory {

    @Override public Storage create(ServerConfig config) {
        String url = config.jdbcUrl();
        if (url == null || url.isBlank()) return new InMemoryStorage();
        return new JdbcStorage(url, config.jdbcUser(), config.jdbcPassword(), config.jdbcPoolSize(), dialect(url));
    }

    private static Dialect dialect(String url) {
        if (url.startsWith("jdbc:postgresql:")) return new PostgresDialect();
        if (url.startsWith("jdbc:h2:")) return new H2Dialect();       // tests and local runs only
        throw new IllegalArgumentException("no storage backend for URL '" + url
                + "' -- expected jdbc:postgresql: (or jdbc:h2: for tests)");
    }
}
