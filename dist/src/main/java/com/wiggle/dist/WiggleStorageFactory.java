package com.wiggle.dist;

import com.wiggle.cassandra.CassandraStorage;
import com.wiggle.jdbc.Dialect;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.mysql.MySqlDialect;
import com.wiggle.oracle.OracleDialect;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.postgres.PostgresDialect;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.CoordinatorStoreProvider;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageFactory;
import com.wiggle.sqlserver.SqlServerDialect;

/**
 * The standalone server's storage selection: an explicit switch on the URL scheme, wired at compile
 * time. No {@code ServiceLoader}, no {@code META-INF/services} -- the mapping is right here and the
 * compiler checks it. Bundling this module (and only this module) into the image ships every
 * backend; {@code WIGGLE_JDBC_URL} picks one at runtime.
 */
public final class WiggleStorageFactory implements StorageFactory {

    @Override public Storage create(ServerConfig config) {
        String url = config.jdbcUrl();
        if (url == null || url.isBlank()) return new InMemoryStorage();
        if (url.startsWith("cassandra:")) {
            return CassandraStorage.fromUrl(url, config.jdbcUser(), config.jdbcPassword());
        }
        return new JdbcStorage(url, config.jdbcUser(), config.jdbcPassword(), config.jdbcPoolSize(), dialect(url));
    }

    /**
     * The coordinator store over {@code config}'s database: migrate the {@code coord_*} schema, then take
     * the store from the storage adapter's {@link CoordinatorStoreProvider} seam (or a non-durable
     * in-memory store for the in-memory backend). This is the only place the coordinator store is bound
     * to a concrete database — the engine's {@code Storage} interface stays coordinator-free.
     */
    public CoordinatorStore coordinatorStore(ServerConfig config, Storage storage) {
        // coordinatorStore() migrates the coord schema itself; in-memory storage has no provider.
        return storage instanceof CoordinatorStoreProvider p ? p.coordinatorStore() : new InMemoryCoordinatorStore();
    }

    private static Dialect dialect(String url) {
        if (url.startsWith("jdbc:postgresql:")) return new PostgresDialect();
        if (url.startsWith("jdbc:h2:")) return new H2Dialect();
        if (url.startsWith("jdbc:mysql:") || url.startsWith("jdbc:mariadb:")) return new MySqlDialect();
        if (url.startsWith("jdbc:oracle:")) return new OracleDialect();
        if (url.startsWith("jdbc:sqlserver:")) return new SqlServerDialect();
        throw new IllegalArgumentException("no storage backend for URL '" + url
                + "' -- expected jdbc:postgresql:, jdbc:h2:, jdbc:mysql:, jdbc:mariadb:, jdbc:oracle:, "
                + "jdbc:sqlserver: or cassandra://");
    }
}
