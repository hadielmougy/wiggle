package dev.wiggle.dist;

import dev.wiggle.cassandra.CassandraStorage;
import dev.wiggle.jdbc.Dialect;
import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.mysql.MySqlDialect;
import dev.wiggle.oracle.OracleDialect;
import dev.wiggle.postgres.H2Dialect;
import dev.wiggle.postgres.PostgresDialect;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageFactory;
import dev.wiggle.sqlserver.SqlServerDialect;

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
