package dev.wiggle.cassandra;

import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.store.Storage;
import dev.wiggle.server.store.StorageProvider;

/**
 * Contributes the Cassandra store via {@link java.util.ServiceLoader}. Unlike the SQL modules this
 * is not a JDBC dialect -- Cassandra has no cross-partition transactions or {@code SELECT ... FOR
 * UPDATE}, so it implements the {@link Storage} SPI directly on the CQL driver, using
 * partition-local lightweight transactions (Paxos) for the same guarantees.
 *
 * <p>The {@code WIGGLE_JDBC_URL} is reused as the contact-point URL, in the form
 * {@code cassandra://host1[:port][,host2...]/keyspace?dc=<localDatacenter>&rf=<replicationFactor>}.
 * {@code WIGGLE_JDBC_USER}/{@code WIGGLE_JDBC_PASSWORD} become the CQL credentials.
 */
public final class CassandraStorageProvider implements StorageProvider {

    @Override public boolean supports(String url) {
        return url != null && url.startsWith("cassandra:");
    }

    @Override public Storage create(ServerConfig config) {
        return CassandraStorage.fromUrl(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword());
    }
}
