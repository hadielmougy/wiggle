package dev.wiggle.server.store;

import dev.wiggle.server.ServerConfig;

/**
 * Pluggable backend for a database-backed {@link Storage}. Each database lives in its own
 * module (e.g. {@code wiggle-postgres}) and contributes a provider via {@link java.util.ServiceLoader}
 * (a {@code META-INF/services/dev.wiggle.server.store.StorageProvider} entry). The server core
 * depends on none of them: with no JDBC URL configured it uses the in-memory store, and with
 * one it picks the first provider that {@link #supports} the URL.
 */
public interface StorageProvider {

    /** Whether this provider can back the given JDBC URL (e.g. {@code jdbc:postgresql:...}). */
    boolean supports(String jdbcUrl);

    /** Creates (but does not migrate) a storage instance for the configuration. */
    Storage create(ServerConfig config);
}
