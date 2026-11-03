package com.wiggle.server.store;

import com.wiggle.server.ServerConfig;

/**
 * Builds the {@link Storage} a server node runs on from its configuration. The server core is
 * storage-agnostic: it does not know about any database module, so a caller injects a factory that
 * does. {@code PostgresStorageFactory} maps the JDBC URL scheme to a concrete store; a test can
 * supply its own (or a lambda that always returns a particular store).
 *
 * <p>This replaces the old {@code ServiceLoader}-based {@code StorageProvider} SPI: selection is now
 * an explicit, compile-checked switch rather than classpath discovery.
 */
@FunctionalInterface
public interface StorageFactory {

    /** Creates (but does not migrate) the store for this configuration. */
    Storage create(ServerConfig config);
}
