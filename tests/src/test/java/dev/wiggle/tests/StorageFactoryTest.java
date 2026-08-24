package dev.wiggle.tests;

import dev.wiggle.dist.WiggleStorageFactory;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Storage selection is now an explicit factory (no ServiceLoader): {@link WiggleStorageFactory}
 * maps the URL scheme to a store, and the server's default (single-arg) construction is in-memory
 * only. This covers the pure-logic branches -- the ones that don't need a live database.
 */
class StorageFactoryTest {

    private static ServerConfig config(String url) {
        return new ServerConfig(0, "sf-node", url, url == null ? null : "sa", url == null ? null : "", 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("no URL yields the in-memory store")
    void inMemory() {
        try (Storage s = new WiggleStorageFactory().create(config(null))) {
            assertInstanceOf(InMemoryStorage.class, s);
        }
    }

    @Test @DisplayName("a jdbc:h2 URL yields a working JDBC store (the H2 dialect branch, end to end)")
    void h2Jdbc() {
        String url = "jdbc:h2:mem:sf-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (Storage s = new WiggleStorageFactory().create(config(url))) {
            assertTrue(s.getClass().getName().contains("JdbcStorage"), s.getClass().getName());
            s.migrate();   // proves the dialect was selected correctly and the store is usable
        }
    }

    @Test @DisplayName("an unrecognised URL scheme fails fast with a helpful message")
    void unknownScheme() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new WiggleStorageFactory().create(config("jdbc:db2://localhost/nope")));
        assertTrue(e.getMessage().contains("jdbc:db2://localhost/nope"), e.getMessage());
        assertTrue(e.getMessage().contains("no storage backend"), e.getMessage());
    }

    @Test @DisplayName("the default in-memory-only server rejects a storage URL with a clear message")
    void defaultFactoryRejectsUrl() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new WiggleServer(config("jdbc:postgresql://localhost/nope")));
        assertTrue(e.getMessage().contains("no StorageFactory"), e.getMessage());
        assertTrue(e.getMessage().contains("jdbc:postgresql://localhost/nope"), "names the offending URL");
    }
}
