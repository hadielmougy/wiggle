package com.wiggle.tests;

import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Storage selection is an explicit factory (no ServiceLoader): {@link PostgresStorageFactory} maps
 * the URL scheme to a store, and the server's default (single-arg) construction is in-memory only.
 * This covers the pure-logic branches -- the ones that don't need a live database.
 */
class StorageFactoryTest {

    private static ServerConfig config(String url) {
        return new ServerConfig(TestPorts.free(), "sf-node", url, url == null ? null : "sa", url == null ? null : "", 4,
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

    @Test @DisplayName("the published factory is the same mapping the distribution runs")
    void theDistributionDelegatesRatherThanDuplicating() {
        // The mapping lives in the published postgres module so an embedding app can have it;
        // dist keeps the name its image and docs already use. If someone re-implements one of
        // them, an app and the image would select storage differently for the same URL.
        assertTrue(PostgresStorageFactory.class.isAssignableFrom(WiggleStorageFactory.class),
                "WiggleStorageFactory must remain PostgresStorageFactory under another name");

        String url = "jdbc:h2:mem:sf-rel-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (Storage a = new PostgresStorageFactory().create(config(url));
             Storage b = new WiggleStorageFactory().create(config(url))) {
            assertEquals(a.getClass(), b.getClass(), "same URL, same store type");
        }
    }

    @Test @DisplayName("an unknown scheme names what is supported, from the published class too")
    void unknownSchemeFromThePublishedClass() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> new PostgresStorageFactory().create(config("jdbc:oracle:thin:@//h:1521/x")));
        assertTrue(e.getMessage().contains("jdbc:postgresql:"), e.getMessage());
    }
}
