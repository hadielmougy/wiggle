package dev.wiggle.tests;

import dev.wiggle.postgres.JdbcStorageProvider;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The pluggable-storage SPI: which URLs the Postgres module claims, and the no-provider failure. */
class StorageProviderTest {

    @Test @DisplayName("the Postgres provider claims postgresql and h2 URLs, nothing else")
    void providerSupports() {
        JdbcStorageProvider provider = new JdbcStorageProvider();
        assertTrue(provider.supports("jdbc:postgresql://localhost:5432/wiggle"));
        assertTrue(provider.supports("jdbc:h2:mem:test;MODE=PostgreSQL"));
        assertFalse(provider.supports("jdbc:mysql://localhost/wiggle"));
        assertFalse(provider.supports("bogus"));
        assertFalse(provider.supports(null));
    }

    @Test @DisplayName("a JDBC URL no provider claims fails fast with a clear message")
    void noProviderFailsFast() {
        ServerConfig config = new ServerConfig(0, "sp-node", "jdbc:mysql://localhost/nope", null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new WiggleServer(config));
        assertTrue(e.getMessage().contains("no storage provider"), e.getMessage());
        assertTrue(e.getMessage().contains("jdbc:mysql://localhost/nope"), "names the offending URL");
    }
}
