package dev.wiggle.tests;

import dev.wiggle.mysql.MySqlStorageProvider;
import dev.wiggle.oracle.OracleStorageProvider;
import dev.wiggle.postgres.JdbcStorageProvider;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The pluggable-storage SPI: which URLs each database module claims, and the no-provider failure. */
class StorageProviderTest {

    @Test @DisplayName("each provider claims its own URLs and nothing else")
    void providerSupports() {
        JdbcStorageProvider pg = new JdbcStorageProvider();
        assertTrue(pg.supports("jdbc:postgresql://localhost:5432/wiggle"));
        assertTrue(pg.supports("jdbc:h2:mem:test;MODE=PostgreSQL"));
        assertFalse(pg.supports("jdbc:mysql://localhost/wiggle"));
        assertFalse(pg.supports("jdbc:oracle:thin:@localhost:1521/wiggle"));
        assertFalse(pg.supports("bogus"));
        assertFalse(pg.supports(null));

        MySqlStorageProvider mysql = new MySqlStorageProvider();
        assertTrue(mysql.supports("jdbc:mysql://localhost:3306/wiggle"));
        assertTrue(mysql.supports("jdbc:mariadb://localhost:3306/wiggle"));
        assertFalse(mysql.supports("jdbc:postgresql://localhost/wiggle"));
        assertFalse(mysql.supports(null));

        OracleStorageProvider oracle = new OracleStorageProvider();
        assertTrue(oracle.supports("jdbc:oracle:thin:@localhost:1521/wiggle"));
        assertFalse(oracle.supports("jdbc:mysql://localhost/wiggle"));
        assertFalse(oracle.supports(null));
    }

    @Test @DisplayName("a JDBC URL no provider claims fails fast with a clear message")
    void noProviderFailsFast() {
        ServerConfig config = new ServerConfig(0, "sp-node", "jdbc:sqlserver://localhost/nope", null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> new WiggleServer(config));
        assertTrue(e.getMessage().contains("no storage provider"), e.getMessage());
        assertTrue(e.getMessage().contains("jdbc:sqlserver://localhost/nope"), "names the offending URL");
    }
}
