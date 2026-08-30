package com.wiggle.tests;

import com.wiggle.server.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ServerConfig#fromEnvironment()} -- what real deployments rely on. Environment variables
 * cannot be set from a test, but system properties take precedence over them by design, so the
 * property path exercises the same parsing and fallback chain.
 */
class ServerConfigTest {

    private static final List<String> PROPS = List.of(
            "wiggle.port", "wiggle.node.name", "wiggle.jdbc.url", "wiggle.jdbc.poolSize",
            "wiggle.lease.millis", "wiggle.dashboard.port", "wiggle.queueLag.warnThresholdMillis");

    @AfterEach
    void clearProperties() {
        PROPS.forEach(System::clearProperty);
    }

    @Test @DisplayName("defaults apply when nothing is configured")
    void defaults() {
        ServerConfig c = ServerConfig.fromEnvironment();
        assertEquals(8080, c.port());
        assertNull(c.jdbcUrl());
        assertTrue(c.isInMemory(), "no JDBC URL means the in-memory store");
        assertEquals(10, c.jdbcPoolSize());
        assertEquals(Duration.ofSeconds(30), c.defaultLease());
        assertEquals(Duration.ofSeconds(20), c.maxLongPoll());
        assertEquals(0, c.dashboardPort(), "dashboard is off by default");
        assertEquals(Duration.ofSeconds(10), c.queueLagWarnThreshold());
    }

    @Test @DisplayName("system properties override the defaults")
    void propertiesOverride() {
        System.setProperty("wiggle.port", "19099");
        System.setProperty("wiggle.node.name", "cfg-test-node");
        System.setProperty("wiggle.jdbc.url", "jdbc:h2:mem:cfg");
        System.setProperty("wiggle.jdbc.poolSize", "3");
        System.setProperty("wiggle.lease.millis", "1234");
        System.setProperty("wiggle.dashboard.port", "18081");
        System.setProperty("wiggle.queueLag.warnThresholdMillis", "555");

        ServerConfig c = ServerConfig.fromEnvironment();
        assertEquals(19099, c.port());
        assertEquals("cfg-test-node", c.nodeName());
        assertEquals("jdbc:h2:mem:cfg", c.jdbcUrl());
        assertFalse(c.isInMemory(), "a JDBC URL switches off the in-memory store");
        assertEquals(3, c.jdbcPoolSize());
        assertEquals(Duration.ofMillis(1234), c.defaultLease());
        assertEquals(18081, c.dashboardPort());
        assertEquals(Duration.ofMillis(555), c.queueLagWarnThreshold());
    }

    @Test @DisplayName("a blank property falls back to the default")
    void blankFallsBack() {
        System.setProperty("wiggle.jdbc.url", "   ");
        ServerConfig c = ServerConfig.fromEnvironment();
        assertNull(c.jdbcUrl());
        assertTrue(c.isInMemory());
    }
}
