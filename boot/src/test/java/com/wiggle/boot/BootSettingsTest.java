package com.wiggle.boot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@code WIGGLE_LOG_LEVEL} takes {@link System.Logger.Level} names; Boot gets its own. */
class BootSettingsTest {

    @Test @DisplayName("System.Logger level names map to Boot's, unset stays unset, and anything else is refused")
    void logLevels() {
        assertEquals("WARN", BootSettings.logLevel("WARNING"));
        assertEquals("TRACE", BootSettings.logLevel("all"));
        assertEquals("DEBUG", BootSettings.logLevel(" debug "));
        assertNull(BootSettings.logLevel(null));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> BootSettings.logLevel("loud"));
        assertTrue(e.getMessage().contains("WIGGLE_LOG_LEVEL='loud'"), e.getMessage());
    }
}
