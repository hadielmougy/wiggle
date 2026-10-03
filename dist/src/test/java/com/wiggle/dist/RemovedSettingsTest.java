package com.wiggle.dist;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A coordinator setting left in a deployment stops the process instead of being ignored. */
class RemovedSettingsTest {

    @Test @DisplayName("a removed coordinator setting is refused, naming the variable")
    void removedSettingIsRefused() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> RemovedSettings.reject(Map.of("WIGGLE_COORDINATOR_URL", "coord:9090",
                        "WIGGLE_CELL_ID", "a")));
        assertTrue(e.getMessage().contains("WIGGLE_COORDINATOR_URL"), e.getMessage());
        assertTrue(e.getMessage().contains("WIGGLE_CELL_ID"), e.getMessage());
    }

    @Test @DisplayName("a blank removed setting and unrelated settings start normally")
    void blankOrUnrelatedIsFine() {
        assertDoesNotThrow(() -> RemovedSettings.reject(Map.of("WIGGLE_NAMESPACE", " ",
                "WIGGLE_JDBC_URL", "jdbc:postgresql://db/wiggle")));
    }
}
