package com.wiggle.tests;

import com.wiggle.server.Logging;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Handler;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** File logging via java.util.logging: a Wiggle DEBUG line actually reaches the file. */
class LoggingTest {

    @Test @DisplayName("attaches a file handler and writes Wiggle's log lines to it")
    void writesToFile(@TempDir Path dir) throws IOException {
        // %g is substituted by the rotating handler; generation 0 is the live file.
        String pattern = dir.resolve("wiggle-%g.log").toString();
        Handler handler = Logging.configure(pattern, "DEBUG");
        try {
            System.getLogger("com.wiggle.probe").log(System.Logger.Level.DEBUG, "hello-from-debug");
            System.getLogger("com.wiggle.probe").log(System.Logger.Level.INFO, "hello-from-info");
            handler.flush();

            String contents = Files.readString(dir.resolve("wiggle-0.log"));
            assertTrue(contents.contains("hello-from-debug"), "DEBUG captured at level DEBUG");
            assertTrue(contents.contains("hello-from-info"), "INFO captured too");
        } finally {
            Logger.getLogger("").removeHandler(handler);
            handler.close();
        }
    }

    @Test @DisplayName("no file configured is a clean no-op")
    void noFileIsNoOp() {
        assertNull(Logging.configure(null, "INFO"), "null path -> no handler");
        assertNull(Logging.configure("  ", "INFO"), "blank path -> no handler");
    }
}
