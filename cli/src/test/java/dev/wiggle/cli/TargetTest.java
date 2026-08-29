package dev.wiggle.cli;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The switchable CLI target: `wiggle use`, persistence, and the flag > env > saved > default order. */
class TargetTest {

    @BeforeEach void isolate(@TempDir Path dir) {
        System.setProperty("wiggle.config.home", dir.toString());
    }

    @AfterEach void reset() {
        System.clearProperty("wiggle.config.home");
        Target.clear();
    }

    private static int run(String... args) {
        return new CommandLine(new Wiggle()).execute(args);
    }

    @Test @DisplayName("`wiggle use` saves a target that later loads back")
    void saveAndLoad() {
        assertEquals(0, run("use", "coordinator", "10.0.0.1:8099"));
        Target t = Target.load().orElseThrow();
        assertEquals(Target.Kind.COORDINATOR, t.kind());
        assertEquals("10.0.0.1:8099", t.address());
    }

    @Test @DisplayName("a flag overrides the saved target; otherwise the saved target is used")
    void resolutionOrder() {
        new Target(Target.Kind.COORDINATOR, "saved:8099").save();
        assertEquals("saved:8099", Target.resolve(Target.Kind.COORDINATOR, null), "saved is used");
        assertEquals("flag:9000", Target.resolve(Target.Kind.COORDINATOR, "flag:9000"), "flag wins");
    }

    @Test @DisplayName("a command needing a coordinator rejects a saved cell target with a clear message")
    void wrongKind() {
        new Target(Target.Kind.CELL, "cell:8081").save();
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> Target.resolve(Target.Kind.COORDINATOR, null));
        assertTrue(e.getMessage().contains("needs a COORDINATOR"), e.getMessage());
    }

    @Test @DisplayName("with nothing saved, resolution falls back to the kind default")
    void defaults() {
        assertEquals("127.0.0.1:8099", Target.resolve(Target.Kind.COORDINATOR, null));
        assertEquals("localhost:8080", Target.resolve(Target.Kind.CELL, null));
    }

    @Test @DisplayName("`use --clear` removes the saved target")
    void clear() {
        new Target(Target.Kind.CELL, "cell:8081").save();
        assertEquals(0, run("use", "--clear"));
        assertTrue(Target.load().isEmpty());
    }
}
