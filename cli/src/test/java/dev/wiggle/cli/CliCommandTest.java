package dev.wiggle.cli;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Exercises the {@code wiggle} subcommands end to end: validate offline, register against a server. */
class CliCommandTest {

    private static final String YAML = """
            workflow: cli-order
            steps:
              - task: validate
              - gate: in-stock
              - fork:
                  payment:
                    - task: charge
                      queue: payments
                  shipping:
                    - task: ship
              - effect: notify
            """;

    private static int run(String... args) {
        return new CommandLine(new Wiggle()).execute(args);
    }

    @Test
    @DisplayName("validate returns 0 for a good file and 1 for a broken one")
    void validate(@TempDir Path dir) throws Exception {
        Path good = Files.writeString(dir.resolve("good.yaml"), YAML);
        Path bad = Files.writeString(dir.resolve("bad.yaml"), "workflow: x\nsteps:\n  - frobnicate: a\n");
        assertEquals(0, run("validate", good.toString()));
        assertEquals(1, run("validate", bad.toString()));
    }

    @Test
    @DisplayName("register compiles the file and registers it with the server")
    void register(@TempDir Path dir) throws Exception {
        Path file = Files.writeString(dir.resolve("cli-order.yaml"), YAML);
        ServerConfig config = new ServerConfig(0, "cli-test", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0, Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            assertEquals(0, run("register", file.toString(), "--server", server.baseUrl()));

            var def = client.getWorkflow("cli-order");   // 404s if it was not registered
            assertEquals("cli-order", def.name());
            assertEquals(1, forkCount(def), "the fork survived the round-trip");
        }
    }

    private static long forkCount(dev.wiggle.core.WorkflowDefinition def) {
        return def.nodes().values().stream().filter(n -> n.kind().name().equals("FORK")).count();
    }
}
