package dev.wiggle.dist;

import dev.wiggle.server.Logging;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;

/**
 * Entry point for the standalone server distribution. Reads configuration from the environment,
 * wires the all-backends {@link WiggleStorageFactory}, and runs until the JVM is stopped.
 */
public final class Main {

    private Main() { }

    public static void main(String[] args) throws Exception {
        Logging.configureFromEnv();   // opt-in file logging, before anything logs
        ServerConfig config = ServerConfig.fromEnvironment();
        WiggleServer server = new WiggleServer(config, new WiggleStorageFactory()).start();
        boolean tls = config.tls().hasKeyStore();
        System.out.println("Wiggle server '" + config.nodeName() + "' on " + server.baseUrl()
                + " (gRPC: " + (tls ? "TLS" : "plaintext")
                + ", storage: " + (config.isInMemory() ? "in-memory" : config.jdbcUrl()) + ")");
        String logFile = System.getenv("WIGGLE_LOG_FILE");
        if (logFile != null && !logFile.isBlank()) System.out.println("Logging to " + logFile);
        if (server.dashboardPort() > 0) {
            System.out.println("Dashboard at " + (tls ? "https" : "http") + "://localhost:" + server.dashboardPort());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
