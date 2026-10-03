package com.wiggle.dist;

import com.wiggle.server.Logging;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.store.Storage;

/**
 * Entry point for the standalone server distribution. Reads configuration from the environment,
 * wires the all-backends {@link WiggleStorageFactory}, and runs until the JVM is stopped.
 */
public final class Main {

    private Main() { }

    public static void main(String[] args) throws Exception {
        Logging.configureFromEnv();   // opt-in file logging, before anything logs
        RemovedSettings.reject(System.getenv());

        // The ops console is a second role in the one image: a pure gRPC client + web UI, not a
        // server (no engine, no storage). It reads its own env (WIGGLE_URL).
        if (Role.fromEnvironment() == Role.CONSOLE) {
            com.wiggle.console.ConsoleMain.main(args);
            return;
        }

        ServerConfig config = ServerConfig.fromEnvironment();

        // One-shot schema migration then exit, for a CI/DBA-owned schema (run this, then run the app
        // with WIGGLE_SCHEMA_MODE=verify). Forces APPLY even if the app env pins verify.
        if (Boolean.parseBoolean(System.getenv().getOrDefault("WIGGLE_MIGRATE_ONLY", "false"))) {
            System.setProperty("wiggle.schema.forceApply", "true");
            try (Storage storage = new WiggleStorageFactory().create(config)) {
                storage.migrate();
            }
            System.out.println("Wiggle schema migrated ("
                    + storage(config) + "); exiting (WIGGLE_MIGRATE_ONLY).");
            return;
        }

        WiggleServer server = new WiggleServer(config, new WiggleStorageFactory()).start();
        boolean tls = config.tls().hasKeyStore();
        System.out.println("Wiggle server '" + config.nodeName() + "' on port " + server.port()
                + " (gRPC: " + (tls ? "TLS" : "plaintext")
                + ", storage: " + storage(config) + ")");
        String logFile = System.getenv("WIGGLE_LOG_FILE");
        if (logFile != null && !logFile.isBlank()) System.out.println("Logging to " + logFile);

        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }

    private static String storage(ServerConfig config) {
        if (config.topology() != null) return config.topology().shards().size() + " shards";
        return config.isInMemory() ? "in-memory" : config.jdbcUrl();
    }
}
