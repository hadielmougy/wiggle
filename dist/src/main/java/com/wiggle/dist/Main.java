package com.wiggle.dist;

import com.wiggle.console.Portal;
import com.wiggle.server.Logging;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.search.Embedders;
import com.wiggle.server.store.Storage;

import java.util.Optional;

/**
 * Entry point for the standalone server distribution. Reads configuration from the environment,
 * wires the all-backends {@link WiggleStorageFactory}, and runs until the JVM is stopped.
 */
public final class Main {

    private Main() { }

    public static void main(String[] args) throws Exception {
        Logging.configureFromEnv();   // opt-in file logging, before anything logs
        RemovedSettings.reject(System.getenv());

        Role.fromEnvironment();

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

        WiggleServer server = new WiggleServer(config, new WiggleStorageFactory(),
                Embedders.fromEnvironment(System.getenv())).start();
        boolean tls = config.tls().hasKeyStore();
        System.out.println("Wiggle server '" + config.nodeName() + "' on port " + server.port()
                + " (gRPC: " + (tls ? "TLS" : "plaintext")
                + ", storage: " + storage(config) + ")");
        Optional<Portal> portal = Portal.fromEnvironment(server, config.tls(), System.getenv());
        portal.ifPresent(p -> System.out.println("Portal on " + (tls ? "https" : "http") + "://localhost:" + p.port()));
        String logFile = System.getenv("WIGGLE_LOG_FILE");
        if (logFile != null && !logFile.isBlank()) System.out.println("Logging to " + logFile);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            portal.ifPresent(Portal::close);
            server.close();
        }));
        Thread.currentThread().join();
    }

    private static String storage(ServerConfig config) {
        if (config.topology() != null) return config.topology().shards().size() + " shards";
        return config.isInMemory() ? "in-memory" : config.jdbcUrl();
    }
}
