package com.wiggle.boot;

import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.store.Storage;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * A wiggle server node as a Spring Boot application, configured by the same {@code WIGGLE_*} settings.
 *
 * <p>A removed setting or role is refused before anything starts. With {@code WIGGLE_MIGRATE_ONLY=true}
 * the node applies the schema and exits, for a schema a CI job or a DBA owns.
 */
@SpringBootApplication(proxyBeanMethods = false)
public class WiggleApplication {

    public static void main(String[] args) {
        RemovedSettings.reject(System.getenv());
        Role.fromEnvironment();
        if (Boolean.parseBoolean(System.getenv().getOrDefault("WIGGLE_MIGRATE_ONLY", "false"))) {
            migrateOnly();
            return;
        }
        SpringApplication.run(WiggleApplication.class, args);
    }

    /** Applies the schema, even where the deployment pins {@code WIGGLE_SCHEMA_MODE=verify}, and returns. */
    private static void migrateOnly() {
        System.setProperty("wiggle.schema.forceApply", "true");
        ServerConfig config = ServerConfig.fromEnvironment();
        try (Storage storage = new PostgresStorageFactory().create(config)) {
            storage.migrate();
        }
        System.out.println("Wiggle schema migrated (" + StartupLog.storage(config) + "); exiting (WIGGLE_MIGRATE_ONLY).");
    }
}
