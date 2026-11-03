package com.wiggle.boot;

import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.search.Search;
import com.wiggle.server.store.StorageFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;

import java.util.Map;
import java.util.Optional;

/**
 * The Boot application run in this JVM on a {@link ServerConfig} built in code, for the examples and
 * tests in this repository: its API on {@code config.port()} (a free one when that is 0), its probe on
 * {@code config.dashboardPort()}, and no portal unless the settings or the environment ask for one. The
 * config, and the {@link StorageFactory} when one is given, replace the ones the application would build
 * from the environment.
 */
public final class WiggleNode implements AutoCloseable {

    private final ConfigurableApplicationContext context;
    private final ServerConfig config;
    private final WiggleServer server;
    private final int port;

    private WiggleNode(ConfigurableApplicationContext context, ServerConfig config) {
        this.context = context;
        this.config = config;
        this.server = context.getBean(WiggleServer.class);
        this.port = ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    /** A node on {@code config}, its store from {@code PostgresStorageFactory}. */
    public static WiggleNode start(ServerConfig config) {
        return start(config, null, Map.of());
    }

    /** A node on {@code config}, its store from {@code storage}. */
    public static WiggleNode start(ServerConfig config, StorageFactory storage) {
        return start(config, storage, Map.of());
    }

    /**
     * A node on {@code config}, its store from {@code storage} (the application's factory when null),
     * with {@code settings} (such as {@code WIGGLE_PORTAL_PORT}) over the environment's.
     */
    public static WiggleNode start(ServerConfig config, StorageFactory storage, Map<String, Object> settings) {
        return startWith(config, storage, context -> context.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("wiggle-node", settings)));
    }

    /**
     * A node on {@code config}, its store from {@code storage} (the application's factory when null),
     * with {@code initializer} run on the context before anything else (none when null).
     */
    public static WiggleNode startWith(ServerConfig config, StorageFactory storage,
                                       ApplicationContextInitializer<ConfigurableApplicationContext> initializer) {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(WiggleApplication.class)
                .properties(Map.of("WIGGLE_PORT", config.port(), "WIGGLE_PORTAL_PORT", 0,
                        "WIGGLE_DASHBOARD_PORT", config.dashboardPort()))
                .initializers(context -> {
                    if (initializer != null) initializer.initialize(context);
                    context.getBeanFactory().registerSingleton("serverConfig", config);
                    if (storage != null) context.getBeanFactory().registerSingleton("storageFactory", storage);
                });
        return new WiggleNode(builder.run(), config);
    }

    /** The API's address on loopback, for a client in this JVM. */
    public String baseUrl() { return "127.0.0.1:" + port; }

    /** The port the API is served on. */
    public int port() { return port; }

    public ServerConfig config() { return config; }

    /** The node as the application runs it. */
    public WiggleServer server() { return server; }

    public WorkflowEngine engine() { return server.engine(); }

    public ClusterManager cluster() { return server.cluster(); }

    public Accounts accounts() { return server.accounts(); }

    public AuthCache authCache() { return server.authCache(); }

    public Optional<Search> search() { return server.search(); }

    /** The application's context, for a bean the accessors above do not reach. */
    public ConfigurableApplicationContext context() { return context; }

    /** Stops the node, as a shutdown would. */
    @Override public void close() {
        if (context.isActive()) context.close();
    }
}
