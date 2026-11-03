package com.wiggle.boot.test;

import com.wiggle.boot.WiggleApplication;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.search.Search;
import com.wiggle.server.store.StorageFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * One node of the Boot application with exactly the {@link ServerConfig} a test built, and the
 * {@link StorageFactory} it names, for tests that set up a node themselves: a database of their own,
 * a storage topology, a restart on the same port. Its API is served on {@code config.port()}.
 */
public final class WiggleTestServer implements AutoCloseable {

    private final ConfigurableApplicationContext context;
    private final ServerConfig config;
    private final WiggleServer server;

    private WiggleTestServer(ConfigurableApplicationContext context, ServerConfig config) {
        this.context = context;
        this.config = config;
        this.server = context.getBean(WiggleServer.class);
    }

    /** A node on {@code config}, its store from the application's factory. */
    public static WiggleTestServer start(ServerConfig config) {
        return start(config, null);
    }

    /** A node on {@code config}, its store from {@code storage}, or the application's factory when null. */
    public static WiggleTestServer start(ServerConfig config, StorageFactory storage) {
        Map<String, Object> settings = new HashMap<>();
        settings.put("WIGGLE_PORT", config.port());
        settings.put("WIGGLE_DASHBOARD_PORT", config.dashboardPort());
        ConfigurableApplicationContext context = new SpringApplicationBuilder(WiggleApplication.class, WiggleTestNode.Config.class)
                .initializers(c -> {
                    TestNodeInitializer.apply((ConfigurableEnvironment) c.getEnvironment(), settings);
                    c.getBeanFactory().registerSingleton("serverConfig", config);
                    if (storage != null) c.getBeanFactory().registerSingleton("storageFactory", storage);
                })
                .run();
        return new WiggleTestServer(context, config);
    }

    /** The API's address on loopback. */
    public String baseUrl() { return "127.0.0.1:" + config.port(); }

    /** The port the API is served on. */
    public int port() { return config.port(); }

    public ServerConfig config() { return config; }

    /** The node as the application runs it. */
    public WiggleServer server() { return server; }

    public WiggleTestNode node() { return context.getBean(WiggleTestNode.class); }

    public WorkflowEngine engine() { return server.engine(); }

    public ClusterManager cluster() { return server.cluster(); }

    public Accounts accounts() { return server.accounts(); }

    public AuthCache authCache() { return server.authCache(); }

    public Optional<Search> search() { return server.search(); }

    /** Stops the node, as a shutdown would. */
    @Override public void close() {
        if (context.isActive()) context.close();
    }
}
