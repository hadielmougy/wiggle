package com.wiggle.boot.test;

import com.wiggle.boot.WiggleNode;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.auth.Accounts;
import com.wiggle.server.auth.AuthCache;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.search.Search;
import com.wiggle.server.store.StorageFactory;
import org.springframework.context.annotation.AnnotationConfigRegistry;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link WiggleNode} on the {@link ServerConfig} a test built, and the {@link StorageFactory} it names,
 * for tests that set up a node themselves: a database of their own, a storage topology, a restart on the
 * same port. It starts from {@link TestNodeInitializer}'s settings: no portal, and none of the process's
 * own {@code WIGGLE_*} variables.
 */
public final class WiggleTestServer implements AutoCloseable {

    private final WiggleNode node;

    private WiggleTestServer(WiggleNode node) {
        this.node = node;
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
        return new WiggleTestServer(WiggleNode.startWith(config, storage, context -> {
            TestNodeInitializer.apply((ConfigurableEnvironment) context.getEnvironment(), settings);
            if (context instanceof AnnotationConfigRegistry registry) registry.register(WiggleTestNode.Config.class);
        }));
    }

    /** The API's address on loopback. */
    public String baseUrl() { return node.baseUrl(); }

    /** The port the API is served on. */
    public int port() { return node.port(); }

    public ServerConfig config() { return node.config(); }

    /** The node as the application runs it. */
    public WiggleServer server() { return node.server(); }

    public WiggleTestNode node() { return node.context().getBean(WiggleTestNode.class); }

    public WorkflowEngine engine() { return node.engine(); }

    public ClusterManager cluster() { return node.cluster(); }

    public Accounts accounts() { return node.accounts(); }

    public AuthCache authCache() { return node.authCache(); }

    public Optional<Search> search() { return node.search(); }

    /** Stops the node, as a shutdown would. */
    @Override public void close() { node.close(); }
}
