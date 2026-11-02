package com.wiggle.boot.test;

import com.wiggle.boot.WiggleApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Several nodes of the Boot application on one database, each its own application context: H2 in
 * memory, shared by the nodes and dropped when the last one stops, unless {@code WIGGLE_JDBC_URL} is
 * among the settings. Each node starts from {@link TestNodeInitializer}'s settings, with ports of its
 * own.
 */
public final class WiggleTestNodes implements AutoCloseable {

    private final Map<String, Object> settings;
    private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();

    private WiggleTestNodes(Map<String, Object> settings) {
        this.settings = settings;
    }

    /** {@code count} nodes, started in turn, each with {@code settings} on top of the test defaults. */
    public static WiggleTestNodes start(int count, Map<String, Object> settings) {
        Map<String, Object> shared = new HashMap<>(settings);
        shared.putIfAbsent("WIGGLE_JDBC_URL", "jdbc:h2:mem:nodes-" + UUID.randomUUID() + ";MODE=PostgreSQL");
        shared.putIfAbsent("WIGGLE_JDBC_USER", "sa");
        shared.putIfAbsent("WIGGLE_JDBC_PASSWORD", "");
        WiggleTestNodes nodes = new WiggleTestNodes(shared);
        try {
            for (int i = 0; i < count; i++) nodes.add();
        } catch (RuntimeException e) {
            nodes.close();
            throw e;
        }
        return nodes;
    }

    /** Starts one more node on the same database; its index is the number of nodes before it. */
    public WiggleTestNode add() {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(WiggleApplication.class, WiggleTestNode.Config.class)
                .initializers(c -> TestNodeInitializer.apply((ConfigurableEnvironment) c.getEnvironment(), settings))
                .run();
        contexts.add(context);
        return context.getBean(WiggleTestNode.class);
    }

    /** Node {@code i}; refused once it is stopped. */
    public WiggleTestNode node(int i) {
        ConfigurableApplicationContext context = contexts.get(i);
        if (context == null || !context.isActive()) throw new IllegalStateException("node " + i + " is stopped");
        return context.getBean(WiggleTestNode.class);
    }

    public int size() {
        return contexts.size();
    }

    /** Stops node {@code i}, as a crash or a shutdown would, and keeps the others running. */
    public void stop(int i) {
        ConfigurableApplicationContext context = contexts.get(i);
        if (context != null && context.isActive()) context.close();
    }

    @Override
    public void close() {
        RuntimeException first = null;
        for (ConfigurableApplicationContext c : contexts) {
            try {
                if (c.isActive()) c.close();
            } catch (RuntimeException e) {
                if (first == null) first = e;
                else first.addSuppressed(e);
            }
        }
        if (first != null) throw first;
    }
}
