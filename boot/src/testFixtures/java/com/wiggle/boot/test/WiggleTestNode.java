package com.wiggle.boot.test;

import com.wiggle.client.WiggleClient;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/** One running node of the Boot application, as a test reaches it. */
public final class WiggleTestNode {

    private final WiggleServer server;
    private final ServerConfig config;
    private final Environment environment;

    WiggleTestNode(WiggleServer server, ServerConfig config, Environment environment) {
        this.server = server;
        this.config = config;
        this.environment = environment;
    }

    /** The API's address on loopback, for a {@link WiggleClient}. */
    public String baseUrl() {
        return "127.0.0.1:" + config.port();
    }

    /** A new client of this node; the caller closes it. */
    public WiggleClient client() {
        return new WiggleClient(baseUrl());
    }

    public WiggleServer server() {
        return server;
    }

    public ServerConfig config() {
        return config;
    }

    /** The portal's port, or 0 when the node serves none. */
    public int portalPort() {
        return environment.getProperty("WIGGLE_PORTAL_PORT", Integer.class, 0);
    }

    /** The probe's port, or 0 when the node serves none. */
    public int probePort() {
        return environment.getProperty("WIGGLE_DASHBOARD_PORT", Integer.class, 0);
    }

    /** The bean, imported by {@link WiggleBootTest} and every node of {@link WiggleTestNodes}. */
    @Configuration(proxyBeanMethods = false)
    public static class Config {
        @Bean
        WiggleTestNode wiggleTestNode(WiggleServer server, ServerConfig config, Environment environment) {
            return new WiggleTestNode(server, config, environment);
        }
    }
}
