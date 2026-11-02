package com.wiggle.boot;

import com.wiggle.core.Settings;
import com.wiggle.core.wire.ControlPlane;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.api.ApiServlet;
import com.wiggle.server.store.StorageFactory;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.io.IOException;

/** The node: its settings, its store and its parts, and the API servlet on the web server's port. */
@Configuration(proxyBeanMethods = false)
class NodeConfiguration {

    /** Each setting from Spring's environment, by its {@code wiggle.*} property name or its {@code WIGGLE_*} one. */
    @Bean
    static Settings settings(Environment environment) {
        return (property, env) -> {
            String v = property == null ? null : environment.getProperty(property);
            return v != null ? v : env == null ? null : environment.getProperty(env);
        };
    }

    @Bean
    ServerConfig serverConfig(Settings settings) {
        return ServerConfig.from(settings);
    }

    @Bean
    StorageFactory storageFactory() {
        return new PostgresStorageFactory();
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    WiggleServer wiggleServer(ServerConfig config, StorageFactory storageFactory) throws IOException {
        return WiggleServer.withoutHttpServers(config, storageFactory);
    }

    @Bean
    ServletRegistrationBean<ApiServlet> apiServlet(WiggleServer server) {
        return new ServletRegistrationBean<>(server.apiServlet(), ControlPlane.PREFIX + "*");
    }

    /** The web server listens on {@code WIGGLE_PORT}. */
    @Bean
    WebServerFactoryCustomizer<TomcatServletWebServerFactory> apiPort(ServerConfig config) {
        return factory -> factory.setPort(config.port());
    }
}
