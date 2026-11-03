package com.wiggle.boot;

import com.wiggle.console.DashboardServlet;
import com.wiggle.console.Portal;
import com.wiggle.core.Settings;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import jakarta.servlet.Filter;
import org.apache.catalina.connector.Connector;
import org.apache.coyote.http2.Http2Protocol;
import org.apache.tomcat.util.threads.VirtualThreadExecutor;
import org.springframework.boot.tomcat.servlet.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.type.AnnotatedTypeMetadata;

/** The portal on its own port ({@code WIGGLE_PORTAL_PORT}, 8070 by default, 0 for none), a second connector of the API's Tomcat. */
@Configuration(proxyBeanMethods = false)
@Conditional(PortalConfiguration.Enabled.class)
class PortalConfiguration {

    /** Whether the settings ask for a portal. */
    static final class Enabled implements Condition {
        @Override public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return Portal.port(NodeConfiguration.settings(context.getEnvironment()).byEnvName()) > 0;
        }
    }

    @Bean
    PortalPort portalPort(Settings settings, ServerConfig config) {
        int port = Portal.port(settings.byEnvName());
        if (port == config.port()) {
            throw new IllegalStateException("WIGGLE_PORTAL_PORT and WIGGLE_PORT are both " + port
                    + "; the portal needs a port of its own");
        }
        return new PortalPort(port);
    }

    /** The port the portal is served on. */
    record PortalPort(int value) {}

    @Bean
    Portal.Web portalWeb(WiggleServer server, ServerConfig config, Settings settings, PortalPort port) {
        Portal.Web web = Portal.web(server, config.tls().hasKeyStore(), settings.byEnvName());
        web.warnIfSetupRequired(config.tls().hasKeyStore(), port.value());
        return web;
    }

    @Bean
    WebServerFactoryCustomizer<TomcatServletWebServerFactory> portalConnector(PortalPort port, ServerConfig config) {
        return factory -> {
            Connector connector = new Connector();
            connector.setPort(port.value());
            if (config.tls().hasKeyStore()) TomcatTls.configure(connector, config.tls());
            connector.addUpgradeProtocol(new Http2Protocol());
            connector.getProtocolHandler().setExecutor(new VirtualThreadExecutor("portal-"));
            factory.addAdditionalConnectors(connector);
        };
    }

    @Bean
    ServletRegistrationBean<DashboardServlet> portalServlet(Portal.Web web) {
        return new ServletRegistrationBean<>(web.servlet(), "/*");
    }

    @Bean
    FilterRegistrationBean<PortScopeFilter> portScope(ServerConfig config, PortalPort port) {
        FilterRegistrationBean<PortScopeFilter> bean =
                new FilterRegistrationBean<>(new PortScopeFilter(config.port(), port.value()));
        bean.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return bean;
    }

    /** The portal's sign-in, on the portal's port only. */
    @Bean
    FilterRegistrationBean<Filter> portalAuth(Portal.Web web, PortalPort port) {
        Filter auth = web.filter();
        FilterRegistrationBean<Filter> bean = new FilterRegistrationBean<>((req, resp, chain) -> {
            if (req.getLocalPort() == port.value()) auth.doFilter(req, resp, chain);
            else chain.doFilter(req, resp);
        });
        bean.setOrder(Ordered.HIGHEST_PRECEDENCE + 1);
        return bean;
    }
}
