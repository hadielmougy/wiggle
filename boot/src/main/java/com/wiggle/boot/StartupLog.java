package com.wiggle.boot;

import com.wiggle.core.Settings;
import com.wiggle.console.Portal;
import com.wiggle.server.ServerConfig;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/** Says where the node serves, once it is ready. */
@Component
class StartupLog {

    private static final System.Logger LOG = System.getLogger(StartupLog.class.getName());

    private final ServerConfig config;
    private final Settings settings;

    StartupLog(ServerConfig config, Settings settings) {
        this.config = config;
        this.settings = settings;
    }

    @EventListener
    void ready(ApplicationReadyEvent event) {
        boolean tls = config.tls().hasKeyStore();
        int portal = Portal.port(settings.byEnvName());
        int port = event.getApplicationContext() instanceof WebServerApplicationContext web
                ? web.getWebServer().getPort() : config.port();
        LOG.log(System.Logger.Level.INFO, () -> "Wiggle server '" + config.nodeName() + "' on port " + port
                + " (API: " + (tls ? "HTTPS" : "plaintext HTTP") + ", storage: " + storage(config) + ")"
                + (portal > 0 ? "; portal on " + (tls ? "https" : "http") + "://localhost:" + portal : ""));
    }

    /** The store {@code config} names, for a log line: the shard count, H2 in memory, or the URL. */
    static String storage(ServerConfig config) {
        if (config.topology() != null) return config.topology().shards().size() + " shards";
        return config.isInMemory() ? "H2 in memory" : config.jdbcUrl();
    }
}
