package dev.wiggle.dist.coord;

import dev.wiggle.server.ServerConfig;

/** The standalone path: config from environment/system properties, exactly as today. */
public final class EnvConfigSource implements ConfigSource {
    @Override public ServerConfig load() {
        return ServerConfig.fromEnvironment();
    }
}
