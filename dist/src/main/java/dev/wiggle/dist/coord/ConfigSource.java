package dev.wiggle.dist.coord;

import dev.wiggle.server.ServerConfig;

/**
 * Where a node's {@link ServerConfig} comes from. The default {@link EnvConfigSource} reads the
 * environment exactly as the server always has; a {@link CoordinatorConfigSource} (when a coordinator
 * is configured) can overlay coordinator-supplied config on top.
 */
public interface ConfigSource {
    ServerConfig load();
}
