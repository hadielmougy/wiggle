package dev.wiggle.dist.coord;

import dev.wiggle.server.ServerConfig;

/**
 * Config source used when a coordinator is configured. It starts from a base (env) config and, in a
 * later phase, overlays the coordinator's namespace-scoped config (storage + tuning), caching the
 * result and falling back cached-then-local if the coordinator is unreachable.
 *
 * <p>Phase 0 stub: the seam exists but there is no overlay yet -- it returns the base config, so a
 * coordinated node still boots correctly. The real {@code FetchConfig} overlay + cache/fallback lands
 * in Phase 1 (see {@code docs/phase-1-tickets.md}, T7).
 */
public final class CoordinatorConfigSource implements ConfigSource {

    private final ConfigSource base;
    private final String coordinatorUrl;

    public CoordinatorConfigSource(ConfigSource base, String coordinatorUrl) {
        this.base = base;
        this.coordinatorUrl = coordinatorUrl;
    }

    /** The coordinator this source will fetch from once T7 wires the overlay. */
    public String coordinatorUrl() { return coordinatorUrl; }

    @Override public ServerConfig load() {
        // Phase 0: no overlay yet. T7 will FetchConfig from `coordinatorUrl` and merge over `base`.
        return base.load();
    }
}
