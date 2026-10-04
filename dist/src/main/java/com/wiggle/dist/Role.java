package com.wiggle.dist;

import java.util.Locale;

/**
 * Which process this image should run, from {@code WIGGLE_ROLE}. There is one: the server, which
 * also serves the portal when {@code WIGGLE_PORTAL_PORT} is set.
 *
 * <p>{@code cell} is the old name for {@link #SERVER} and is still accepted, so existing
 * deployments and manifests keep working unchanged.
 *
 * <p>An unrecognised value is refused rather than defaulted. Dispatch used to be two
 * {@code equalsIgnoreCase} checks with everything else falling through to the server, so a
 * misspelt role started a server and looked fine.
 */
enum Role {
    SERVER;

    static final String ENV = "WIGGLE_ROLE";

    /** The role {@code raw} names; {@link #SERVER} when it is null or blank. */
    static Role of(String raw) {
        if (raw == null || raw.isBlank()) return SERVER;
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "server", "cell" -> SERVER;
            case "console" -> throw new IllegalArgumentException(
                    ENV + "=console: the standalone console was removed; set WIGGLE_PORTAL_PORT on a "
                    + "server node to serve the portal from it");
            case "coordinator" -> throw new IllegalArgumentException(
                    ENV + "=coordinator: the cell coordinator was removed; run server nodes that share "
                    + "a database instead");
            default -> throw new IllegalArgumentException(
                    ENV + "='" + raw.trim() + "' is not a role; expected: server "
                    + "('cell' is the old name for server)");
        };
    }

    /** The role from the environment. */
    static Role fromEnvironment() {
        return of(System.getenv(ENV));
    }
}
