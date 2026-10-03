package com.wiggle.dist;

import java.util.Locale;

/**
 * Which of the two processes this image should run, from {@code WIGGLE_ROLE}.
 *
 * <p>{@code cell} is the old name for {@link #SERVER} and is still accepted, so existing
 * deployments and manifests keep working unchanged.
 *
 * <p>An unrecognised value is refused rather than defaulted. Dispatch used to be two
 * {@code equalsIgnoreCase} checks with everything else falling through to the server, so a
 * misspelt role started a server and looked fine.
 */
enum Role {
    SERVER, CONSOLE;

    static final String ENV = "WIGGLE_ROLE";

    /** The role {@code raw} names; {@link #SERVER} when it is null or blank. */
    static Role of(String raw) {
        if (raw == null || raw.isBlank()) return SERVER;
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "server", "cell" -> SERVER;
            case "console" -> CONSOLE;
            case "coordinator" -> throw new IllegalArgumentException(
                    ENV + "=coordinator: the cell coordinator was removed; run server nodes that share "
                    + "a database instead");
            default -> throw new IllegalArgumentException(
                    ENV + "='" + raw.trim() + "' is not a role; expected one of: server, console "
                    + "('cell' is the old name for server)");
        };
    }

    /** The role from the environment. */
    static Role fromEnvironment() {
        return of(System.getenv(ENV));
    }
}
