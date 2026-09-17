package com.wiggle.dist;

import java.util.Locale;

/**
 * Which of the three processes this image should run, from {@code WIGGLE_ROLE}.
 *
 * <p>{@code cell} is the old name for {@link #SERVER} and is still accepted, so existing
 * deployments and manifests keep working unchanged.
 *
 * <p>An unrecognised value is refused rather than defaulted. Dispatch used to be two
 * {@code equalsIgnoreCase} checks with everything else falling through to the server, so
 * {@code WIGGLE_ROLE=coordinatr} started a server and looked fine until something tried to resolve
 * through it.
 */
enum Role {
    SERVER, COORDINATOR, CONSOLE;

    static final String ENV = "WIGGLE_ROLE";

    /** The role {@code raw} names; {@link #SERVER} when it is null or blank. */
    static Role of(String raw) {
        if (raw == null || raw.isBlank()) return SERVER;
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "server", "cell" -> SERVER;
            case "coordinator" -> COORDINATOR;
            case "console" -> CONSOLE;
            default -> throw new IllegalArgumentException(
                    ENV + "='" + raw.trim() + "' is not a role; expected one of: server, "
                    + "coordinator, console ('cell' is the old name for server)");
        };
    }

    /** The role from the environment. */
    static Role fromEnvironment() {
        return of(System.getenv(ENV));
    }
}
