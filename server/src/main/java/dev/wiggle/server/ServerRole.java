package dev.wiggle.server;

import java.util.Locale;

/**
 * How a server node runs. A {@code CELL} node is the engine as it has always been: it serves the
 * {@code WiggleControlPlane}, dispatches work, and runs the clock-driven housekeeping. A
 * {@code COORDINATOR} node runs no engine -- it serves the control plane that shards workflow
 * instances across cells (see {@code docs/cell-coordinator-tickets.md}).
 *
 * <p>The default is {@link #CELL}, so a server with no {@code WIGGLE_ROLE} set behaves exactly as it
 * always has.
 */
public enum ServerRole {
    CELL,
    COORDINATOR;

    /** Parses {@code WIGGLE_ROLE}; blank/null is {@link #CELL}. */
    public static ServerRole fromString(String s) {
        if (s == null || s.isBlank()) return CELL;
        return switch (s.trim().toLowerCase(Locale.ROOT)) {
            case "cell" -> CELL;
            case "coordinator" -> COORDINATOR;
            default -> throw new IllegalArgumentException(
                    "unknown WIGGLE_ROLE '" + s + "' (expected 'cell' or 'coordinator')");
        };
    }
}
