package com.wiggle.dist;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Settings of the removed cell coordinator. A deployment that still sets one is refused at startup
 * rather than started without the behaviour it asked for.
 */
final class RemovedSettings {

    static final List<String> COORDINATOR = List.of(
            "WIGGLE_COORDINATOR_URL", "WIGGLE_NAMESPACE", "WIGGLE_CELL_ID", "WIGGLE_ADVERTISE_HOST",
            "WIGGLE_REGION", "WIGGLE_COORD_STORE", "WIGGLE_COORD_JDBC_USER", "WIGGLE_COORD_JDBC_PASSWORD",
            "WIGGLE_COORD_JDBC_POOL", "WIGGLE_ENDPOINT_REWRITE");

    private RemovedSettings() { }

    /** Throws when {@code env} sets any removed setting to a non-blank value. */
    static void reject(Map<String, String> env) {
        List<String> set = new ArrayList<>();
        for (String name : COORDINATOR) {
            String v = env.get(name);
            if (v != null && !v.isBlank()) set.add(name);
        }
        if (!set.isEmpty()) {
            throw new IllegalStateException("the cell coordinator was removed, but " + String.join(", ", set)
                    + (set.size() == 1 ? " is" : " are") + " set. Unset "
                    + (set.size() == 1 ? "it" : "them") + "; server nodes that share a database form a "
                    + "cluster on their own, and clients connect with WIGGLE_URL.");
        }
    }
}
