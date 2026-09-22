package com.wiggle.server.engine;

/** Reading the engine's numeric environment overrides. */
final class ServerEnv {

    private ServerEnv() {}

    /** A long from {@code env}, or {@code def} if unset, blank or unparseable. */
    /** A system property first, then the environment: the property is how a test shortens a threshold. */
    static long envLong(String prop, String env, long def) {
        String v = System.getProperty(prop);
        if (v != null && !v.isBlank()) {
            try { return Long.parseLong(v.trim()); } catch (NumberFormatException ignored) { }
        }
        return envLong(env, def);
    }

    static long envLong(String env, long def) {
        String v = System.getenv(env);
        if (v == null || v.isBlank()) return def;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
