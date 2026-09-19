package com.wiggle.server.engine;

/** Reading the engine's numeric environment overrides. */
final class ServerEnv {

    private ServerEnv() {}

    /** A long from {@code env}, or {@code def} if unset, blank or unparseable. */
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
