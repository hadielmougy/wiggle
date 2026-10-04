package com.wiggle.server.search;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The embedders a server is configured with, from the environment: {@code WIGGLE_EMBEDDER} is
 * {@code none} (the default), {@code hashing} or {@code http}. For {@code http},
 * {@code WIGGLE_EMBEDDER_URL}, {@code WIGGLE_EMBEDDER_MODEL}, {@code WIGGLE_EMBEDDER_DIMENSION} and
 * {@code WIGGLE_EMBEDDER_API_KEY} configure the model new vectors are made with, and
 * {@code WIGGLE_EMBEDDER_PREVIOUS_MODEL} / {@code _DIMENSION} name the model it replaces on the same
 * service, so queries can keep using that one until the new index is built. For {@code hashing},
 * {@code WIGGLE_EMBEDDER_DIMENSION} (default 256).
 */
public final class Embedders {

    /** The embedders in {@code env}: the one indexing first, then any it replaces. Empty when none. */
    public static List<Embedder> fromEnvironment(Map<String, String> env) {
        String kind = env.getOrDefault("WIGGLE_EMBEDDER", "none").trim().toLowerCase();
        List<Embedder> out = new ArrayList<>();
        switch (kind) {
            case "", "none" -> { }
            case "hashing" -> out.add(new HashingEmbedder(intOf(env, "WIGGLE_EMBEDDER_DIMENSION", 256)));
            case "http" -> {
                String url = env.get("WIGGLE_EMBEDDER_URL");
                String key = env.get("WIGGLE_EMBEDDER_API_KEY");
                out.add(new HttpEmbedder(url, env.get("WIGGLE_EMBEDDER_MODEL"),
                        intOf(env, "WIGGLE_EMBEDDER_DIMENSION", 0), key));
                String previous = env.get("WIGGLE_EMBEDDER_PREVIOUS_MODEL");
                if (previous != null && !previous.isBlank()) {
                    out.add(new HttpEmbedder(url, previous, intOf(env, "WIGGLE_EMBEDDER_PREVIOUS_DIMENSION", 0), key));
                }
            }
            default -> throw new IllegalArgumentException("WIGGLE_EMBEDDER='" + kind + "' is not one of none, hashing, http");
        }
        return out;
    }

    private static int intOf(Map<String, String> env, String key, int def) {
        String v = env.get(key);
        if (v == null || v.isBlank()) {
            if (def > 0) return def;
            throw new IllegalArgumentException(key + " is required");
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + "='" + v + "' is not a number");
        }
    }

    private Embedders() { }
}
