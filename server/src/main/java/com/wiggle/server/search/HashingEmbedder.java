package com.wiggle.server.search;

import com.wiggle.server.store.SearchText;

import java.util.ArrayList;
import java.util.List;

/**
 * An embedder that needs no model: each word is hashed to one of {@code dimension} buckets with a
 * sign, and the vector is normalised. Texts that share words point the same way; words that only
 * mean the same thing do not. For development and tests, and to try the semantic path without an
 * embedding service.
 */
public final class HashingEmbedder implements Embedder {

    private final int dimension;

    public HashingEmbedder(int dimension) {
        if (dimension <= 0) throw new IllegalArgumentException("dimension is positive: " + dimension);
        this.dimension = dimension;
    }

    @Override public String model() {
        return "hashing-" + dimension;
    }

    @Override public int dimension() {
        return dimension;
    }

    @Override public List<float[]> embed(List<String> texts) {
        List<float[]> out = new ArrayList<>(texts.size());
        for (String text : texts) {
            float[] v = new float[dimension];
            for (String w : SearchText.terms(text)) {
                int h = w.hashCode() * 0x9E3779B1;
                v[Math.floorMod(h, dimension)] += (h & 0x10000) == 0 ? 1 : -1;
            }
            double norm = 0;
            for (float f : v) norm += f * f;
            if (norm > 0) {
                float n = (float) Math.sqrt(norm);
                for (int i = 0; i < v.length; i++) v[i] /= n;
            }
            out.add(v);
        }
        return out;
    }
}
