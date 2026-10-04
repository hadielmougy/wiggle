package com.wiggle.server.store;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Embeddings as stores keep them, and the nearest-neighbour search for stores without their own. */
public final class Vectors {

    /** Cosine similarity: 1 for the same direction, 0 for unrelated, -1 for opposite; 0 when either is all zeros. */
    public static double cosine(float[] a, float[] b) {
        if (a.length != b.length) throw new IllegalArgumentException("vectors of " + a.length + " and " + b.length + " dimensions");
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            na += (double) a[i] * a[i];
            nb += (double) b[i] * b[i];
        }
        return na == 0 || nb == 0 ? 0 : dot / Math.sqrt(na * nb);
    }

    /** Little-endian float32s, four bytes a dimension. */
    public static byte[] encode(float[] v) {
        ByteBuffer b = ByteBuffer.allocate(v.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (float f : v) b.putFloat(f);
        return b.array();
    }

    public static float[] decode(byte[] bytes) {
        ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        float[] v = new float[bytes.length / 4];
        for (int i = 0; i < v.length; i++) v[i] = b.getFloat();
        return v;
    }

    /** pgvector's text form, {@code [1,2,3]}. */
    public static String literal(float[] v) {
        StringBuilder s = new StringBuilder("[");
        for (int i = 0; i < v.length; i++) {
            if (i > 0) s.append(',');
            s.append(v[i]);
        }
        return s.append(']').toString();
    }

    public static float[] parseLiteral(String text) {
        String body = text.trim();
        body = body.substring(1, body.length() - 1);
        if (body.isBlank()) return new float[0];
        String[] parts = body.split(",");
        float[] v = new float[parts.length];
        for (int i = 0; i < parts.length; i++) v[i] = Float.parseFloat(parts[i].trim());
        return v;
    }

    /**
     * The {@code limit} documents of {@code docs} whose vector in {@code vectors} is closest to
     * {@code query}, scored by cosine similarity, best first; a document without one is skipped.
     */
    public static List<Rows.SearchHit> nearest(List<Rows.SearchDoc> docs, Map<String, float[]> vectors, float[] query,
                                               int limit) {
        List<Rows.SearchHit> hits = new ArrayList<>();
        for (Rows.SearchDoc d : docs) {
            float[] v = vectors.get(d.instanceId());
            if (v != null && v.length == query.length) hits.add(new Rows.SearchHit(d, cosine(v, query)));
        }
        hits.sort(Comparator.comparingDouble(Rows.SearchHit::score).reversed()
                .thenComparing(SearchText.BEST_FIRST));
        return hits.size() > limit ? List.copyOf(hits.subList(0, limit)) : hits;
    }

    private Vectors() { }
}
