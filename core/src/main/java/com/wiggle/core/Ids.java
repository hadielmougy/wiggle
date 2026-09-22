package com.wiggle.core;

import java.security.SecureRandom;

public final class Ids {
    private static final SecureRandom RND = new SecureRandom();
    private static final char[] B32 = "0123456789abcdefghjkmnpqrstvwxyz".toCharArray();

    private Ids() {}

    /** Lexicographically sortable, time-prefixed id (ULID-ish), e.g. {@code wfi_01h8...}. */
    public static String next(String prefix) {
        return prefix + '_' + token();
    }

    /** A bare 22-char lexicographically sortable token (10 time chars + 12 random), no prefix. */
    /**
     * A token-shaped digest of {@code key}: 22 base32 characters of its SHA-256, so an id derived
     * from a business key has the same shape as a minted one and the same key always yields it.
     */
    public static String digest(String key) {
        byte[] h;
        try {
            h = java.security.MessageDigest.getInstance("SHA-256").digest(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        StringBuilder sb = new StringBuilder(22);
        long bits = 0; int n = 0, i = 0;
        while (sb.length() < 22) {
            if (n < 5) { bits = (bits << 8) | (h[i++] & 0xff); n += 8; }
            sb.append(B32[(int) ((bits >>> (n - 5)) & 31)]);
            n -= 5;
        }
        return sb.toString();
    }

    public static String token() {
        long t = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(22);
        for (int i = 9; i >= 0; i--) sb.append(B32[(int) ((t >>> (i * 5)) & 31)]);
        for (int i = 0; i < 12; i++) sb.append(B32[RND.nextInt(32)]);
        return sb.toString();
    }
}
