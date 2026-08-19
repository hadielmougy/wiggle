package dev.wiggle.core;

import java.security.SecureRandom;
import java.util.UUID;

public final class Ids {
    private static final SecureRandom RND = new SecureRandom();
    private static final char[] B32 = "0123456789abcdefghjkmnpqrstvwxyz".toCharArray();

    private Ids() {}

    /** Lexicographically sortable, time-prefixed id (ULID-ish). */
    public static String next(String prefix) {
        long t = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(prefix.length() + 1 + 26);
        sb.append(prefix).append('_');
        for (int i = 9; i >= 0; i--) sb.append(B32[(int) ((t >>> (i * 5)) & 31)]);
        for (int i = 0; i < 12; i++) sb.append(B32[RND.nextInt(32)]);
        return sb.toString();
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }
}
