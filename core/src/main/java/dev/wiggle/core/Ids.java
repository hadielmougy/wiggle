package dev.wiggle.core;

import java.security.SecureRandom;
import java.util.UUID;

public final class Ids {
    private static final SecureRandom RND = new SecureRandom();
    private static final char[] B32 = "0123456789abcdefghjkmnpqrstvwxyz".toCharArray();

    private Ids() {}

    /** Lexicographically sortable, time-prefixed id (ULID-ish), e.g. {@code wfi_01h8...}. */
    public static String next(String prefix) {
        return prefix + '_' + token();
    }

    /** A bare 22-char lexicographically sortable token (10 time chars + 12 random), no prefix. */
    public static String token() {
        long t = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder(22);
        for (int i = 9; i >= 0; i--) sb.append(B32[(int) ((t >>> (i * 5)) & 31)]);
        for (int i = 0; i < 12; i++) sb.append(B32[RND.nextInt(32)]);
        return sb.toString();
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }
}
