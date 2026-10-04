package com.wiggle.server.auth;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.regex.Pattern;

/**
 * PBKDF2-HMAC-SHA256 password hashes over a per-account random salt, and the rules a name and a
 * password must meet. The iteration count travels with each hash, so it can be raised without
 * invalidating hashes already stored.
 */
public final class Passwords {

    /** OWASP's floor for PBKDF2-HMAC-SHA256, and low enough that HTTP Basic stays usable per request. */
    public static final int ITERATIONS = 210_000;
    private static final int SALT_BYTES = 16;
    private static final int KEY_BITS = 256;
    private static final int MIN_PASSWORD = 8;
    private static final int MAX_PASSWORD = 200;
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9._-]{1,64}");
    private static final SecureRandom RANDOM = new SecureRandom();

    /** A derived key and the salt and rounds it was derived with, both Base64. */
    public record Hashed(String salt, String hash, int iterations) { }

    public static Hashed hash(String password) {
        byte[] salt = new byte[SALT_BYTES];
        RANDOM.nextBytes(salt);
        Base64.Encoder enc = Base64.getEncoder();
        return new Hashed(enc.encodeToString(salt), enc.encodeToString(derive(password, salt, ITERATIONS, KEY_BITS)),
                ITERATIONS);
    }

    /** Whether {@code password} derives {@code hash}, compared in constant time. */
    public static boolean matches(String password, String salt, String hash, int iterations) {
        if (password == null) return false;
        byte[] expected = Base64.getDecoder().decode(hash);
        byte[] actual = derive(password, Base64.getDecoder().decode(salt), iterations, expected.length * 8);
        return MessageDigest.isEqual(expected, actual);
    }

    public static void requireName(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("a name is 1-64 characters of letters, digits, "
                    + "dot, dash or underscore; got '" + name + "'");
        }
    }

    public static void requirePassword(String password) {
        if (password == null || password.length() < MIN_PASSWORD) {
            throw new IllegalArgumentException("a password is at least " + MIN_PASSWORD + " characters");
        }
        if (password.length() > MAX_PASSWORD) {
            throw new IllegalArgumentException("a password is at most " + MAX_PASSWORD + " characters");
        }
    }

    private static byte[] derive(String password, byte[] salt, int iterations, int bits) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, bits);
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException("PBKDF2 is unavailable on this JVM", e);
        }
    }

    private Passwords() { }
}
