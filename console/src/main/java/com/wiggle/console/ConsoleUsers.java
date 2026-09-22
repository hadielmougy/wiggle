package com.wiggle.console;

import com.wiggle.core.Json;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The console's own user accounts, kept in one JSON file next to the console process rather than in
 * the control plane. The gRPC API has no per-RPC authorization yet, so anything reachable there is
 * reachable by every worker; console credentials stay out of that blast radius.
 *
 * <p>A password is stored as a PBKDF2-HMAC-SHA256 hash over a per-user random salt, never in the
 * clear, and the iteration count travels with each record so it can be raised without invalidating
 * what is already stored. Every change rewrites the whole file atomically, so a crash mid-write
 * leaves the previous file intact.
 */
final class ConsoleUsers {

    /** OWASP's floor for PBKDF2-HMAC-SHA256, and low enough that HTTP Basic stays usable per request. */
    private static final int ITERATIONS = 210_000;
    private static final int SALT_BYTES = 16;
    private static final int KEY_BITS = 256;
    private static final int MIN_PASSWORD = 8;
    private static final int MAX_PASSWORD = 200;
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    /** One account. {@code hash} is the derived key; nothing here can recover the password. */
    record User(String name, ConsoleAuth.Role role, String salt, String hash, int iterations,
                long createdAt, long updatedAt) { }

    private final Path file;
    private final SecureRandom random = new SecureRandom();
    /** name -> account, in insertion order. Guarded by {@code this}. */
    private final Map<String, User> users = new LinkedHashMap<>();

    ConsoleUsers(Path file) {
        this.file = file;
        load();
    }

    /** The accounts, oldest first. */
    synchronized List<User> list() {
        return List.copyOf(users.values());
    }

    synchronized boolean isEmpty() {
        return users.isEmpty();
    }

    synchronized boolean has(String name) {
        return users.containsKey(name);
    }

    synchronized int admins() {
        return (int) users.values().stream().filter(u -> u.role() == ConsoleAuth.Role.ADMIN).count();
    }

    /** The role these credentials authenticate as, or null when they match no account. */
    synchronized ConsoleAuth.Role verify(String name, String password) {
        User u = users.get(name);
        if (u == null || password == null) return null;
        byte[] expected = Base64.getDecoder().decode(u.hash());
        byte[] actual = derive(password, Base64.getDecoder().decode(u.salt()), u.iterations(), expected.length * 8);
        return MessageDigest.isEqual(expected, actual) ? u.role() : null;
    }

    /** Adds an account. Refuses a name that is taken, reserved by a built-in, or malformed. */
    synchronized User create(String name, String password, ConsoleAuth.Role role, Set<String> reserved, long now) {
        requireName(name);
        requirePassword(password);
        if (users.containsKey(name)) {
            throw new IllegalArgumentException("user '" + name + "' already exists");
        }
        if (reserved.contains(name)) {
            throw new IllegalArgumentException("user '" + name + "' is the name of a built-in account "
                    + "configured in the environment; pick another name");
        }
        User u = hashed(name, password, role, now, now);
        users.put(name, u);
        save();
        return u;
    }

    /** Replaces an account's password, leaving its role and creation time alone. */
    synchronized void setPassword(String name, String password, long now) {
        requirePassword(password);
        User existing = require(name);
        users.put(name, hashed(name, password, existing.role(), existing.createdAt(), now));
        save();
    }

    synchronized void delete(String name) {
        require(name);
        users.remove(name);
        save();
    }

    private User require(String name) {
        User u = users.get(name);
        if (u == null) throw new IllegalArgumentException("no user '" + name + "'");
        return u;
    }

    private User hashed(String name, String password, ConsoleAuth.Role role, long createdAt, long updatedAt) {
        byte[] salt = new byte[SALT_BYTES];
        random.nextBytes(salt);
        byte[] key = derive(password, salt, ITERATIONS, KEY_BITS);
        Base64.Encoder enc = Base64.getEncoder();
        return new User(name, role, enc.encodeToString(salt), enc.encodeToString(key), ITERATIONS,
                createdAt, updatedAt);
    }

    static void requireName(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("a user name is 1-64 characters of letters, digits, "
                    + "dot, dash or underscore; got '" + name + "'");
        }
    }

    static void requirePassword(String password) {
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

    // -- persistence ------------------------------------------------------------------

    private void load() {
        if (!Files.exists(file)) return;
        Object parsed;
        try {
            parsed = Json.parse(Files.readString(file, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("could not read the console user file " + file, e);
        }
        for (Object entry : Json.asArray(Json.asObject(parsed).get("users"))) {
            Map<String, Object> m = Json.asObject(entry);
            String name = Json.reqStr(m, "name");
            users.put(name, new User(name,
                    ConsoleAuth.Role.of(Json.str(m, "role", "viewer")),
                    Json.reqStr(m, "salt"), Json.reqStr(m, "hash"),
                    (int) Json.num(m, "iterations", ITERATIONS),
                    Json.num(m, "createdAt", 0), Json.num(m, "updatedAt", 0)));
        }
    }

    /** Writes the whole file, then moves it into place: a crash mid-write cannot truncate the accounts. */
    private void save() {
        List<Object> out = new ArrayList<>(users.size());
        for (User u : users.values()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", u.name());
            m.put("role", u.role().wire());
            m.put("salt", u.salt());
            m.put("hash", u.hash());
            m.put("iterations", u.iterations());
            m.put("createdAt", u.createdAt());
            m.put("updatedAt", u.updatedAt());
            out.add(m);
        }
        String json = Json.write(Map.of("users", out));
        try {
            Path dir = file.toAbsolutePath().getParent();
            if (dir != null) Files.createDirectories(dir);
            Path tmp = Files.createTempFile(dir, ".wiggle-users", ".tmp");
            restrict(tmp);
            Files.writeString(tmp, json, StandardCharsets.UTF_8);
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
            restrict(file);
        } catch (IOException e) {
            throw new UncheckedIOException("could not write the console user file " + file, e);
        }
    }

    /** Owner-only where the filesystem has POSIX permissions; elsewhere the default stands. */
    private static void restrict(Path path) {
        try {
            Files.setPosixFilePermissions(path, Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
        } catch (IOException | UnsupportedOperationException ignored) {
            // Windows, or a filesystem without POSIX bits: the file keeps the process umask.
        }
    }
}
