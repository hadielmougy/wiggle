package com.wiggle.server.auth;

import com.wiggle.core.Json;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Portal accounts, roles and sessions on the auth shard. Every change is one transaction there that
 * also appends to the audit, which is what tells every node to drop what it has cached.
 *
 * <p>Changes that would leave no account able to manage users are refused unless {@code builtinAdmin}
 * says an account from the environment can still sign in and put it right.
 */
public final class Accounts {

    private static final System.Logger LOG = System.getLogger(Accounts.class.getName());
    private static final SecureRandom RANDOM = new SecureRandom();
    static final String IMPORTED = "users.import";

    /** An account as an administrator sees it: no hash. */
    public record User(String name, List<String> roles, boolean disabled, long createdAt, long updatedAt) { }

    /** An account with what it may do and what its password must derive. */
    public record Account(String name, Set<String> permissions, boolean disabled, String salt, String hash,
                          int iterations) {
        public boolean passwordMatches(String password) {
            return Passwords.matches(password, salt, hash, iterations);
        }
    }

    /** A machine credential as a caller: what it may do, and when it stops working (null: never). */
    public record Machine(String id, Set<String> permissions, Long expiresAt) {
        public boolean expired(long now) {
            return expiresAt != null && expiresAt <= now;
        }
    }

    /** What an API key starts with, so a leaked one is recognisable in logs and scanners. */
    public static final String KEY_PREFIX = "wgk_";

    private final Storage storage;
    private final LongSupplier clock;

    public Accounts(Storage storage, LongSupplier clock) {
        this.storage = storage;
        this.clock = clock;
    }

    /** Writes the built-in roles where they are missing or differ. */
    public void bootstrap() {
        storage.inAuth(tx -> {
            long now = clock.getAsLong();
            Map<String, Rows.AuthRole> existing = rolesByName(tx);
            Permissions.BUILTIN_ROLES.forEach((name, perms) -> {
                Rows.AuthRole r = existing.get(name);
                if (r != null && r.builtin() && r.permissions().equals(perms)) return;
                tx.putAuthRole(new Rows.AuthRole(name, perms, true, r == null ? now : r.createdAt(), now));
                audit(tx, null, "role.put", name, String.join(" ", new TreeSet<>(perms)));
            });
            return null;
        });
    }

    /**
     * Imports a console users file the first time any node sees one, keeping each hash as it is.
     * Accounts already on the auth shard are left alone. Returns how many were imported; 0 when
     * this deployment imported before.
     */
    public int importFile(Path file, Set<String> reserved) {
        Object parsed;
        try {
            parsed = Json.parse(Files.readString(file, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("could not read the console user file " + file, e);
        }
        List<Object> entries = Json.asArray(Json.asObject(parsed).get("users"));
        return storage.inAuth(tx -> {
            if (tx.authAuditHas(IMPORTED)) return 0;
            Set<String> roles = rolesByName(tx).keySet();
            int imported = 0;
            for (Object entry : entries) {
                Map<String, Object> m = Json.asObject(entry);
                String name = Json.reqStr(m, "name");
                if (reserved.contains(name) || tx.findAuthUser(name).isPresent()) continue;
                String role = Json.str(m, "role", Permissions.VIEWER).trim().toLowerCase();
                if (role.equals("operator")) role = Permissions.ADMIN;
                if (!roles.contains(role)) role = Permissions.VIEWER;
                long now = clock.getAsLong();
                tx.putAuthUser(new Rows.AuthUser(name, Json.reqStr(m, "hash"), Json.reqStr(m, "salt"),
                        (int) Json.num(m, "iterations", Passwords.ITERATIONS), false,
                        Json.num(m, "createdAt", now), Json.num(m, "updatedAt", now)));
                tx.setAuthRolesOf(name, List.of(role));
                audit(tx, null, "user.create", name, "imported from " + file.getFileName());
                imported++;
            }
            audit(tx, null, IMPORTED, null, imported + " account(s) from " + file);
            return imported;
        });
    }

    /** Whether no account exists yet. */
    public boolean isEmpty() {
        return storage.inAuth(tx -> tx.authUsers().isEmpty());
    }

    public List<User> users() {
        return storage.inAuth(tx -> tx.authUsers().stream()
                .map(u -> new User(u.name(), tx.authRolesOf(u.name()), u.disabled(), u.createdAt(), u.updatedAt()))
                .toList());
    }

    public List<Rows.AuthRole> roles() {
        return storage.inAuth(Tx::authRoles);
    }

    /** The account {@code name}, read from the auth primary. */
    public Optional<Account> account(String name) {
        return storage.inAuth(tx -> tx.findAuthUser(name).map(u -> new Account(u.name(), permissionsOf(tx, u.name()),
                u.disabled(), u.salt(), u.hash(), u.iterations())));
    }

    /** Creates an account holding {@code roles}. Refuses a name taken, reserved by a built-in, or malformed. */
    public void create(String actor, String name, String password, List<String> roles, Set<String> reserved,
                       boolean builtinAdmin) {
        Passwords.requireName(name);
        Passwords.requirePassword(password);
        if (reserved.contains(name)) {
            throw new IllegalArgumentException("user '" + name + "' is the name of a built-in account "
                    + "configured in the environment; pick another name");
        }
        Passwords.Hashed h = Passwords.hash(password);
        storage.inAuth(tx -> {
            if (tx.findAuthUser(name).isPresent()) throw new IllegalArgumentException("user '" + name + "' already exists");
            requireRoles(tx, roles);
            requireManager(tx, builtinAdmin, "creating '" + name + "' with roles " + roles,
                    users -> users.put(name, new Member(false, roles)), r -> { });
            long now = clock.getAsLong();
            tx.putAuthUser(new Rows.AuthUser(name, h.hash(), h.salt(), h.iterations(), false, now, now));
            tx.setAuthRolesOf(name, roles);
            audit(tx, actor, "user.create", name, String.join(" ", roles));
            return null;
        });
    }

    /** Replaces an account's password and ends its sessions, except {@code keepToken} when non-null. */
    public void setPassword(String actor, String name, String password, String keepToken) {
        Passwords.requirePassword(password);
        Passwords.Hashed h = Passwords.hash(password);
        storage.inAuth(tx -> {
            Rows.AuthUser u = require(tx, name);
            tx.putAuthUser(new Rows.AuthUser(name, h.hash(), h.salt(), h.iterations(), u.disabled(), u.createdAt(),
                    clock.getAsLong()));
            tx.deleteAuthSessionsOf(name, keepToken == null ? null : tokenHash(keepToken));
            audit(tx, actor, "user.password", name, null);
            return null;
        });
    }

    public void setRoles(String actor, String name, List<String> roles, boolean builtinAdmin) {
        storage.inAuth(tx -> {
            Rows.AuthUser u = require(tx, name);
            requireRoles(tx, roles);
            requireManager(tx, builtinAdmin, "giving '" + name + "' roles " + roles,
                    users -> users.put(name, new Member(u.disabled(), roles)), r -> { });
            tx.setAuthRolesOf(name, roles);
            audit(tx, actor, "user.roles", name, String.join(" ", roles));
            return null;
        });
    }

    /** Disables or enables an account; disabling also ends its sessions. */
    public void setDisabled(String actor, String name, boolean disabled, boolean builtinAdmin) {
        storage.inAuth(tx -> {
            Rows.AuthUser u = require(tx, name);
            requireManager(tx, builtinAdmin, "disabling '" + name + "'",
                    users -> users.put(name, new Member(disabled, tx.authRolesOf(name))), r -> { });
            tx.putAuthUser(new Rows.AuthUser(name, u.hash(), u.salt(), u.iterations(), disabled, u.createdAt(),
                    clock.getAsLong()));
            if (disabled) tx.deleteAuthSessionsOf(name, null);
            audit(tx, actor, disabled ? "user.disable" : "user.enable", name, null);
            return null;
        });
    }

    public void delete(String actor, String name, boolean builtinAdmin) {
        storage.inAuth(tx -> {
            require(tx, name);
            requireManager(tx, builtinAdmin, "deleting '" + name + "'", users -> users.remove(name), r -> { });
            tx.deleteAuthUser(name);
            audit(tx, actor, "user.delete", name, null);
            return null;
        });
    }

    /** Creates or replaces a role. A built-in role cannot be changed. */
    public void putRole(String actor, String name, Collection<String> permissions, boolean builtinAdmin) {
        Passwords.requireName(name);
        Set<String> perms = Permissions.validate(permissions);
        if (perms.isEmpty()) throw new IllegalArgumentException("a role grants at least one permission");
        storage.inAuth(tx -> {
            Rows.AuthRole existing = rolesByName(tx).get(name);
            if (existing != null && existing.builtin()) {
                throw new IllegalArgumentException("'" + name + "' is a built-in role and cannot be changed");
            }
            requireManager(tx, builtinAdmin, "changing role '" + name + "'", u -> { }, r -> r.put(name, perms));
            long now = clock.getAsLong();
            tx.putAuthRole(new Rows.AuthRole(name, perms, false, existing == null ? now : existing.createdAt(), now));
            audit(tx, actor, "role.put", name, String.join(" ", new TreeSet<>(perms)));
            return null;
        });
    }

    /**
     * Creates an API key credential holding {@code role} and returns the key. Only its hash is
     * stored, so this is the one time anyone sees it.
     */
    public String createApiKey(String actor, String id, String role, Long expiresAt) {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        String key = KEY_PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        createCredential(actor, new Rows.AuthCredential(id, Rows.AuthCredential.API_KEY, tokenHash(key), null, role,
                clock.getAsLong(), expiresAt));
        return key;
    }

    /** Creates a credential for the client certificate whose subject is {@code subject}, holding {@code role}. */
    public void createCertificate(String actor, String id, String subject, String role, Long expiresAt) {
        if (subject == null || subject.isBlank()) throw new IllegalArgumentException("a certificate credential names a subject");
        createCredential(actor, new Rows.AuthCredential(id, Rows.AuthCredential.MTLS, null, subject.trim(), role,
                clock.getAsLong(), expiresAt));
    }

    private void createCredential(String actor, Rows.AuthCredential c) {
        Passwords.requireName(c.id());
        storage.inAuth(tx -> {
            if (tx.authCredentials().stream().anyMatch(x -> x.id().equals(c.id()))) {
                throw new IllegalArgumentException("credential '" + c.id() + "' already exists");
            }
            if (c.subject() != null && tx.findAuthCredentialBySubject(c.subject()).isPresent()) {
                throw new IllegalArgumentException("a credential for subject '" + c.subject() + "' already exists");
            }
            requireRoles(tx, List.of(c.role()));
            tx.insertAuthCredential(c);
            audit(tx, actor, "credential.create", c.id(), c.kind() + " " + c.role());
            return null;
        });
    }

    /** Every machine credential; a key's hash is the only trace of it. */
    public List<Rows.AuthCredential> credentials() {
        return storage.inAuth(Tx::authCredentials);
    }

    public void deleteCredential(String actor, String id) {
        storage.inAuth(tx -> {
            if (!tx.deleteAuthCredential(id)) throw new IllegalArgumentException("no credential '" + id + "'");
            audit(tx, actor, "credential.delete", id, null);
            return null;
        });
    }

    /** The API key credential whose key hashes to {@code keyHash}, read from the auth primary. */
    public Optional<Machine> machineByKeyHash(String keyHash) {
        return storage.inAuth(tx -> tx.findAuthCredentialByKeyHash(keyHash).map(c -> machine(tx, c)));
    }

    /** The credential for client certificate subject {@code subject}, read from the auth primary. */
    public Optional<Machine> machineBySubject(String subject) {
        return storage.inAuth(tx -> tx.findAuthCredentialBySubject(subject).map(c -> machine(tx, c)));
    }

    private static Machine machine(Tx tx, Rows.AuthCredential c) {
        Rows.AuthRole role = rolesByName(tx).get(c.role());
        return new Machine(c.id(), role == null ? Set.of() : role.permissions(), c.expiresAt());
    }

    /** Deletes a role and every grant of it. A built-in role cannot be deleted. */
    public void deleteRole(String actor, String name, boolean builtinAdmin) {
        storage.inAuth(tx -> {
            Rows.AuthRole existing = rolesByName(tx).get(name);
            if (existing == null) throw new IllegalArgumentException("no role '" + name + "'");
            if (existing.builtin()) throw new IllegalArgumentException("'" + name + "' is a built-in role and cannot be deleted");
            List<String> bound = tx.authCredentials().stream().filter(c -> c.role().equals(name))
                    .map(Rows.AuthCredential::id).toList();
            if (!bound.isEmpty()) {
                throw new IllegalArgumentException("role '" + name + "' is held by credential(s) " + bound
                        + "; delete them first");
            }
            requireManager(tx, builtinAdmin, "deleting role '" + name + "'", u -> { }, r -> r.remove(name));
            tx.deleteAuthRole(name);
            audit(tx, actor, "role.delete", name, null);
            return null;
        });
    }

    /** Opens a session for {@code user} and returns its token; only the token's hash is stored. */
    public String openSession(String user, long ttlMillis) {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        storage.inAuth(tx -> {
            long now = clock.getAsLong();
            tx.deleteExpiredAuthSessions(now, 100);
            tx.insertAuthSession(new Rows.AuthSession(tokenHash(token), user, now + ttlMillis, now));
            return null;
        });
        return token;
    }

    /** The session stored under {@code idHash}, read from the auth primary. */
    public Optional<Rows.AuthSession> session(String idHash) {
        return storage.inAuth(tx -> tx.findAuthSession(idHash));
    }

    /** Ends the session of {@code token} and records it, so every node drops it. */
    public void closeSession(String token) {
        String hash = tokenHash(token);
        storage.inAuth(tx -> {
            Optional<Rows.AuthSession> s = tx.findAuthSession(hash);
            if (s.isEmpty()) return null;
            tx.deleteAuthSession(hash);
            audit(tx, s.get().user(), "session.close", s.get().user(), hash);
            return null;
        });
    }

    /** Up to {@code max} audit entries after {@code afterSeq}, oldest first. */
    public List<Rows.AuthAudit> auditAfter(long afterSeq, int max) {
        return storage.inAuth(tx -> tx.authAuditAfter(afterSeq, max));
    }

    public long auditHead() {
        return storage.inAuth(Tx::authAuditHead);
    }

    /** The stored form of a session token. */
    public static String tokenHash(String token) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(token.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable on this JVM", e);
        }
    }

    private static Rows.AuthUser require(Tx tx, String name) {
        return tx.findAuthUser(name).orElseThrow(() -> new IllegalArgumentException("no user '" + name + "'"));
    }

    private static void requireRoles(Tx tx, List<String> roles) {
        if (roles.isEmpty()) throw new IllegalArgumentException("an account holds at least one role");
        Set<String> known = rolesByName(tx).keySet();
        for (String r : roles) {
            if (!known.contains(r)) throw new IllegalArgumentException("no role '" + r + "'; the roles are " + known);
        }
    }

    private static Map<String, Rows.AuthRole> rolesByName(Tx tx) {
        Map<String, Rows.AuthRole> out = new LinkedHashMap<>();
        for (Rows.AuthRole r : tx.authRoles()) out.put(r.name(), r);
        return out;
    }

    private static Set<String> permissionsOf(Tx tx, String user) {
        Map<String, Rows.AuthRole> roles = rolesByName(tx);
        Set<String> out = new TreeSet<>();
        for (String r : tx.authRolesOf(user)) {
            Rows.AuthRole role = roles.get(r);
            if (role != null) out.addAll(role.permissions());
        }
        return out;
    }

    /** An account's state as the manager check sees it. */
    private record Member(boolean disabled, List<String> roles) { }

    /**
     * Refuses a change, before anything is written, when it would leave no enabled account that may
     * manage users, unless a built-in admin can still sign in. That includes deleting the last
     * account, which would otherwise reopen the portal to anyone. {@code users} and
     * {@code roles} apply the change to copies of the current accounts and role permissions.
     */
    private static void requireManager(Tx tx, boolean builtinAdmin, String change,
                                       Consumer<Map<String, Member>> users, Consumer<Map<String, Set<String>>> roles) {
        if (builtinAdmin) return;
        Map<String, Member> after = new LinkedHashMap<>();
        for (Rows.AuthUser u : tx.authUsers()) after.put(u.name(), new Member(u.disabled(), tx.authRolesOf(u.name())));
        Map<String, Set<String>> perms = new LinkedHashMap<>();
        for (Rows.AuthRole r : tx.authRoles()) perms.put(r.name(), r.permissions());
        users.accept(after);
        roles.accept(perms);
        for (Member m : after.values()) {
            if (m.disabled()) continue;
            Set<String> granted = new TreeSet<>();
            for (String r : m.roles()) granted.addAll(perms.getOrDefault(r, Set.of()));
            if (Permissions.allows(granted, Permissions.USER_MANAGE, null)) return;
        }
        throw new IllegalArgumentException(change + " would leave no account that can manage users, and there is "
                + "no built-in admin to fall back on; give another account that permission first");
    }

    private void audit(Tx tx, String actor, String action, String target, String detail) {
        tx.appendAuthAudit(new Rows.AuthAudit(0, clock.getAsLong(), actor, action, target, detail));
        LOG.log(System.Logger.Level.INFO, () -> "auth: " + action + (target == null ? "" : " " + target)
                + (actor == null ? "" : " by " + actor));
    }
}
