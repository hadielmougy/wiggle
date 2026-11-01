package com.wiggle.server.auth;

import com.wiggle.server.store.Repositories;
import com.wiggle.server.store.ShardTransactions;
import com.wiggle.core.Json;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;

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
    private final ShardTransactions shards;
    private final LongSupplier clock;

    public Accounts(Storage storage, LongSupplier clock) {
        this.storage = storage;
        this.shards = new ShardTransactions(storage);
        this.clock = clock;
    }

    /** Writes the built-in roles where they are missing or differ. */
    public void bootstrap() {
        shards.inAuth(() -> {
            long now = clock.getAsLong();
            Map<String, Rows.AuthRole> existing = rolesByName();
            Permissions.BUILTIN_ROLES.forEach((name, perms) -> {
                Rows.AuthRole r = existing.get(name);
                if (r != null && r.builtin() && r.permissions().equals(perms)) return;
                Repositories.auth().putAuthRole(new Rows.AuthRole(name, perms, true, r == null ? now : r.createdAt(), now));
                audit(null, "role.put", name, String.join(" ", new TreeSet<>(perms)));
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
        return shards.inAuth(() -> {
            if (Repositories.auth().authAuditHas(IMPORTED)) return 0;
            Set<String> roles = rolesByName().keySet();
            int imported = 0;
            for (Object entry : entries) {
                Map<String, Object> m = Json.asObject(entry);
                String name = Json.reqStr(m, "name");
                if (reserved.contains(name) || Repositories.auth().findAuthUser(name).isPresent()) continue;
                String role = Json.str(m, "role", Permissions.VIEWER).trim().toLowerCase();
                if (role.equals("operator")) role = Permissions.ADMIN;
                if (!roles.contains(role)) role = Permissions.VIEWER;
                long now = clock.getAsLong();
                Repositories.auth().putAuthUser(new Rows.AuthUser(name, Json.reqStr(m, "hash"), Json.reqStr(m, "salt"),
                        (int) Json.num(m, "iterations", Passwords.ITERATIONS), false,
                        Json.num(m, "createdAt", now), Json.num(m, "updatedAt", now)));
                Repositories.auth().setAuthRolesOf(name, List.of(role));
                audit(null, "user.create", name, "imported from " + file.getFileName());
                imported++;
            }
            audit(null, IMPORTED, null, imported + " account(s) from " + file);
            return imported;
        });
    }

    /** Whether no account exists yet. */
    public boolean isEmpty() {
        return shards.inAuth(() -> Repositories.auth().authUsers().isEmpty());
    }

    public List<User> users() {
        return shards.inAuth(() -> Repositories.auth().authUsers().stream()
                .map(u -> new User(u.name(), Repositories.auth().authRolesOf(u.name()), u.disabled(), u.createdAt(), u.updatedAt()))
                .toList());
    }

    public List<Rows.AuthRole> roles() {
        return shards.inAuth(() -> Repositories.auth().authRoles());
    }

    /** The account {@code name}, read from the auth primary. */
    public Optional<Account> account(String name) {
        return shards.inAuth(() -> Repositories.auth().findAuthUser(name).map(u -> new Account(u.name(), permissionsOf(u.name()),
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
        shards.inAuth(() -> {
            if (Repositories.auth().findAuthUser(name).isPresent()) throw new IllegalArgumentException("user '" + name + "' already exists");
            requireRoles(roles);
            requireManager(builtinAdmin, "creating '" + name + "' with roles " + roles,
                    users -> users.put(name, new Member(false, roles)), r -> { });
            long now = clock.getAsLong();
            Repositories.auth().putAuthUser(new Rows.AuthUser(name, h.hash(), h.salt(), h.iterations(), false, now, now));
            Repositories.auth().setAuthRolesOf(name, roles);
            audit(actor, "user.create", name, String.join(" ", roles));
            return null;
        });
    }

    /** Replaces an account's password and ends its sessions, except {@code keepToken} when non-null. */
    public void setPassword(String actor, String name, String password, String keepToken) {
        Passwords.requirePassword(password);
        Passwords.Hashed h = Passwords.hash(password);
        shards.inAuth(() -> {
            Rows.AuthUser u = require(name);
            Repositories.auth().putAuthUser(new Rows.AuthUser(name, h.hash(), h.salt(), h.iterations(), u.disabled(), u.createdAt(),
                    clock.getAsLong()));
            Repositories.auth().deleteAuthSessionsOf(name, keepToken == null ? null : tokenHash(keepToken));
            audit(actor, "user.password", name, null);
            return null;
        });
    }

    public void setRoles(String actor, String name, List<String> roles, boolean builtinAdmin) {
        shards.inAuth(() -> {
            Rows.AuthUser u = require(name);
            requireRoles(roles);
            requireManager(builtinAdmin, "giving '" + name + "' roles " + roles,
                    users -> users.put(name, new Member(u.disabled(), roles)), r -> { });
            Repositories.auth().setAuthRolesOf(name, roles);
            audit(actor, "user.roles", name, String.join(" ", roles));
            return null;
        });
    }

    /** Disables or enables an account; disabling also ends its sessions. */
    public void setDisabled(String actor, String name, boolean disabled, boolean builtinAdmin) {
        shards.inAuth(() -> {
            Rows.AuthUser u = require(name);
            requireManager(builtinAdmin, "disabling '" + name + "'",
                    users -> users.put(name, new Member(disabled, Repositories.auth().authRolesOf(name))), r -> { });
            Repositories.auth().putAuthUser(new Rows.AuthUser(name, u.hash(), u.salt(), u.iterations(), disabled, u.createdAt(),
                    clock.getAsLong()));
            if (disabled) Repositories.auth().deleteAuthSessionsOf(name, null);
            audit(actor, disabled ? "user.disable" : "user.enable", name, null);
            return null;
        });
    }

    public void delete(String actor, String name, boolean builtinAdmin) {
        shards.inAuth(() -> {
            require(name);
            requireManager(builtinAdmin, "deleting '" + name + "'", users -> users.remove(name), r -> { });
            Repositories.auth().deleteAuthUser(name);
            audit(actor, "user.delete", name, null);
            return null;
        });
    }

    /** Creates or replaces a role. A built-in role cannot be changed. */
    public void putRole(String actor, String name, Collection<String> permissions, boolean builtinAdmin) {
        Passwords.requireName(name);
        Set<String> perms = Permissions.validate(permissions);
        if (perms.isEmpty()) throw new IllegalArgumentException("a role grants at least one permission");
        shards.inAuth(() -> {
            Rows.AuthRole existing = rolesByName().get(name);
            if (existing != null && existing.builtin()) {
                throw new IllegalArgumentException("'" + name + "' is a built-in role and cannot be changed");
            }
            requireManager(builtinAdmin, "changing role '" + name + "'", u -> { }, r -> r.put(name, perms));
            long now = clock.getAsLong();
            Repositories.auth().putAuthRole(new Rows.AuthRole(name, perms, false, existing == null ? now : existing.createdAt(), now));
            audit(actor, "role.put", name, String.join(" ", new TreeSet<>(perms)));
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
        shards.inAuth(() -> {
            if (Repositories.auth().authCredentials().stream().anyMatch(x -> x.id().equals(c.id()))) {
                throw new IllegalArgumentException("credential '" + c.id() + "' already exists");
            }
            if (c.subject() != null && Repositories.auth().findAuthCredentialBySubject(c.subject()).isPresent()) {
                throw new IllegalArgumentException("a credential for subject '" + c.subject() + "' already exists");
            }
            requireRoles(List.of(c.role()));
            Repositories.auth().insertAuthCredential(c);
            audit(actor, "credential.create", c.id(), c.kind() + " " + c.role());
            return null;
        });
    }

    /** Every machine credential; a key's hash is the only trace of it. */
    public List<Rows.AuthCredential> credentials() {
        return shards.inAuth(() -> Repositories.auth().authCredentials());
    }

    public void deleteCredential(String actor, String id) {
        shards.inAuth(() -> {
            if (!Repositories.auth().deleteAuthCredential(id)) throw new IllegalArgumentException("no credential '" + id + "'");
            audit(actor, "credential.delete", id, null);
            return null;
        });
    }

    /** The API key credential whose key hashes to {@code keyHash}, read from the auth primary. */
    public Optional<Machine> machineByKeyHash(String keyHash) {
        return shards.inAuth(() -> Repositories.auth().findAuthCredentialByKeyHash(keyHash).map(c -> machine(c)));
    }

    /** The credential for client certificate subject {@code subject}, read from the auth primary. */
    public Optional<Machine> machineBySubject(String subject) {
        return shards.inAuth(() -> Repositories.auth().findAuthCredentialBySubject(subject).map(c -> machine(c)));
    }

    private static Machine machine(Rows.AuthCredential c) {
        Rows.AuthRole role = rolesByName().get(c.role());
        return new Machine(c.id(), role == null ? Set.of() : role.permissions(), c.expiresAt());
    }

    /** Deletes a role and every grant of it. A built-in role cannot be deleted. */
    public void deleteRole(String actor, String name, boolean builtinAdmin) {
        shards.inAuth(() -> {
            Rows.AuthRole existing = rolesByName().get(name);
            if (existing == null) throw new IllegalArgumentException("no role '" + name + "'");
            if (existing.builtin()) throw new IllegalArgumentException("'" + name + "' is a built-in role and cannot be deleted");
            List<String> bound = Repositories.auth().authCredentials().stream().filter(c -> c.role().equals(name))
                    .map(Rows.AuthCredential::id).toList();
            if (!bound.isEmpty()) {
                throw new IllegalArgumentException("role '" + name + "' is held by credential(s) " + bound
                        + "; delete them first");
            }
            requireManager(builtinAdmin, "deleting role '" + name + "'", u -> { }, r -> r.remove(name));
            Repositories.auth().deleteAuthRole(name);
            audit(actor, "role.delete", name, null);
            return null;
        });
    }

    /** Opens a session for {@code user} and returns its token; only the token's hash is stored. */
    public String openSession(String user, long ttlMillis) {
        byte[] bytes = new byte[32];
        RANDOM.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        shards.inAuth(() -> {
            long now = clock.getAsLong();
            Repositories.auth().deleteExpiredAuthSessions(now, 100);
            Repositories.auth().insertAuthSession(new Rows.AuthSession(tokenHash(token), user, now + ttlMillis, now));
            return null;
        });
        return token;
    }

    /** The session stored under {@code idHash}, read from the auth primary. */
    public Optional<Rows.AuthSession> session(String idHash) {
        return shards.inAuth(() -> Repositories.auth().findAuthSession(idHash));
    }

    /** Ends the session of {@code token} and records it, so every node drops it. */
    public void closeSession(String token) {
        String hash = tokenHash(token);
        shards.inAuth(() -> {
            Optional<Rows.AuthSession> s = Repositories.auth().findAuthSession(hash);
            if (s.isEmpty()) return null;
            Repositories.auth().deleteAuthSession(hash);
            audit(s.get().user(), "session.close", s.get().user(), hash);
            return null;
        });
    }

    /** Up to {@code max} audit entries after {@code afterSeq}, oldest first. */
    public List<Rows.AuthAudit> auditAfter(long afterSeq, int max) {
        return shards.inAuth(() -> Repositories.auth().authAuditAfter(afterSeq, max));
    }

    public long auditHead() {
        return shards.inAuth(() -> Repositories.auth().authAuditHead());
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

    private static Rows.AuthUser require(String name) {
        return Repositories.auth().findAuthUser(name).orElseThrow(() -> new IllegalArgumentException("no user '" + name + "'"));
    }

    private static void requireRoles(List<String> roles) {
        if (roles.isEmpty()) throw new IllegalArgumentException("an account holds at least one role");
        Set<String> known = rolesByName().keySet();
        for (String r : roles) {
            if (!known.contains(r)) throw new IllegalArgumentException("no role '" + r + "'; the roles are " + known);
        }
    }

    private static Map<String, Rows.AuthRole> rolesByName() {
        Map<String, Rows.AuthRole> out = new LinkedHashMap<>();
        for (Rows.AuthRole r : Repositories.auth().authRoles()) out.put(r.name(), r);
        return out;
    }

    private static Set<String> permissionsOf(String user) {
        Map<String, Rows.AuthRole> roles = rolesByName();
        Set<String> out = new TreeSet<>();
        for (String r : Repositories.auth().authRolesOf(user)) {
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
    private static void requireManager(boolean builtinAdmin, String change,
                                       Consumer<Map<String, Member>> users, Consumer<Map<String, Set<String>>> roles) {
        if (builtinAdmin) return;
        Map<String, Member> after = new LinkedHashMap<>();
        for (Rows.AuthUser u : Repositories.auth().authUsers()) after.put(u.name(), new Member(u.disabled(), Repositories.auth().authRolesOf(u.name())));
        Map<String, Set<String>> perms = new LinkedHashMap<>();
        for (Rows.AuthRole r : Repositories.auth().authRoles()) perms.put(r.name(), r.permissions());
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

    private void audit(String actor, String action, String target, String detail) {
        Repositories.auth().appendAuthAudit(new Rows.AuthAudit(0, clock.getAsLong(), actor, action, target, detail));
        LOG.log(System.Logger.Level.INFO, () -> "auth: " + action + (target == null ? "" : " " + target)
                + (actor == null ? "" : " by " + actor));
    }
}
