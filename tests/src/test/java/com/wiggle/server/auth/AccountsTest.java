package com.wiggle.server.auth;

import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Accounts, roles and sessions on the auth shard, and the rules that keep someone able to manage them. */
class AccountsTest {

    private final AtomicLong clock = new AtomicLong(1_000_000);

    private Accounts accounts(Storage storage) {
        storage.migrate();
        Accounts a = new Accounts(storage, clock::get);
        a.bootstrap();
        return a;
    }

    private static List<String> actions(Accounts a) {
        return a.auditAfter(0, 1000).stream().map(Rows.AuthAudit::action).toList();
    }

    @Test @DisplayName("the built-in roles exist after bootstrap, once, and cannot be changed or deleted")
    void builtinRoles() {
        Accounts a = accounts(new InMemoryStorage());
        a.bootstrap();
        assertEquals(List.of("admin", "viewer"), a.roles().stream().map(Rows.AuthRole::name).toList());
        assertEquals(Set.of("*"), a.roles().getFirst().permissions());
        assertEquals(2, actions(a).size(), "a second bootstrap writes nothing");
        assertThrows(IllegalArgumentException.class, () -> a.putRole("x", "admin", List.of("read"), true));
        assertThrows(IllegalArgumentException.class, () -> a.deleteRole("x", "viewer", true));
    }

    @Test @DisplayName("an account signs in with its password, holds its roles' permissions, and every change is audited")
    void accountLifecycle() {
        Accounts a = accounts(new InMemoryStorage());
        a.putRole("root", "ops", List.of("read", "instance.cancel:orders"), true);
        a.create("root", "dana", "dana-password", List.of("ops"), Set.of(), true);

        Accounts.Account dana = a.account("dana").orElseThrow();
        assertTrue(dana.passwordMatches("dana-password"));
        assertFalse(dana.passwordMatches("wrong"));
        assertEquals(Set.of("read", "instance.cancel:orders"), dana.permissions());

        a.setRoles("root", "dana", List.of("ops", "viewer"), true);
        a.setPassword("root", "dana", "new-password", null);
        a.setDisabled("root", "dana", true, true);
        assertTrue(a.account("dana").orElseThrow().disabled());
        assertTrue(a.account("dana").orElseThrow().passwordMatches("new-password"));
        a.delete("root", "dana", true);
        assertTrue(a.account("dana").isEmpty());

        assertEquals(List.of("role.put", "role.put", "role.put", "user.create", "user.roles", "user.password",
                "user.disable", "user.delete"), actions(a));
        assertEquals("root", a.auditAfter(0, 1000).getLast().actor());
    }

    @Test @DisplayName("a role grants only known permissions; an account holds only existing roles")
    void validation() {
        Accounts a = accounts(new InMemoryStorage());
        assertThrows(IllegalArgumentException.class, () -> a.putRole("x", "bad", List.of("instance.delete"), true));
        assertThrows(IllegalArgumentException.class, () -> a.putRole("x", "bad", List.of("user.manage:dana"), true),
                "user.manage takes no scope");
        assertThrows(IllegalArgumentException.class, () -> a.putRole("x", "bad", List.of(), true));
        assertThrows(IllegalArgumentException.class,
                () -> a.create("x", "dana", "dana-password", List.of("nope"), Set.of(), true));
        assertThrows(IllegalArgumentException.class,
                () -> a.create("x", "dana", "short", List.of("viewer"), Set.of(), true));
        assertThrows(IllegalArgumentException.class,
                () -> a.create("x", "admin", "long-enough", List.of("viewer"), Set.of("admin"), true));
        a.create("x", "dana", "dana-password", List.of("viewer"), Set.of(), true);
        assertThrows(IllegalArgumentException.class,
                () -> a.create("x", "dana", "dana-password", List.of("viewer"), Set.of(), true));
    }

    @Test @DisplayName("with no built-in admin, nothing may leave the accounts without one that can manage users")
    void someoneCanAlwaysManage() {
        Accounts a = accounts(new InMemoryStorage());
        assertThrows(IllegalArgumentException.class,
                () -> a.create(null, "rey", "rey-password", List.of("viewer"), Set.of(), false),
                "a first account that cannot manage users would lock everyone out");
        a.create(null, "dana", "dana-password", List.of("admin"), Set.of(), false);
        a.create("dana", "rey", "rey-password", List.of("viewer"), Set.of(), false);

        assertThrows(IllegalArgumentException.class, () -> a.delete("dana", "dana", false));
        assertThrows(IllegalArgumentException.class, () -> a.setDisabled("dana", "dana", true, false));
        assertThrows(IllegalArgumentException.class, () -> a.setRoles("dana", "dana", List.of("viewer"), false));
        assertTrue(a.account("dana").orElseThrow().permissions().contains("*"), "a refused change wrote nothing");

        a.putRole("dana", "managers", List.of("user.manage"), false);
        a.setRoles("dana", "rey", List.of("managers"), false);
        a.delete("rey", "dana", false);
        assertThrows(IllegalArgumentException.class, () -> a.deleteRole("rey", "managers", false),
                "rey is the last manager, and only through that role");
        assertThrows(IllegalArgumentException.class,
                () -> a.putRole("rey", "managers", List.of("read"), false), "nor may the role lose the permission");
        a.deleteRole("rey", "managers", true);
    }

    @Test @DisplayName("sessions store only a hash of their token, and end on logout and on a password change")
    void sessions() {
        Accounts a = accounts(new InMemoryStorage());
        a.create(null, "dana", "dana-password", List.of("admin"), Set.of(), true);
        String one = a.openSession("dana", 60_000);
        String two = a.openSession("dana", 60_000);
        assertTrue(a.session(one).isEmpty(), "a token is not its own id");
        Rows.AuthSession s = a.session(Accounts.tokenHash(one)).orElseThrow();
        assertEquals("dana", s.user());
        assertEquals(clock.get() + 60_000, s.expiresAt());

        a.setPassword("dana", "dana", "new-password", one);
        assertTrue(a.session(Accounts.tokenHash(one)).isPresent(), "the session that changed it stays");
        assertTrue(a.session(Accounts.tokenHash(two)).isEmpty(), "every other one ends");
        a.closeSession(one);
        assertTrue(a.session(Accounts.tokenHash(one)).isEmpty());
        assertEquals("session.close", actions(a).getLast());
    }

    @Test @DisplayName("an API key is shown once and stored as a hash; a certificate credential names its subject")
    void credentials() {
        Accounts a = accounts(new InMemoryStorage());
        a.putRole("ops", "worker", List.of("task.poll"), true);
        String key = a.createApiKey("ops", "w1", "worker", null);
        assertTrue(key.startsWith(Accounts.KEY_PREFIX));
        Rows.AuthCredential stored = a.credentials().getFirst();
        assertEquals(Accounts.tokenHash(key), stored.keyHash());
        assertEquals(Set.of("task.poll"), a.machineByKeyHash(Accounts.tokenHash(key)).orElseThrow().permissions());

        a.createCertificate("ops", "c1", "CN=worker", "viewer", 5L);
        Accounts.Machine m = a.machineBySubject("CN=worker").orElseThrow();
        assertEquals(Set.of("read"), m.permissions());
        assertTrue(m.expired(5));
        assertFalse(m.expired(4));

        assertThrows(IllegalArgumentException.class, () -> a.createApiKey("ops", "w1", "worker", null), "an id is unique");
        assertThrows(IllegalArgumentException.class, () -> a.createCertificate("ops", "c2", "CN=worker", "viewer", null));
        assertThrows(IllegalArgumentException.class, () -> a.createApiKey("ops", "w2", "nope", null), "the role must exist");
        assertThrows(IllegalArgumentException.class, () -> a.deleteRole("ops", "worker", true),
                "a role a credential holds is not deleted from under it");
        a.deleteCredential("ops", "w1");
        a.deleteRole("ops", "worker", true);
        assertEquals(List.of("credential.create", "credential.create", "credential.delete", "role.delete"),
                actions(a).subList(actions(a).size() - 4, actions(a).size()));
    }

    @Test @DisplayName("a console users file is imported once with its hashes, skipping built-in names")
    void importsTheUsersFileOnce(@TempDir Path dir) throws Exception {
        Passwords.Hashed h = Passwords.hash("dana-password");
        Path file = dir.resolve("wiggle-users.json");
        Files.writeString(file, "{\"users\":[{\"name\":\"dana\",\"role\":\"operator\",\"salt\":\"" + h.salt()
                + "\",\"hash\":\"" + h.hash() + "\",\"iterations\":" + h.iterations() + ",\"createdAt\":5,\"updatedAt\":6},"
                + "{\"name\":\"rey\",\"role\":\"viewer\",\"salt\":\"" + h.salt() + "\",\"hash\":\"" + h.hash()
                + "\",\"iterations\":" + h.iterations() + "},"
                + "{\"name\":\"admin\",\"role\":\"admin\",\"salt\":\"x\",\"hash\":\"y\",\"iterations\":1}]}");
        Accounts a = accounts(new InMemoryStorage());

        assertEquals(2, a.importFile(file, Set.of("admin")));
        Accounts.Account dana = a.account("dana").orElseThrow();
        assertTrue(dana.passwordMatches("dana-password"), "the hash came over as it was");
        assertEquals(Set.of("*"), dana.permissions(), "'operator' is the old name for admin");
        assertEquals(List.of("viewer"), a.users().get(1).roles());
        assertTrue(a.account("admin").isEmpty(), "a built-in name is not imported");
        assertEquals(5, a.users().getFirst().createdAt());

        a.delete(null, "rey", true);
        assertEquals(0, a.importFile(file, Set.of("admin")), "a second start imports nothing");
        assertTrue(a.account("rey").isEmpty(), "so a deleted account does not come back");
    }
}
