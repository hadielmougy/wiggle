package com.wiggle.server.auth;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Permissions as roles hold them: exact and prefix scopes, and what a permission may be written as. */
class PermissionsTest {

    @Test @DisplayName("a prefix scope covers every name under it, and nothing beside it")
    void prefixScope() {
        Set<String> granted = Set.of("read:acme.*");
        assertTrue(Permissions.allows(granted, "read", "acme.orders"));
        assertTrue(Permissions.allows(granted, "read", "acme.eu.orders"), "deeper names are under the prefix too");
        assertFalse(Permissions.allows(granted, "read", "acme"), "the prefix itself, with no dot, is not under it");
        assertFalse(Permissions.allows(granted, "read", "acmex.orders"));
        assertFalse(Permissions.allows(granted, "read", "globex.orders"));
        assertFalse(Permissions.allows(granted, "read", null), "a prefix is not every name");
        assertFalse(Permissions.allows(granted, "instance.start", "acme.orders"), "only the action it names");
        assertTrue(Permissions.allows(Set.of("read:acme.eu.*"), "read", "acme.eu.orders"));
        assertFalse(Permissions.allows(Set.of("read:acme.eu.*"), "read", "acme.us.orders"));
    }

    @Test @DisplayName("a scope gathers a role's exact names and prefixes for one action")
    void scope() {
        Scope s = Permissions.readable(Set.of("read:acme.*", "read:shared", "instance.start:globex.*"));
        assertTrue(s.matches("acme.orders"));
        assertTrue(s.matches("shared"));
        assertFalse(s.matches("acme"));
        assertFalse(s.matches("acmex.orders"));
        assertFalse(s.matches("globex.orders"), "another action's scope reads nothing");
        assertTrue(Permissions.readable(Set.of("read")).all());
        assertTrue(Permissions.readable(Set.of("*")).all());
        assertTrue(Permissions.readable(Set.of("task.poll")).isEmpty());
        assertEquals(Scope.of("acme.orders"), s.narrow("acme.orders"));
        assertTrue(s.narrow("globex.orders").isEmpty());
        assertEquals(s, s.narrow(null));
    }

    @Test @DisplayName("a scope may end in .* and hold no other *")
    void validate() {
        assertEquals(Set.of("read:acme.*", "task.poll:acme.gpu"), Permissions.validate(List.of("read:acme.*", "task.poll:acme.gpu")));
        for (String bad : List.of("read:*", "read:a*b", "read:.*", "read:acme*", "read:*.acme", "read:acme.*.*")) {
            assertThrows(IllegalArgumentException.class, () -> Permissions.validate(List.of(bad)), bad);
        }
    }
}
