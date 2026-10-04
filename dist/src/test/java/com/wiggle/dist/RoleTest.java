package com.wiggle.dist;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Which process the one image runs, and the old name that must keep working. */
class RoleTest {

    @Test @DisplayName("server is the default when WIGGLE_ROLE is unset or blank")
    void defaultsToServer() {
        assertSame(Role.SERVER, Role.of(null));
        assertSame(Role.SERVER, Role.of(""));
        assertSame(Role.SERVER, Role.of("   "));
    }

    @Test @DisplayName("'cell' still means server, so existing deployments keep working")
    void cellIsStillAccepted() {
        assertSame(Role.SERVER, Role.of("cell"));
        assertSame(Role.SERVER, Role.of("CELL"));
        assertSame(Role.SERVER, Role.of(" cell "));
    }

    @Test @DisplayName("both roles parse, case- and space-insensitively")
    void bothRoles() {
        assertSame(Role.SERVER, Role.of(" Server "));
        assertSame(Role.CONSOLE, Role.of("CONSOLE"));
    }

    @Test @DisplayName("the removed coordinator role is refused with a pointer to what replaces it")
    void coordinatorRoleIsRemoved() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Role.of("Coordinator"));
        assertTrue(e.getMessage().contains("removed"), e.getMessage());
    }

    @Test @DisplayName("an unrecognised role is refused, not silently run as a server")
    void unknownRoleFails() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Role.of("sever"));
        assertTrue(e.getMessage().contains("sever"), e.getMessage());
        assertTrue(e.getMessage().contains("server, console"), e.getMessage());
    }

    @Test @DisplayName("every role has a name the parser accepts back")
    void everyRoleRoundTrips() {
        for (Role r : Role.values()) {
            assertEquals(r, Role.of(r.name()), r + " must parse from its own name");
        }
    }
}
