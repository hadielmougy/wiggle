package com.wiggle.tests;

import com.wiggle.boot.test.WiggleBootTest;
import com.wiggle.boot.test.WiggleTestNode;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;


import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A {@link WiggleServer} runs the engine and serves the control plane on a bound API port. */
@WiggleBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class ServerRoleTest {

    @Autowired WiggleTestNode node;

    @Test @DisplayName("a server serves the engine on a bound API port")
    void cellServesEngine() throws Exception {
        assertNotNull(node.server().engine(), "a server exposes the engine");
        assertTrue(node.server().port() > 0, "a server binds the WiggleControlPlane port");
    }
}
