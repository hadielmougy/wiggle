package com.wiggle.testfixtures;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPortsTest {

    /** A listener another process might hold: {@code SO_REUSEADDR} on, as Java and gRPC set it. */
    private static ServerSocket hold(InetSocketAddress at) throws Exception {
        ServerSocket s = new ServerSocket();
        s.setReuseAddress(true);
        s.bind(at);
        return s;
    }

    @Test
    @DisplayName("a port held only on 127.0.0.1 is not handed out, though a wildcard bind would succeed")
    void loopbackHolderIsSeen() throws Exception {
        int port = TestPorts.free();
        try (ServerSocket ignored = hold(new InetSocketAddress(InetAddress.getLoopbackAddress(), port))) {
            assertFalse(TestPorts.bindable(port));
        }
    }

    @Test
    @DisplayName("a port held on the wildcard address, as a gRPC server holds it, is not handed out")
    void wildcardHolderIsSeen() throws Exception {
        int port = TestPorts.free();
        try (ServerSocket ignored = hold(new InetSocketAddress(port))) {
            assertFalse(TestPorts.bindable(port));
        }
    }

    @Test
    @DisplayName("free() returns a port both binds accept, and never the same one twice in a row")
    void freeIsBindable() {
        int a = TestPorts.free();
        int b = TestPorts.free();
        assertTrue(TestPorts.bindable(a));
        assertTrue(a != b);
    }
}
