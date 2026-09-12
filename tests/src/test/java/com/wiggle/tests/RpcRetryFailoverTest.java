package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Client/worker RPCs retry on UNAVAILABLE, so an operation issued while the cell is momentarily
 * gone — a restart or an active/passive failover — succeeds once it comes back, instead of failing.
 */
class RpcRetryFailoverTest {

    @AfterEach void clear() {
        System.clearProperty("wiggle.rpc.maxAttempts");
        System.clearProperty("wiggle.rpc.retryDelayMillis");
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }

    private static ServerConfig config(int port) {
        return new ServerConfig(port, "failover", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(30),
                Duration.ofMillis(200), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @Timeout(30)
    @DisplayName("a call issued while the cell is down rides out the outage and succeeds once it returns")
    void ridesOutRescheduling() throws Exception {
        int port = freePort();
        Blueprint bp = Workflow.define("wf").step("a").build();
        System.setProperty("wiggle.rpc.maxAttempts", "60");
        System.setProperty("wiggle.rpc.retryDelayMillis", "150");

        AtomicReference<WiggleServer> server = new AtomicReference<>();
        Thread late = new Thread(() -> {
            try {
                Thread.sleep(700);                              // the cell is absent for a beat...
                server.set(new WiggleServer(config(port)).start());  // ...then a node takes over the address
            } catch (Exception e) { throw new RuntimeException(e); }
        });
        try (WiggleClient client = new WiggleClient("localhost:" + port)) {
            late.start();
            client.register(bp);                                // issued while DOWN; retry must ride it out
            assertTrue(client.workflowNames().contains("wf"), "registration landed after the cell returned");
        } finally {
            late.join(5_000);
            if (server.get() != null) server.get().close();
        }
    }

    @Test @Timeout(30)
    @DisplayName("retry gives up cleanly when the cell never returns")
    void exhaustsWhenNeverUp() throws Exception {
        int port = freePort();   // nothing ever listens here
        System.setProperty("wiggle.rpc.maxAttempts", "3");
        System.setProperty("wiggle.rpc.retryDelayMillis", "50");
        try (WiggleClient client = new WiggleClient("localhost:" + port)) {
            WiggleClient.WiggleApiException e = assertThrows(WiggleClient.WiggleApiException.class,
                    () -> client.workflowNames());
            assertEquals(0, e.status(), "transient/unavailable maps to status 0");
            assertTrue(e.getMessage().contains("after 3 attempts"), e.getMessage());
        }
    }

    @Test @Timeout(30)
    @DisplayName("maxAttempts=1 disables retry (fails fast against a down cell)")
    void disabled() throws Exception {
        int port = freePort();
        System.setProperty("wiggle.rpc.maxAttempts", "1");
        try (WiggleClient client = new WiggleClient("localhost:" + port)) {
            assertThrows(WiggleClient.WiggleApiException.class, () -> client.workflowNames());
        }
    }
}
