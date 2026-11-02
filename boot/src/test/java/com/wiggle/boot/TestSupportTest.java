package com.wiggle.boot;

import com.wiggle.boot.test.WiggleBootTest;
import com.wiggle.boot.test.WiggleTestNode;
import com.wiggle.client.WiggleClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** What {@code @WiggleBootTest} and its profiles give a test. */
class TestSupportTest {

    private static int get(int port, String path) throws Exception {
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode();
        }
    }

    @Nested @WiggleBootTest
    class Defaults {
        @Autowired WiggleTestNode node;

        @Test @DisplayName("one node on H2 in memory with the fast test timings, no portal and no probe")
        void defaults() {
            assertTrue(node.config().isInMemory());
            assertEquals(Duration.ofMillis(500), node.config().maxLongPoll());
            assertEquals(Duration.ofMillis(100), node.config().pollInterval());
            assertEquals(0, node.portalPort());
            assertEquals(0, node.probePort());
            try (WiggleClient client = node.client()) {
                client.cluster();
            }
        }
    }

    @Nested @WiggleBootTest @ActiveProfiles({"portal", "probe"})
    class PortalAndProbe {
        @Autowired WiggleTestNode node;

        @Test @DisplayName("the portal and probe profiles serve those on ports of their own")
        void portsOfTheirOwn() throws Exception {
            assertNotEquals(0, node.portalPort());
            assertNotEquals(0, node.probePort());
            assertEquals(200, get(node.portalPort(), "/healthz"));
            assertEquals(200, get(node.probePort(), "/healthz"));
        }
    }

    @Nested @WiggleBootTest @ActiveProfiles("postgres")
    @EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
    class OnPostgres {
        @Autowired WiggleTestNode node;

        @Test @DisplayName("the postgres profile runs the node on WIGGLE_TEST_PG_URL")
        void onTheLiveDatabase() {
            assertEquals(System.getenv("WIGGLE_TEST_PG_URL"), node.config().jdbcUrl());
            try (WiggleClient client = node.client()) {
                client.cluster();
            }
        }
    }
}
