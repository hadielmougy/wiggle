package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.docs.VersioningSnippet;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The claims {@code docs/versioning.md} makes, as tests.
 *
 * <p>Versioning is the part of an engine people trust prose about and then discover differs in
 * production, so the page should not be the only thing asserting any of this. Each test here
 * corresponds to a sentence on that page: a changed topology is a new version, an unchanged one is
 * not, an in-flight instance keeps the version it started on, and an unscoped worker serves every
 * version while a scoped one serves exactly its own.
 */
class VersioningTest {

    private static ServerConfig config() {
        String url = "jdbc:h2:mem:ver-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        return new ServerConfig(0, "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    private static Map<String, Object> order() {
        return Map.of("id", "A-1", "status", "NEW", "currency", "USD");
    }

    @Test @DisplayName("the version is a hash of the topology: same graph, same version; changed graph, new one")
    void versionIsTheContentHash() {
        FlowSpec a = VersioningSnippet.v1();
        FlowSpec b = VersioningSnippet.v1();
        assertEquals(a.version(), b.version(), "the same topology defined twice is the same version");

        assertNotEquals(a.version(), VersioningSnippet.v2().version(),
                "adding a step changes the graph, so it changes the version");
    }

    @Test @DisplayName("registering an unchanged graph is a no-op; a changed one adds a version")
    void registrationIsIdempotent() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            FlowSpec v1 = VersioningSnippet.v1();
            client.register(v1);
            client.register(VersioningSnippet.v1());   // byte-identical topology -> nothing new

            FlowSpec v2 = VersioningSnippet.v2();
            client.register(v2);

            assertNotEquals(v1.version(), v2.version());
            // both remain startable: registering v2 redirects nothing
            assertTrue(client.start("orders", order(), v1.version(), "c1") != null);
            assertTrue(client.start("orders", order(), v2.version(), "c2") != null);
        }
    }

    @Test @DisplayName("an in-flight instance keeps the version it started on, whatever is registered after")
    void inFlightInstancesStayOnTheirVersion() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            FlowSpec v1 = VersioningSnippet.v1();
            client.register(v1);
            String id = client.start("orders", order(), v1.version(), "c1");

            client.register(VersioningSnippet.v2());   // deploy happens mid-flight

            // a v1 worker finishes it: the instance never moved to v2's graph
            try (Worker w = new Worker(client, "v1-" + Ids.next("x"))
                    .registerHandler(new VersioningSnippet.V1Handlers(), v1.version()).start()) {
                InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));
                assertEquals("COMPLETED", v.status());
                // v2 inserts fraudCheck between validate and charge; this instance ran v1's two steps
                assertEquals("CHARGED", Json.asObject(v.context()).get("status"));
            }
        }
    }

    @Test @DisplayName("an unscoped worker serves every version; step names are what it binds")
    void unscopedWorkerServesEveryVersion() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            FlowSpec v1 = VersioningSnippet.v1();
            FlowSpec v2 = VersioningSnippet.v2();
            client.register(v1);
            Thread.sleep(5);        // see below: "latest" ties are broken by version number
            client.register(v2);

            // An unscoped worker binds against the LATEST graph, so v2 -- whose steps are a
            // superset of v1's. That is what lets it serve both, not some per-version binding.
            try (Worker w = new Worker(client, "all-" + Ids.next("x"))
                    .registerHandler(new VersioningSnippet.V2Handlers()).start()) {

                String oldOne = client.start("orders", order(), v1.version(), "c1");
                String newOne = client.start("orders", order(), v2.version(), "c2");

                InstanceView a = client.awaitCompletion(oldOne, Duration.ofSeconds(30));
                InstanceView b = client.awaitCompletion(newOne, Duration.ofSeconds(30));
                assertEquals("COMPLETED", a.status(), "v1 instance: " + a);
                assertEquals("COMPLETED", b.status(), "v2 instance: " + b);
            }
        }
    }

    @Test @DisplayName("unscoped binding is against ONE graph: a step only the newer version has "
            + "is unbound if the worker bound the older one")
    void unscopedBindingIsAgainstTheLatestGraphOnly() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            // register them the other way round, so the LATEST graph is the one WITHOUT fraudCheck
            FlowSpec v2 = VersioningSnippet.v2();
            FlowSpec v1 = VersioningSnippet.v1();
            client.register(v2);
            Thread.sleep(5);
            client.register(v1);

            String onV2 = client.start("orders", order(), v2.version(), "c2");

            try (Worker w = new Worker(client, "all-" + Ids.next("x"))
                    .registerHandler(new VersioningSnippet.V2Handlers()).start()) {
                InstanceView v = client.awaitCompletion(onV2, Duration.ofSeconds(30));
                assertEquals("FAILED", v.status(),
                        "the worker bound v1's graph, which has no fraudCheck step, so v2's "
                        + "fraudCheck task had no handler: " + v);
                assertTrue(String.valueOf(v.error()).contains("fraudCheck"), v.error());
            }
        }
    }

    @Test @DisplayName("a scoped worker will not touch another version's work")
    void scopedWorkerClaimsOnlyItsOwnVersion() throws Exception {
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            FlowSpec v1 = VersioningSnippet.v1();
            FlowSpec v2 = VersioningSnippet.v2();
            client.register(v1);
            client.register(v2);

            String onV2 = client.start("orders", order(), v2.version(), "c2");

            // only a v1-scoped worker is running, so v2's work must sit untouched
            try (Worker w = new Worker(client, "v1only-" + Ids.next("x"))
                    .registerHandler(new VersioningSnippet.V1Handlers(), v1.version()).start()) {
                Thread.sleep(1500);
                assertEquals("RUNNING", client.instance(onV2).status(),
                        "a v1-scoped worker claimed v2's task; scoping is what makes a staged "
                        + "hand-over between services safe");
            }
        }
    }
}
