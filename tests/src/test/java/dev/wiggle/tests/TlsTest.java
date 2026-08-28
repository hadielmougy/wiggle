package dev.wiggle.tests;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.core.Tls;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end TLS: real keystores are generated with {@code keytool}, then the gRPC API and HTTP
 * dashboard are driven over TLS -- server-side TLS, mutual TLS, and the plaintext fallback that
 * every other test relies on (which holds because {@code WIGGLE_TLS_*} is unset in this JVM).
 */
class TlsTest {

    private static final String STORE = "storepass";

    @TempDir static Path dir;
    private static Path serverKs, clientKs, trust;

    private static final Blueprint<Map<String, Object>> BP =
            Workflow.define("tls-wf").step("work", ctx -> ctx).build();

    @BeforeAll
    static void generateCerts() throws Exception {
        Assumptions.assumeTrue(Files.isExecutable(Path.of(keytool())), "keytool must be available");
        serverKs = dir.resolve("server.p12");
        clientKs = dir.resolve("client.p12");
        trust = dir.resolve("trust.p12");
        Path serverCrt = dir.resolve("server.crt");
        Path clientCrt = dir.resolve("client.crt");

        // Server identity is reachable as both localhost and 127.0.0.1 (baseUrl() uses the IP).
        genKeypair(serverKs, "server", "CN=localhost", "SAN=dns:localhost,ip:127.0.0.1");
        genKeypair(clientKs, "client", "CN=wiggle-client", null);
        exportCert(serverKs, "server", serverCrt);
        exportCert(clientKs, "client", clientCrt);
        importCert(trust, "server", serverCrt);   // one truststore trusts both identities
        importCert(trust, "client", clientCrt);
    }

    // ---------------------------------------------------------------- gRPC

    @Test @DisplayName("gRPC server-side TLS: a trusting client works, a plaintext client is refused")
    void grpcServerTls() throws Exception {
        ServerConfig config = serverConfig(opts(serverKs, null), 0, null);   // keystore only
        try (WiggleServer server = new WiggleServer(config).start()) {
            Tls.Options clientTls = opts(null, trust);   // trusts the server, no client cert

            try (WiggleClient client = new WiggleClient(server.baseUrl(), clientTls);
                 Worker w = new Worker(client, "tls-w").register(BP)) {
                w.start();
                String id = client.start(BP, Map.of());
                assertEquals("COMPLETED", client.awaitCompletion(id, Duration.ofSeconds(20)).status());
            }

            // A plaintext client (no TLS configured) cannot talk to a TLS endpoint.
            try (WiggleClient plaintext = new WiggleClient(server.baseUrl(), Tls.Options.DISABLED)) {
                assertThrows(RuntimeException.class, plaintext::cluster);
            }
        }
    }

    @Test @DisplayName("gRPC mTLS: a client with a trusted cert works, one without a cert is rejected")
    void grpcMutualTls() throws Exception {
        ServerConfig config = serverConfig(opts(serverKs, trust), 0, null);   // keystore + truststore => mTLS
        try (WiggleServer server = new WiggleServer(config).start()) {

            try (WiggleClient client = new WiggleClient(server.baseUrl(), opts(clientKs, trust));   // presents a cert
                 Worker w = new Worker(client, "mtls-w").register(BP)) {
                w.start();
                String id = client.start(BP, Map.of());
                assertEquals("COMPLETED", client.awaitCompletion(id, Duration.ofSeconds(20)).status());
            }

            // Trusts the server but presents no client certificate -> the server rejects the handshake.
            try (WiggleClient noCert = new WiggleClient(server.baseUrl(), opts(null, trust))) {
                assertThrows(RuntimeException.class, noCert::cluster);
            }
        }
    }

    // ---------------------------------------------------------------- HTTP dashboard

    @Test @DisplayName("HTTPS dashboard: served over TLS, Basic auth still enforced, /healthz open")
    void httpsDashboard() throws Exception {
        int dash = freePort();
        ServerConfig config = serverConfig(opts(serverKs, null), dash, "s3cret");
        try (WiggleServer server = new WiggleServer(config).start()) {
            String base = "https://localhost:" + server.dashboardPort();
            HttpClient https = HttpClient.newBuilder().sslContext(clientSsl()).build();

            HttpResponse<String> health = send(https, base + "/healthz", null);
            assertEquals(200, health.statusCode(), "healthz open over HTTPS");
            assertEquals("ok", health.body());

            assertEquals(401, send(https, base + "/api/instances", null).statusCode(),
                    "auth still enforced under TLS");
            HttpResponse<String> authed = send(https, base + "/api/cluster", "admin:s3cret");
            assertEquals(200, authed.statusCode());
            assertTrue(authed.body().contains("\"members\""));
        }
    }

    // ---------------------------------------------------------------- helpers

    private static Tls.Options opts(Path keystore, Path truststore) {
        return new Tls.Options(
                keystore == null ? null : keystore.toString(), keystore == null ? null : STORE,
                truststore == null ? null : truststore.toString(), truststore == null ? null : STORE);
    }

    private static ServerConfig serverConfig(Tls.Options tls, int dashboardPort, String dashboardPassword) {
        return new ServerConfig(0, "tls-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, dashboardPort,
                Duration.ofSeconds(5), Duration.ofSeconds(10), "admin", dashboardPassword, tls);
    }

    private static SSLContext clientSsl() {
        return Tls.sslContext(opts(null, trust));   // trusts our CA, presents no client cert
    }

    private static HttpResponse<String> send(HttpClient c, String url, String basic) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).GET();
        if (basic != null) {
            b.header("Authorization", "Basic " + Base64.getEncoder()
                    .encodeToString(basic.getBytes(StandardCharsets.UTF_8)));
        }
        return c.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }

    // ---- keytool wrappers ----

    private static String keytool() {
        return System.getProperty("java.home") + "/bin/keytool";
    }

    private static void genKeypair(Path ks, String alias, String dname, String san) throws Exception {
        List<String> args = new ArrayList<>(List.of(
                "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                "-dname", dname, "-keystore", ks.toString(), "-storetype", "PKCS12",
                "-storepass", STORE, "-keypass", STORE));
        if (san != null) { args.add("-ext"); args.add(san); }
        keytool(args);
    }

    private static void exportCert(Path ks, String alias, Path out) throws Exception {
        keytool(List.of("-exportcert", "-rfc", "-alias", alias, "-keystore", ks.toString(),
                "-storepass", STORE, "-file", out.toString()));
    }

    private static void importCert(Path trustStore, String alias, Path cert) throws Exception {
        keytool(List.of("-importcert", "-noprompt", "-alias", alias, "-file", cert.toString(),
                "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", STORE));
    }

    private static void keytool(List<String> args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(keytool());
        cmd.addAll(args);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) throw new IllegalStateException("keytool failed: " + args + "\n" + out);
    }
}
