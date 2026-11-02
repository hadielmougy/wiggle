package com.wiggle.boot;

import com.wiggle.client.WiggleClient;
import com.wiggle.core.Tls;
import com.wiggle.testfixtures.TestCerts;
import com.wiggle.testfixtures.TestPorts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** {@code WIGGLE_TLS_*} on both connectors: a keystore and a truststore mean mutual TLS on the API and the portal. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class TlsTest {

    private static final int API = TestPorts.free();
    private static final int PORTAL = TestPorts.free();
    private static final TestCerts CERTS;

    static {
        try {
            CERTS = TestCerts.generate(Files.createTempDirectory("boot-tls"));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void settings(DynamicPropertyRegistry registry) {
        registry.add("WIGGLE_PORT", () -> API);
        registry.add("WIGGLE_PORTAL_PORT", () -> PORTAL);
        registry.add("WIGGLE_TLS_KEYSTORE", () -> CERTS.serverKeyStore().toString());
        registry.add("WIGGLE_TLS_KEYSTORE_PASSWORD", () -> TestCerts.PASSWORD);
        registry.add("WIGGLE_TLS_TRUSTSTORE", () -> CERTS.trustStore().toString());
        registry.add("WIGGLE_TLS_TRUSTSTORE_PASSWORD", () -> TestCerts.PASSWORD);
    }

    private static Tls.Options client(boolean withCertificate) {
        return new Tls.Options(withCertificate ? CERTS.clientKeyStore().toString() : null,
                withCertificate ? TestCerts.PASSWORD : null, CERTS.trustStore().toString(), TestCerts.PASSWORD);
    }

    @Test @DisplayName("the API takes a client with a trusted certificate, and refuses one without")
    void apiMutualTls() {
        try (WiggleClient trusted = new WiggleClient("127.0.0.1:" + API, client(true))) {
            trusted.cluster();
        }
        try (WiggleClient noCertificate = new WiggleClient("127.0.0.1:" + API, client(false))) {
            assertThrows(RuntimeException.class, noCertificate::cluster);
        }
    }

    @Test @DisplayName("the portal is served over the same TLS, and not in plaintext")
    void portalOverTls() throws Exception {
        try (HttpClient https = HttpClient.newBuilder().sslContext(Tls.sslContext(client(true))).build();
             HttpClient plain = HttpClient.newHttpClient()) {
            assertEquals(200, https.send(HttpRequest.newBuilder(URI.create("https://localhost:" + PORTAL + "/healthz")).build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode());
            assertEquals(400, plain.send(HttpRequest.newBuilder(URI.create("http://localhost:" + PORTAL + "/healthz")).build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode(), "Tomcat answers plaintext on a TLS port with 400");
        }
    }
}
