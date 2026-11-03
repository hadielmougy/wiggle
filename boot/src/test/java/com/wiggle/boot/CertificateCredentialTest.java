package com.wiggle.boot;

import com.wiggle.client.WiggleClient;
import com.wiggle.core.Tls;
import com.wiggle.server.WiggleServer;
import com.wiggle.testfixtures.TestCerts;
import com.wiggle.testfixtures.TestPorts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** With mutual TLS and {@code WIGGLE_API_AUTH=enforce}, a client certificate's subject is its credential. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class CertificateCredentialTest {

    private static final int API = TestPorts.free();
    private static final TestCerts CERTS;

    static {
        try {
            CERTS = TestCerts.generate(Files.createTempDirectory("boot-cert-credential"));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void settings(DynamicPropertyRegistry registry) {
        registry.add("WIGGLE_PORT", () -> API);
        registry.add("WIGGLE_PORTAL_PORT", () -> 0);
        registry.add("WIGGLE_API_AUTH", () -> "enforce");
        registry.add("WIGGLE_TLS_KEYSTORE", () -> CERTS.serverKeyStore().toString());
        registry.add("WIGGLE_TLS_KEYSTORE_PASSWORD", () -> TestCerts.PASSWORD);
        registry.add("WIGGLE_TLS_TRUSTSTORE", () -> CERTS.trustStore().toString());
        registry.add("WIGGLE_TLS_TRUSTSTORE_PASSWORD", () -> TestCerts.PASSWORD);
    }

    @Autowired WiggleServer server;

    @Test @DisplayName("a trusted certificate with no credential is refused, and its subject made a credential is let in")
    void subjectIsTheCredential() throws Exception {
        Tls.Options tls = new Tls.Options(CERTS.clientKeyStore().toString(), TestCerts.PASSWORD,
                CERTS.trustStore().toString(), TestCerts.PASSWORD);
        try (WiggleClient client = new WiggleClient("127.0.0.1:" + API, tls)) {
            WiggleClient.WiggleApiException refused = assertThrows(WiggleClient.WiggleApiException.class, client::cluster);
            assertEquals(401, refused.status(), "a trusted certificate with no credential is not enough");

            server.accounts().createCertificate("ops", "tls-client", "CN=wiggle-client", "admin", null);
            long deadline = System.currentTimeMillis() + 5_000;
            while (true) {
                try {
                    client.cluster();
                    return;
                } catch (WiggleClient.WiggleApiException e) {
                    if (System.currentTimeMillis() > deadline) throw e;
                    Thread.sleep(100);
                }
            }
        }
    }
}
