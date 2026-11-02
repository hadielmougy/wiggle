package com.wiggle.server.api;

import com.wiggle.core.Tls;
import com.wiggle.core.wire.ControlPlane;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Serves the {@link ControlPlaneApi} on embedded Tomcat: {@code POST /v1/<Method>} with a JSON body,
 * as {@link ControlPlane} describes. HTTPS when a keystore is configured, requiring client
 * certificates when a truststore is too. Every call runs on its own virtual thread, so a long poll
 * holds no platform thread.
 */
public final class HttpApi implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(HttpApi.class.getName());

    private final Tomcat tomcat;
    private final ExecutorService pool;

    /** Serves {@code servlet} on {@code port}, over TLS when {@code tls} has a keystore. */
    public HttpApi(int port, Tls.Options tls, ApiServlet servlet) throws IOException {
        this.pool = Executors.newVirtualThreadPerTaskExecutor();

        this.tomcat = new Tomcat();
        this.tomcat.setBaseDir(Files.createTempDirectory("wiggle-api").toString());
        Connector connector = new Connector("HTTP/1.1");
        connector.setPort(port);
        connector.getProtocolHandler().setExecutor(pool);
        if (tls.hasKeyStore()) {
            configureTls(connector, tls);
        } else {
            LOG.log(System.Logger.Level.WARNING, "API is PLAINTEXT; set WIGGLE_TLS_KEYSTORE to enable TLS");
        }
        tomcat.setConnector(connector);

        Context ctx = tomcat.addContext("", null);
        Tomcat.addServlet(ctx, "api", servlet);
        ctx.addServletMappingDecoded(ControlPlane.PREFIX + "*", "api");
    }

    private static void configureTls(Connector connector, Tls.Options tls) {
        connector.setSecure(true);
        connector.setScheme("https");
        connector.setProperty("SSLEnabled", "true");
        SSLHostConfig ssl = new SSLHostConfig();
        SSLHostConfigCertificate cert = new SSLHostConfigCertificate(ssl, SSLHostConfigCertificate.Type.UNDEFINED);
        cert.setCertificateKeystoreFile(tls.keyStorePath());
        if (tls.keyStorePassword() != null) cert.setCertificateKeystorePassword(tls.keyStorePassword());
        ssl.addCertificate(cert);
        if (tls.hasTrustStore()) {
            ssl.setTruststoreFile(tls.trustStorePath());
            if (tls.trustStorePassword() != null) ssl.setTruststorePassword(tls.trustStorePassword());
            ssl.setCertificateVerification("required");
            LOG.log(System.Logger.Level.INFO, "API TLS enabled with required client certificates (mTLS)");
        } else {
            LOG.log(System.Logger.Level.INFO, "API TLS enabled (server-side)");
        }
        connector.addSslHostConfig(ssl);
    }

    public void start() {
        try {
            tomcat.start();
        } catch (LifecycleException e) {
            throw new IllegalStateException("failed to start the API on Tomcat: " + e.getMessage(), e);
        }
        LOG.log(System.Logger.Level.DEBUG, () -> "API listening on port " + port());
    }

    public int port() {
        return tomcat.getConnector().getLocalPort();
    }

    @Override public void close() {
        pool.shutdownNow();
        try {
            tomcat.stop();
            tomcat.destroy();
        } catch (LifecycleException e) {
            LOG.log(System.Logger.Level.DEBUG, () -> "API stop: " + e.getMessage());
        }
    }
}
