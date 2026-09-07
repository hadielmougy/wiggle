package com.wiggle.console;

import com.wiggle.core.Tls;
import org.apache.catalina.Context;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;

/**
 * The console's embedded Tomcat: one {@link DashboardServlet} mapped to {@code /*} behind an
 * {@link AuthFilter}, serving the dashboard SPA + JSON API. Plaintext by default; HTTPS (and mTLS when a
 * truststore is set) when the {@link Tls.Options} carries a keystore.
 */
final class ConsoleServer implements AutoCloseable {

    private final Tomcat tomcat;

    ConsoleServer(DashboardData data, ConsoleAuth auth, int port, Tls.Options tls) {
        this.tomcat = new Tomcat();
        this.tomcat.setBaseDir(System.getProperty("java.io.tmpdir"));

        Connector connector = new Connector("HTTP/1.1");
        connector.setPort(port);
        if (tls.hasKeyStore()) configureTls(connector, tls);
        tomcat.setConnector(connector);

        Context ctx = tomcat.addContext("", null);
        Tomcat.addServlet(ctx, "dashboard", new DashboardServlet(data, auth));
        ctx.addServletMappingDecoded("/*", "dashboard");

        FilterDef fd = new FilterDef();
        fd.setFilterName("auth");
        fd.setFilter(new AuthFilter(auth));
        ctx.addFilterDef(fd);
        FilterMap fm = new FilterMap();
        fm.setFilterName("auth");
        fm.addURLPattern("/*");
        ctx.addFilterMap(fm);
    }

    ConsoleServer start() {
        try {
            tomcat.start();
        } catch (org.apache.catalina.LifecycleException e) {
            throw new IllegalStateException("failed to start console on Tomcat: " + e.getMessage(), e);
        }
        return this;
    }

    int port() {
        return tomcat.getConnector().getLocalPort();
    }

    @Override public void close() {
        try {
            tomcat.stop();
            tomcat.destroy();
        } catch (org.apache.catalina.LifecycleException ignored) {
            // best effort on shutdown
        }
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
            ssl.setCertificateVerification("required");   // mTLS: require a trusted client cert
        }
        connector.addSslHostConfig(ssl);
    }
}
