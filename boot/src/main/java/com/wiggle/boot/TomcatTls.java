package com.wiggle.boot;

import com.wiggle.core.Tls;
import org.apache.catalina.connector.Connector;
import org.apache.tomcat.util.net.SSLHostConfig;
import org.apache.tomcat.util.net.SSLHostConfigCertificate;

/** TLS on a Tomcat connector from {@link Tls.Options}. */
public final class TomcatTls {

    private TomcatTls() {}

    /**
     * Serves {@code connector} over TLS with the keystore's identity, requiring a client certificate the
     * truststore trusts when there is one. Only for options that {@link Tls.Options#hasKeyStore have a keystore}.
     */
    public static void configure(Connector connector, Tls.Options tls) {
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
        }
        connector.addSslHostConfig(ssl);
    }
}
