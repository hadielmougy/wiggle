package dev.wiggle.core;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;

/**
 * Optional TLS material loaded from a keystore/truststore, shared by the gRPC server/client and
 * the HTTP dashboard. All of it is plain JSSE -- no gRPC or HTTP dependency -- so it lives in core
 * and every module can build the {@link KeyManager}/{@link TrustManager} arrays it needs.
 *
 * <p>Everything is opt-in. A consumer with a {@link Options#DISABLED} (or empty) options object
 * falls back to plaintext. On a <b>server</b>, a keystore turns TLS on and a truststore additionally
 * requires client certificates (mTLS). On a <b>client</b>, a truststore verifies the server and a
 * keystore presents a client certificate for mTLS.
 */
public final class Tls {

    private Tls() {}

    /** Keystore/truststore locations and passwords; any field may be null/blank to mean "unset". */
    public record Options(String keyStorePath, String keyStorePassword,
                          String trustStorePath, String trustStorePassword) {

        public static final Options DISABLED = new Options(null, null, null, null);

        public boolean hasKeyStore() { return keyStorePath != null && !keyStorePath.isBlank(); }

        public boolean hasTrustStore() { return trustStorePath != null && !trustStorePath.isBlank(); }

        /** A client uses TLS if it has anything to load; a server needs a keystore (see callers). */
        public boolean any() { return hasKeyStore() || hasTrustStore(); }

        /** Reads {@code WIGGLE_TLS_KEYSTORE[_PASSWORD]} / {@code WIGGLE_TLS_TRUSTSTORE[_PASSWORD]}. */
        public static Options fromEnvironment() {
            return new Options(
                    prop("wiggle.tls.keystore", "WIGGLE_TLS_KEYSTORE"),
                    prop("wiggle.tls.keystore.password", "WIGGLE_TLS_KEYSTORE_PASSWORD"),
                    prop("wiggle.tls.truststore", "WIGGLE_TLS_TRUSTSTORE"),
                    prop("wiggle.tls.truststore.password", "WIGGLE_TLS_TRUSTSTORE_PASSWORD"));
        }

        private static String prop(String sysProp, String env) {
            String v = System.getProperty(sysProp);
            if (v == null) v = System.getenv(env);
            return v == null || v.isBlank() ? null : v;
        }
    }

    /** Key managers from the keystore (the local identity), or {@code null} if none is configured. */
    public static KeyManager[] keyManagers(Options o) {
        if (!o.hasKeyStore()) return null;
        try {
            char[] pw = password(o.keyStorePassword());
            KeyStore ks = loadStore(o.keyStorePath(), pw);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, pw);
            return kmf.getKeyManagers();
        } catch (Exception e) {
            throw new IllegalStateException("cannot load TLS keystore '" + o.keyStorePath() + "': " + e.getMessage(), e);
        }
    }

    /** Trust managers from the truststore (whom to trust), or {@code null} if none is configured. */
    public static TrustManager[] trustManagers(Options o) {
        if (!o.hasTrustStore()) return null;
        try {
            KeyStore ts = loadStore(o.trustStorePath(), password(o.trustStorePassword()));
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            return tmf.getTrustManagers();
        } catch (Exception e) {
            throw new IllegalStateException("cannot load TLS truststore '" + o.trustStorePath() + "': " + e.getMessage(), e);
        }
    }

    /** An {@link SSLContext} for the HTTP dashboard, wiring in whichever managers are configured. */
    public static SSLContext sslContext(Options o) {
        try {
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(keyManagers(o), trustManagers(o), null);
            return ctx;
        } catch (Exception e) {
            throw new IllegalStateException("cannot build TLS context: " + e.getMessage(), e);
        }
    }

    /** PKCS12 by default; a {@code .jks} path is loaded as a JKS store. */
    private static KeyStore loadStore(String path, char[] password) throws Exception {
        String type = path.toLowerCase().endsWith(".jks") ? "JKS" : "PKCS12";
        KeyStore ks = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            ks.load(in, password);
        }
        return ks;
    }

    private static char[] password(String p) {
        return p == null ? null : p.toCharArray();
    }
}
