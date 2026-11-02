package com.wiggle.testfixtures;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Keystores made with {@code keytool} for TLS tests: a server identity for {@code localhost} and
 * {@code 127.0.0.1}, a client identity ({@code CN=wiggle-client}), and a truststore trusting both.
 * Every store is PKCS12 under {@link #PASSWORD}.
 */
public record TestCerts(Path serverKeyStore, Path clientKeyStore, Path trustStore) {

    public static final String PASSWORD = "storepass";

    /** Whether {@code keytool} is there to make them. */
    public static boolean available() {
        return Files.isExecutable(Path.of(keytool()));
    }

    /** New stores in {@code dir}. */
    public static TestCerts generate(Path dir) throws Exception {
        TestCerts certs = new TestCerts(dir.resolve("server.p12"), dir.resolve("client.p12"), dir.resolve("trust.p12"));
        Path serverCrt = dir.resolve("server.crt");
        Path clientCrt = dir.resolve("client.crt");
        genKeypair(certs.serverKeyStore, "server", "CN=localhost", "SAN=dns:localhost,ip:127.0.0.1");
        genKeypair(certs.clientKeyStore, "client", "CN=wiggle-client", null);
        exportCert(certs.serverKeyStore, "server", serverCrt);
        exportCert(certs.clientKeyStore, "client", clientCrt);
        importCert(certs.trustStore, "server", serverCrt);
        importCert(certs.trustStore, "client", clientCrt);
        return certs;
    }

    private static String keytool() {
        return System.getProperty("java.home") + "/bin/keytool";
    }

    private static void genKeypair(Path ks, String alias, String dname, String san) throws Exception {
        List<String> args = new ArrayList<>(List.of(
                "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                "-dname", dname, "-keystore", ks.toString(), "-storetype", "PKCS12",
                "-storepass", PASSWORD, "-keypass", PASSWORD));
        if (san != null) {
            args.add("-ext");
            args.add(san);
        }
        keytool(args);
    }

    private static void exportCert(Path ks, String alias, Path out) throws Exception {
        keytool(List.of("-exportcert", "-rfc", "-alias", alias, "-keystore", ks.toString(),
                "-storepass", PASSWORD, "-file", out.toString()));
    }

    private static void importCert(Path trustStore, String alias, Path cert) throws Exception {
        keytool(List.of("-importcert", "-noprompt", "-alias", alias, "-file", cert.toString(),
                "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD));
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
