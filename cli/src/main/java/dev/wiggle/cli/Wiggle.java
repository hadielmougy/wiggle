package dev.wiggle.cli;

import dev.wiggle.client.WiggleClient;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.core.Tls;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;

/**
 * The {@code wiggle} command-line tool. Two subcommands over a declarative workflow YAML file
 * (see {@code docs/workflow-yaml.md}): {@code validate} compiles it offline, {@code register} sends
 * it to a server. Step handlers are bound separately, by name, on workers.
 */
@Command(name = "wiggle", mixinStandardHelpOptions = true, version = "wiggle 2.1.5",
        subcommands = {Wiggle.Validate.class, Wiggle.Register.class},
        description = "Author and register Wiggle workflows from declarative YAML.")
public final class Wiggle implements Runnable {

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);   // no subcommand -> print help
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Wiggle()).setCaseInsensitiveEnumValuesAllowed(true).execute(args));
    }

    @Command(name = "validate", mixinStandardHelpOptions = true,
            description = "Compile and validate a workflow YAML file offline (no server).")
    static final class Validate implements Callable<Integer> {
        @Parameters(index = "0", paramLabel = "FILE", description = "the workflow YAML file")
        Path file;

        @Override
        public Integer call() {
            try {
                Blueprint<Map<String, Object>> bp = WorkflowYaml.load(file);
                System.out.printf("OK  %s  v%d  (%d nodes, queues %s)%n",
                        bp.name(), bp.version(), bp.definition().nodes().size(), new TreeSet<>(bp.queues()));
                return 0;
            } catch (Exception e) {
                System.err.println("invalid: " + describe(e));
                return 1;
            }
        }
    }

    @Command(name = "register", mixinStandardHelpOptions = true,
            description = "Validate and register a workflow YAML file with a server.")
    static final class Register implements Callable<Integer> {
        @Parameters(index = "0", paramLabel = "FILE", description = "the workflow YAML file")
        Path file;

        @Option(names = {"-s", "--server"},
                description = "gRPC target host:port (default: $WIGGLE_URL, else localhost:8080)")
        String server;

        @Option(names = "--tls",
                description = "connect over TLS using the JVM default trust store (implied by any --tls-* option)")
        boolean tls;

        @Option(names = "--tls-truststore", paramLabel = "PATH",
                description = "truststore (.p12/.jks) that verifies the server certificate")
        String trustStore;

        @Option(names = "--tls-truststore-password", paramLabel = "PW", arity = "0..1", interactive = true,
                description = "truststore password (prompted if the flag is given with no value)")
        String trustStorePassword;

        @Option(names = "--tls-keystore", paramLabel = "PATH",
                description = "client keystore (.p12/.jks) presenting a certificate for mTLS")
        String keyStore;

        @Option(names = "--tls-keystore-password", paramLabel = "PW", arity = "0..1", interactive = true,
                description = "keystore password (prompted if the flag is given with no value)")
        String keyStorePassword;

        @Override
        public Integer call() {
            Blueprint<Map<String, Object>> bp;
            try {
                bp = WorkflowYaml.load(file);   // validate before touching the network
            } catch (Exception e) {
                System.err.println("invalid: " + describe(e));
                return 1;
            }
            String target = server != null ? server
                    : System.getenv().getOrDefault("WIGGLE_URL", "localhost:8080");

            // Explicit flags override the WIGGLE_TLS_* environment per field; --tls (or any store)
            // forces TLS. With nothing set, the channel stays plaintext.
            Tls.Options env = Tls.Options.fromEnvironment();
            Tls.Options opts = new Tls.Options(
                    orElse(keyStore, env.keyStorePath()),
                    orElse(keyStorePassword, env.keyStorePassword()),
                    orElse(trustStore, env.trustStorePath()),
                    orElse(trustStorePassword, env.trustStorePassword()));
            boolean requireTls = tls || opts.any();

            try (WiggleClient client = new WiggleClient(target, opts, requireTls)) {
                client.register(bp);
                System.out.printf("registered  %s  v%d  (%d nodes)  ->  %s%s%n",
                        bp.name(), bp.version(), bp.definition().nodes().size(), target,
                        requireTls ? " (TLS)" : "");
                return 0;
            } catch (Exception e) {
                System.err.println("register failed (" + target + "): " + describe(e));
                return 1;
            }
        }

        private static String orElse(String flag, String fallback) {
            return flag != null && !flag.isBlank() ? flag : fallback;
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return msg != null ? msg : t.getClass().getSimpleName();
    }
}