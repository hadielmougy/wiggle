package com.wiggle.cli;

import com.wiggle.client.CellResolver;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.Tls;
import com.wiggle.proto.AllocatedWorkflow;
import com.wiggle.proto.EpochRing;
import com.wiggle.proto.Policy;
import com.wiggle.proto.RingSlot;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;

/**
 * The {@code wiggle} command-line tool. Two subcommands over a declarative workflow YAML file
 * (see {@code docs/workflow-yaml.md}): {@code validate} compiles it offline, {@code register} sends
 * it to a server. Step handlers are bound separately, by name, on workers.
 */
@Command(name = "wiggle", mixinStandardHelpOptions = true, version = "wiggle 2.1.6",
        subcommands = {Wiggle.Use.class, Wiggle.Validate.class, Wiggle.Register.class,
                Wiggle.Allocate.class, Wiggle.Deallocate.class, Wiggle.Allocations.class, Wiggle.OpenEpoch.class},
        description = "Author and register Wiggle workflows; allocate flows to namespaces via a coordinator.")
public final class Wiggle implements Runnable {

    @Override
    public void run() {
        new CommandLine(this).usage(System.out);   // no subcommand -> print help
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new Wiggle()).setCaseInsensitiveEnumValuesAllowed(true).execute(args));
    }

    @Command(name = "use", mixinStandardHelpOptions = true,
            description = "Set (or show) which server the CLI talks to: a coordinator or a single cell, and its address. "
                    + "Saved to ~/.wiggle and used by every command unless overridden by a flag or env var.")
    static final class Use implements Callable<Integer> {
        @Parameters(index = "0", arity = "0..1", paramLabel = "KIND",
                description = "coordinator | cell (omit KIND and ADDRESS to show the current target)")
        String kind;

        @Parameters(index = "1", arity = "0..1", paramLabel = "ADDRESS", description = "host:port")
        String address;

        @Option(names = "--clear", description = "clear the saved target")
        boolean clear;

        @Override
        public Integer call() {
            if (clear) {
                Target.clear();
                System.out.println("target cleared (" + Target.configFile() + ")");
                return 0;
            }
            if (kind == null) {   // show
                Target.load().ifPresentOrElse(
                        t -> System.out.println("current target: " + t + "  (" + Target.configFile() + ")"),
                        () -> System.out.println("no target set; commands default to a coordinator on "
                                + Target.Kind.COORDINATOR.fallback + " / a cell on " + Target.Kind.CELL.fallback));
                return 0;
            }
            Target.Kind k;
            try {
                k = Target.Kind.valueOf(kind.trim().toUpperCase());
            } catch (IllegalArgumentException e) {
                System.err.println("KIND must be 'coordinator' or 'cell', not '" + kind + "'");
                return 2;
            }
            if (address == null || address.isBlank()) {
                System.err.println("ADDRESS (host:port) is required when setting a target");
                return 2;
            }
            Target t = new Target(k, address.trim());
            t.save();
            System.out.println("target set: " + t);
            return 0;
        }
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
            String target;
            try {
                target = Target.resolve(Target.Kind.CELL, server);   // flag > env > saved cell > default
            } catch (IllegalStateException e) {
                System.err.println(e.getMessage());
                return 2;
            }

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

    // ---- coordinator: allocate / deallocate flows to namespaces ----

    private static CellResolver resolver(String coordinator) {
        return CellResolver.coordinator(coordinator, Tls.Options.fromEnvironment(),
                System.getenv().getOrDefault("WIGGLE_REGION", ""));
    }

    @Command(name = "allocate", mixinStandardHelpOptions = true,
            description = "Allocate a workflow (YAML) to a namespace: fan it out to every cell of the namespace via the coordinator.")
    static final class Allocate implements Callable<Integer> {
        @Parameters(index = "0", paramLabel = "FILE", description = "the workflow YAML file")
        Path file;

        @Option(names = {"-n", "--namespace"}, required = true, description = "the target namespace")
        String namespace;

        @Option(names = {"-c", "--coordinator"},
                description = "coordinator gRPC host:port (default: $WIGGLE_COORDINATOR_URL, else localhost:8099)")
        String coordinator;

        @Override
        public Integer call() {
            Blueprint<Map<String, Object>> bp;
            try {
                bp = WorkflowYaml.load(file);
            } catch (Exception e) {
                System.err.println("invalid: " + describe(e));
                return 1;
            }
            final String target;
            try {
                target = Target.resolve(Target.Kind.COORDINATOR, coordinator);   // flag > env > saved coordinator > default
            } catch (IllegalStateException e) {
                System.err.println(e.getMessage());
                return 2;
            }
            try (CellResolver resolver = resolver(target)) {
                resolver.registerWorkflow(namespace, bp);
                System.out.printf("allocated  %s  v%d  ->  namespace '%s'  (via %s)%n",
                        bp.name(), bp.version(), namespace, target);
                return 0;
            } catch (Exception e) {
                System.err.println("allocate failed (" + target + "): " + describe(e));
                return 1;
            }
        }
    }

    @Command(name = "deallocate", mixinStandardHelpOptions = true,
            description = "Deallocate a workflow from a namespace (stops fan-out to cells that join later).")
    static final class Deallocate implements Callable<Integer> {
        @Option(names = {"-w", "--workflow"}, required = true, paramLabel = "NAME", description = "the workflow name")
        String name;

        @Option(names = {"-n", "--namespace"}, required = true, description = "the namespace")
        String namespace;

        @Option(names = {"-c", "--coordinator"},
                description = "coordinator gRPC host:port (default: $WIGGLE_COORDINATOR_URL, else localhost:8099)")
        String coordinator;

        @Override
        public Integer call() {
            final String target;
            try {
                target = Target.resolve(Target.Kind.COORDINATOR, coordinator);   // flag > env > saved coordinator > default
            } catch (IllegalStateException e) {
                System.err.println(e.getMessage());
                return 2;
            }
            try (CellResolver resolver = resolver(target)) {
                boolean removed = resolver.deregisterWorkflow(namespace, name);
                System.out.println(removed
                        ? "deallocated  " + name + "  from namespace '" + namespace + "'"
                        : "not allocated: '" + name + "' is not in namespace '" + namespace + "'");
                return removed ? 0 : 1;
            } catch (Exception e) {
                System.err.println("deallocate failed (" + target + "): " + describe(e));
                return 1;
            }
        }
    }

    @Command(name = "allocations", mixinStandardHelpOptions = true,
            description = "List the workflows currently allocated to a namespace.")
    static final class Allocations implements Callable<Integer> {
        @Option(names = {"-n", "--namespace"}, required = true, description = "the namespace")
        String namespace;

        @Option(names = {"-c", "--coordinator"},
                description = "coordinator gRPC host:port (default: $WIGGLE_COORDINATOR_URL, else localhost:8099)")
        String coordinator;

        @Override
        public Integer call() {
            final String target;
            try {
                target = Target.resolve(Target.Kind.COORDINATOR, coordinator);   // flag > env > saved coordinator > default
            } catch (IllegalStateException e) {
                System.err.println(e.getMessage());
                return 2;
            }
            try (CellResolver resolver = resolver(target)) {
                var flows = resolver.listWorkflows(namespace);
                if (flows.isEmpty()) {
                    System.out.println("namespace '" + namespace + "' has no allocated workflows");
                } else {
                    System.out.println("namespace '" + namespace + "':");
                    for (AllocatedWorkflow w : flows) {
                        System.out.printf("  %-30s v%d%n", w.getName(), w.getVersion());
                    }
                }
                return 0;
            } catch (Exception e) {
                System.err.println("allocations failed (" + target + "): " + describe(e));
                return 1;
            }
        }
    }

    @Command(name = "open-epoch", mixinStandardHelpOptions = true,
            description = "Open a new placement epoch for a namespace: publish a shard->cell ring (a reshard). "
                    + "Each SLOT is 'shard=cellId[@region]', e.g. 0=cellA 1=cellB.")
    static final class OpenEpoch implements Callable<Integer> {
        @Option(names = {"-n", "--namespace"}, required = true, description = "the target namespace")
        String namespace;

        @Option(names = {"-c", "--coordinator"},
                description = "coordinator gRPC host:port (default: $WIGGLE_COORDINATOR_URL, else localhost:8099)")
        String coordinator;

        @Parameters(paramLabel = "SLOT", arity = "1..*", description = "ring slots as shard=cellId[@region]")
        List<String> slots;

        @Override
        public Integer call() {
            List<RingSlot> ring = new ArrayList<>();
            for (String s : slots) {
                int eq = s.indexOf('=');
                if (eq <= 0) { System.err.println("bad slot '" + s + "'; use shard=cellId[@region]"); return 2; }
                int shard;
                try {
                    shard = Integer.parseInt(s.substring(0, eq).trim());
                } catch (NumberFormatException e) {
                    System.err.println("bad shard in slot '" + s + "'"); return 2;
                }
                String rest = s.substring(eq + 1).trim();
                String cell = rest, region = "";
                int at = rest.indexOf('@');
                if (at >= 0) { cell = rest.substring(0, at).trim(); region = rest.substring(at + 1).trim(); }
                if (cell.isEmpty()) { System.err.println("missing cell id in slot '" + s + "'"); return 2; }
                RingSlot.Builder b = RingSlot.newBuilder().setShard(shard).setCellId(cell);
                if (!region.isEmpty()) b.setRegion(region);
                ring.add(b.build());
            }
            final String target;
            try {
                target = Target.resolve(Target.Kind.COORDINATOR, coordinator);
            } catch (IllegalStateException e) {
                System.err.println(e.getMessage());
                return 2;
            }
            try (CellResolver resolver = resolver(target)) {
                Policy p = resolver.openEpoch(namespace, ring);
                System.out.printf("opened epoch %d for namespace '%s'  (revision %d)  via %s%n",
                        p.getCurrentEpoch(), namespace, p.getRevision(), target);
                EpochRing er = p.getEpochsMap().get(p.getCurrentEpoch());
                if (er != null) {
                    for (RingSlot rs : er.getRingList()) {
                        System.out.printf("  shard %d -> %s%s%n", rs.getShard(), rs.getCellId(),
                                rs.getRegion().isEmpty() ? "" : " @" + rs.getRegion());
                    }
                }
                return 0;
            } catch (Exception e) {
                System.err.println("open-epoch failed (" + target + "): " + describe(e));
                return 1;
            }
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return msg != null ? msg : t.getClass().getSimpleName();
    }
}