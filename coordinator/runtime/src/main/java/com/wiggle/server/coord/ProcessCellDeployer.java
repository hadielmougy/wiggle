package com.wiggle.server.coord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CellDeployer} that forks one JVM per node from a launch command (typically the {@code wiggle}
 * distribution launcher), configuring each via environment variables -- the same knobs
 * {@code ServerConfig.fromEnvironment()} reads. Use this to run a cell as separate processes on a box
 * without an orchestrator; a container/pod deployer is a later impl of the same seam.
 *
 * <p>The storage secret is resolved at deploy time and passed to the child via {@code WIGGLE_JDBC_PASSWORD}
 * (never persisted, R22). Schema migration is left to the nodes' own idempotent start-up migration (the
 * JDBC baseline guard makes concurrent first-starts safe), so {@link #migrateSchema} is a no-op here.
 */
public final class ProcessCellDeployer implements CellDeployer {

    private static final System.Logger LOG = System.getLogger(ProcessCellDeployer.class.getName());

    private final List<String> launchCommand;
    private final SecretResolver secrets;
    private final String coordinatorUrl;   // nullable: children register back when set
    private final Map<String, List<Process>> deployments = new ConcurrentHashMap<>();

    public ProcessCellDeployer(List<String> launchCommand, SecretResolver secrets, String coordinatorUrl) {
        if (launchCommand == null || launchCommand.isEmpty()) {
            throw new IllegalArgumentException("launchCommand is required");
        }
        this.launchCommand = List.copyOf(launchCommand);
        this.secrets = secrets == null ? SecretResolver.ENV : secrets;
        this.coordinatorUrl = coordinatorUrl;
    }

    @Override public void migrateSchema(NamespaceSpec spec) {
        // No-op: each forked node migrates its (shared) schema idempotently on start, guarded by the
        // JDBC baseline check, so a dedicated migrate step is not required for the process substrate.
    }

    @Override public Deployment deploy(NamespaceSpec spec) {
        List<Process> existing = deployments.get(spec.namespace());
        if (existing != null && existing.stream().anyMatch(Process::isAlive)) {
            return new Deployment(spec.namespace(), "127.0.0.1:" + spec.basePort());   // idempotent resume
        }
        int replicas = spec.replicas();
        if (replicas > 1 && spec.storage().isInMemory()) {
            LOG.log(System.Logger.Level.WARNING, () -> "in-memory cell '" + spec.namespace()
                    + "' cannot share a store across processes; deploying a single node");
            replicas = 1;
        }
        List<Process> procs = new ArrayList<>();
        try {
            for (int i = 0; i < replicas; i++) {
                procs.add(fork(spec, spec.basePort() + i));
            }
        } catch (IOException e) {
            for (Process p : procs) p.destroyForcibly();   // roll back a partial deploy
            throw new UncheckedIOException("deploy of cell '" + spec.namespace() + "' failed", e);
        }
        deployments.put(spec.namespace(), procs);
        return new Deployment(spec.namespace(), "127.0.0.1:" + spec.basePort());
    }

    @Override public void teardown(String deploymentId) {
        List<Process> procs = deployments.remove(deploymentId);
        if (procs == null) return;
        for (Process p : procs) p.destroy();
        for (Process p : procs) {
            try {
                if (!p.waitFor(5, java.util.concurrent.TimeUnit.SECONDS)) p.destroyForcibly();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                p.destroyForcibly();
            }
        }
    }

    @Override public void close() {
        for (String id : List.copyOf(deployments.keySet())) teardown(id);
    }

    private Process fork(NamespaceSpec spec, int port) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(launchCommand);
        Map<String, String> env = pb.environment();
        env.put("WIGGLE_ROLE", "cell");
        env.put("WIGGLE_NAMESPACE", spec.namespace());
        env.put("WIGGLE_CELL_ID", spec.namespace());
        env.put("WIGGLE_PORT", Integer.toString(port));
        StorageConfig sc = spec.storage();
        if (!sc.isInMemory()) {
            env.put("WIGGLE_JDBC_URL", sc.jdbcUrl());
            if (sc.user() != null) env.put("WIGGLE_JDBC_USER", sc.user());
            String password = secrets.resolve(sc.secretRef());
            if (password != null) env.put("WIGGLE_JDBC_PASSWORD", password);
            if (sc.poolSize() > 0) env.put("WIGGLE_JDBC_POOL_SIZE", Integer.toString(sc.poolSize()));
        }
        if (coordinatorUrl != null && !coordinatorUrl.isBlank()) env.put("WIGGLE_COORDINATOR_URL", coordinatorUrl);
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        return pb.start();
    }
}
