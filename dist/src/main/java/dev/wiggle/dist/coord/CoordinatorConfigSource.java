package dev.wiggle.dist.coord;

import dev.wiggle.proto.CellCoordinatorGrpc;
import dev.wiggle.proto.FetchConfigRequest;
import dev.wiggle.proto.NodeConfig;
import dev.wiggle.proto.NodeInfo;
import dev.wiggle.server.ServerConfig;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.concurrent.TimeUnit;

/**
 * Config source used when a coordinator is configured. It loads the env baseline, then fetches the
 * namespace's config from the coordinator and overlays it. If the coordinator is unreachable at boot,
 * it falls back to the local config -- so a coordinator outage never prevents a node from starting.
 *
 * <p>Phase 1/T7: the fetch + fallback are real. The overlay is currently empty (the coordinator does
 * not yet own per-namespace storage/tuning -- that arrives with provisioning, T13), so today this
 * effectively returns the local config while proving the plumbing and logging the generation.
 */
public final class CoordinatorConfigSource implements ConfigSource {

    private static final System.Logger LOG = System.getLogger(CoordinatorConfigSource.class.getName());

    private final ConfigSource base;
    private final String coordinatorUrl;

    public CoordinatorConfigSource(ConfigSource base, String coordinatorUrl) {
        this.base = base;
        this.coordinatorUrl = coordinatorUrl;
    }

    public String coordinatorUrl() { return coordinatorUrl; }

    @Override public ServerConfig load() {
        ServerConfig local = base.load();
        String namespace = System.getenv().getOrDefault("WIGGLE_NAMESPACE", "");
        ManagedChannel channel = Grpc.newChannelBuilder(coordinatorUrl, InsecureChannelCredentials.create()).build();
        try {
            NodeConfig cfg = CellCoordinatorGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .fetchConfig(FetchConfigRequest.newBuilder()
                            .setNamespace(namespace)
                            .setNode(NodeInfo.newBuilder()
                                    .setName(local.nodeName())
                                    .setEngineVersion(engineVersion())
                                    .build())
                            .build());
            LOG.log(System.Logger.Level.INFO, () -> "fetched coordinator config for namespace '" + namespace
                    + "' (generation " + cfg.getGeneration() + ")");
            return overlay(local, cfg);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING,
                    "coordinator unreachable at boot (" + coordinatorUrl + "); using local config: " + e);
            return local;
        } finally {
            channel.shutdownNow();
        }
    }

    /**
     * Overlays coordinator-supplied config onto the local baseline. Phase 1: the coordinator sends no
     * storage/tuning overlay yet, so this returns the local config. When T13 populates it, apply the
     * storage + sparse tuning here (needs {@code ServerConfig.withStorage(...)} / tuning copiers).
     */
    private static ServerConfig overlay(ServerConfig local, NodeConfig cfg) {
        return local;
    }

    private static String engineVersion() {
        String v = ServerConfig.class.getPackage().getImplementationVersion();
        return v != null ? v : "dev";
    }
}
