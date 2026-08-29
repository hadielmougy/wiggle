package dev.wiggle.dist.coord;

import dev.wiggle.proto.CellCoordinatorGrpc;
import dev.wiggle.proto.CoordinatorHeartbeatRequest;
import dev.wiggle.proto.CoordinatorHeartbeatResponse;
import dev.wiggle.proto.DeregisterRequest;
import dev.wiggle.proto.FetchConfigRequest;
import dev.wiggle.proto.Health;
import dev.wiggle.proto.NodeConfig;
import dev.wiggle.proto.RegisterRequest;
import dev.wiggle.proto.RegisterResponse;
import dev.wiggle.proto.RegisteredNode;
import dev.wiggle.server.CellPlacement;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Talks to the {@code CellCoordinator} over gRPC: register on start, heartbeat on a schedule,
 * deregister on close. Every call is best-effort -- a coordinator that is down or flapping only logs;
 * it never stops the node. If a heartbeat reports the node is unknown (it was expired), the link
 * re-registers on the next beat.
 *
 * <p>Phase 1/T7: registration + liveness are real. A newer config {@code generation} is detected and
 * logged; applying a new config to a running node (or DRAIN-initiated graceful stop) is wired in a
 * later phase (T13) -- for now the coordinator's config is advisory.
 */
public final class HttpCoordinatorLink implements CoordinatorLink {

    private static final System.Logger LOG = System.getLogger(HttpCoordinatorLink.class.getName());

    private final String coordinatorUrl;
    private final ManagedChannel channel;
    private final CellCoordinatorGrpc.CellCoordinatorBlockingStub stub;
    private final ScheduledExecutorService beat =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wiggle-coord-heartbeat");
                t.setDaemon(true);
                return t;
            });

    private volatile CoordinatorLink.NodeInfo node;
    private volatile CoordinatorLink.CellRuntime runtime;   // null for a node that runs no cell
    private volatile String nodeId;
    private volatile long lastGeneration = -1;

    public HttpCoordinatorLink(String coordinatorUrl) {
        this.coordinatorUrl = coordinatorUrl;
        this.channel = Grpc.newChannelBuilder(coordinatorUrl, InsecureChannelCredentials.create()).build();
        this.stub = CellCoordinatorGrpc.newBlockingStub(channel);
    }

    @Override public void register(CoordinatorLink.NodeInfo node, CoordinatorLink.CellRuntime runtime) {
        this.node = node;
        this.runtime = runtime;
        int interval = tryRegister();
        long period = interval > 0 ? interval : 5;   // retry every 5s until registration succeeds
        beat.scheduleAtFixedRate(this::heartbeat, period, period, TimeUnit.SECONDS);
    }

    private CellPlacement placement() {
        CoordinatorLink.CellRuntime r = runtime;
        return r == null ? null : r.placement();
    }

    /** @return the heartbeat interval (seconds) on success, or 0 on failure. */
    private int tryRegister() {
        CoordinatorLink.NodeInfo n = node;
        if (n == null) return 0;
        try {
            RegisterResponse r = stub.register(RegisterRequest.newBuilder()
                    .setNamespace(nz(n.namespace()))
                    .setNode(RegisteredNode.newBuilder()
                            .setName(nz(n.nodeName()))
                            .setEndpoint(nz(n.endpoint()))
                            .setEngineVersion(nz(n.engineVersion()))
                            .setCellId(nz(n.cellId()))
                            .setStartedAt(System.currentTimeMillis())
                            .build())
                    .build());
            this.nodeId = r.getNodeId();
            applyPlacement(r.getEpoch(), r.getShardsList());
            LOG.log(System.Logger.Level.INFO,
                    () -> "registered with coordinator " + coordinatorUrl + " as node " + nodeId
                            + " (epoch " + r.getEpoch() + ", shards " + r.getShardsList() + ")");
            return Math.max(1, r.getHeartbeatIntervalSeconds());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "coordinator register failed (will retry): " + e);
            return 0;
        }
    }

    /** Re-fetches this node's placement (after a policy change) and applies it to the running minter. */
    private void refetchPlacement() {
        CoordinatorLink.NodeInfo n = node;
        if (n == null || placement() == null) return;
        try {
            NodeConfig cfg = stub.fetchConfig(FetchConfigRequest.newBuilder()
                    .setNamespace(nz(n.namespace()))
                    .setNode(dev.wiggle.proto.NodeInfo.newBuilder()
                            .setName(nz(n.nodeName())).setCellId(nz(n.cellId())).build())
                    .build());
            applyPlacement(cfg.getEpoch(), cfg.getShardsList());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "coordinator placement re-fetch failed (best-effort): " + e);
        }
    }

    private void applyPlacement(long epoch, java.util.List<Integer> shards) {
        CellPlacement p = placement();
        if (p != null) p.set(epoch, shards);
    }

    /** This node's current live-by-epoch census for the heartbeat, or empty when it runs no cell. */
    private Map<Long, Integer> liveByEpoch() {
        CoordinatorLink.CellRuntime r = runtime;
        if (r == null || r.liveByEpoch() == null) return Map.of();
        try {
            return r.liveByEpoch().get();
        } catch (RuntimeException e) {
            return Map.of();   // health is advisory; a read failure must not stop the heartbeat
        }
    }

    @Override public void heartbeat() {
        String id = nodeId;
        if (id == null) {
            tryRegister();
            return;
        }
        try {
            CoordinatorHeartbeatResponse r = stub.heartbeat(CoordinatorHeartbeatRequest.newBuilder()
                    .setNodeId(id)
                    .setConfigGeneration(Math.max(0, lastGeneration))
                    .setHealth(Health.newBuilder().putAllLiveByEpoch(liveByEpoch()).build())
                    .build());
            if (!r.getOk()) {
                nodeId = null;   // coordinator no longer knows us; re-register next beat
                return;
            }
            if (r.getConfigGeneration() != lastGeneration) {
                long old = lastGeneration;
                lastGeneration = r.getConfigGeneration();
                LOG.log(System.Logger.Level.INFO, () -> "coordinator config generation " + old + " -> "
                        + lastGeneration + "; re-fetching placement");
                refetchPlacement();   // an epoch bump / ring edit re-points where this node mints (T12)
            }
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "coordinator heartbeat failed (best-effort): " + e);
        }
    }

    @Override public void close() {
        beat.shutdownNow();
        String id = nodeId;
        if (id != null) {
            try {
                stub.deregister(DeregisterRequest.newBuilder().setNodeId(id).build());
            } catch (RuntimeException ignored) {
                // best-effort: the coordinator ages us out via missed heartbeats
            }
        }
        channel.shutdownNow();
    }

    private static String nz(String s) {
        return s == null ? "" : s;
    }
}
