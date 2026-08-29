package dev.wiggle.server.coord;

import dev.wiggle.core.IdCodec;
import dev.wiggle.core.Tls;
import dev.wiggle.proto.ActiveCellsRequest;
import dev.wiggle.proto.ActiveCellsResponse;
import dev.wiggle.proto.CellCoordinatorGrpc;
import dev.wiggle.proto.CoordinatorHeartbeatRequest;
import dev.wiggle.proto.CoordinatorHeartbeatResponse;
import dev.wiggle.proto.DeregisterRequest;
import dev.wiggle.proto.Empty;
import dev.wiggle.proto.Endpoint;
import dev.wiggle.proto.EpochRing;
import dev.wiggle.proto.EpochStatus;
import dev.wiggle.proto.Expected;
import dev.wiggle.proto.FetchConfigRequest;
import dev.wiggle.proto.NodeConfig;
import dev.wiggle.proto.OpenEpochRequest;
import dev.wiggle.proto.Policy;
import dev.wiggle.proto.RegisterRequest;
import dev.wiggle.proto.RegisterResponse;
import dev.wiggle.proto.RegisteredNode;
import dev.wiggle.proto.ResolveRequest;
import dev.wiggle.proto.ResolveResponse;
import dev.wiggle.proto.RingSlot;
import dev.wiggle.proto.SetRingRequest;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The {@code CellCoordinator} gRPC service. Phase 1/T6 implements the admin policy writes
 * ({@code OpenEpoch} / {@code SetRing}) as compare-and-set read-modify-writes over
 * {@link CoordinatorStore} -- so a stale ex-leader's write is rejected -- and hosts the service.
 * The node-lifecycle and resolution RPCs are added in later tickets (T7, T9, T11); until then they
 * return {@code UNIMPLEMENTED} from the generated base.
 */
public final class CoordinatorApi extends CellCoordinatorGrpc.CellCoordinatorImplBase implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(CoordinatorApi.class.getName());
    private static final int CAS_ATTEMPTS = 5;
    private static final int DEFAULT_TTL_S = 300;
    private static final int RESOLVE_TTL_S = 30;   // endpoints move; keep resolution caches short-lived

    /**
     * How often a node heartbeats the coordinator (dictated to nodes in {@code RegisterResponse}).
     * The reaper's dead timeout is derived from this ({@link #nodeDeadMillis}), so node liveness never
     * depends on the coordinator's own cluster-heartbeat config -- a node is only reaped after it
     * genuinely misses several of *these* beats.
     */
    public static final int NODE_HEARTBEAT_INTERVAL_SECONDS = 5;

    /**
     * The reaper's dead timeout: a node is dead after it misses {@code missedHeartbeats} node beats.
     * Floored at two beats so the timeout always exceeds one heartbeat interval, even if
     * {@code missedHeartbeats} is misconfigured to 0 or 1.
     */
    public static long nodeDeadMillis(int missedHeartbeats) {
        return Math.max(2, missedHeartbeats) * (long) NODE_HEARTBEAT_INTERVAL_SECONDS * 1000L;
    }

    private final CoordinatorStore store;
    private final Server server;
    private final ExecutorService pool;
    private volatile boolean started;

    public CoordinatorApi(CoordinatorStore store, int port, Tls.Options tls) throws IOException {
        this.store = store;
        this.pool = Executors.newVirtualThreadPerTaskExecutor();
        this.server = Grpc.newServerBuilderForPort(port, credentials(tls))
                .executor(pool)
                .addService(this)
                .build();
    }

    private static ServerCredentials credentials(Tls.Options tls) throws IOException {
        if (!tls.hasKeyStore()) {
            LOG.log(System.Logger.Level.WARNING,
                    "coordinator gRPC is PLAINTEXT; set WIGGLE_TLS_KEYSTORE to enable TLS");
            return InsecureServerCredentials.create();
        }
        TlsServerCredentials.Builder b = TlsServerCredentials.newBuilder().keyManager(Tls.keyManagers(tls));
        if (tls.hasTrustStore()) {
            b.trustManager(Tls.trustManagers(tls)).clientAuth(TlsServerCredentials.ClientAuth.REQUIRE);
        }
        return b.build();
    }

    public void start() {
        try {
            server.start();
            started = true;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LOG.log(System.Logger.Level.DEBUG, () -> "coordinator gRPC listening on port " + port());
    }

    public int port() { return server.getPort(); }

    @Override public void close() {
        if (started) {
            server.shutdown();
            try {
                server.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        pool.shutdownNow();
    }

    // ---- gRPC handlers ----

    @Override public void openEpoch(OpenEpochRequest req, StreamObserver<Policy> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc OpenEpoch namespace=" + req.getNamespace());
        run(resp, () -> doOpenEpoch(req.getNamespace(), req.getRingList()));
    }

    @Override public void setRing(SetRingRequest req, StreamObserver<Policy> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc SetRing namespace=" + req.getNamespace() + " epoch=" + req.getEpoch());
        run(resp, () -> doSetRing(req.getNamespace(), req.getEpoch(), req.getRingList()));
    }

    @Override public void fetchConfig(FetchConfigRequest req, StreamObserver<NodeConfig> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc FetchConfig namespace=" + req.getNamespace());
        run(resp, () -> doFetchConfig(req.getNamespace()));
    }

    @Override public void register(RegisterRequest req, StreamObserver<RegisterResponse> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc Register namespace=" + req.getNamespace()
                + " node=" + req.getNode().getName());
        run(resp, () -> doRegister(req.getNamespace(), req.getNode()));
    }

    @Override public void heartbeat(CoordinatorHeartbeatRequest req, StreamObserver<CoordinatorHeartbeatResponse> resp) {
        run(resp, () -> doHeartbeat(req.getNodeId(), req.getConfigGeneration()));
    }

    @Override public void deregister(DeregisterRequest req, StreamObserver<Empty> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc Deregister node=" + req.getNodeId());
        run(resp, () -> {
            store.removeNode(req.getNodeId());
            return Empty.getDefaultInstance();
        });
    }

    // ---- node-lifecycle logic (directly unit-testable) ----

    /** The namespace's current config generation (its policy revision; 0 if none yet). */
    public NodeConfig doFetchConfig(String namespace) {
        long generation = store.getPolicy(namespace).map(CoordPolicy::revision).orElse(0L);
        // Phase 1: no storage/tuning overlay yet (populated by provisioning, T13) -- the node keeps
        // its local config. `generation` still lets a node observe namespace change (policy edits).
        return NodeConfig.newBuilder()
                .setNamespace(namespace)
                .setGeneration(generation)
                .setExpected(Expected.newBuilder().build())
                .setTtlSeconds(DEFAULT_TTL_S)
                .build();
    }

    /** Records a node in the roster and returns its coordinator-assigned id. */
    public RegisterResponse doRegister(String namespace, RegisteredNode node) {
        String nodeId = UUID.randomUUID().toString();
        store.upsertNode(new CoordNode(nodeId, namespace, node.getEndpoint(), emptyToNull(node.getRegion()),
                node.getEngineVersion(), 0, System.currentTimeMillis()));
        return RegisterResponse.newBuilder()
                .setNodeId(nodeId)
                .setHeartbeatIntervalSeconds(NODE_HEARTBEAT_INTERVAL_SECONDS)
                .build();
    }

    /** Touches the node's liveness; returns {@code ok=false} for an unknown node (it should re-register). */
    public CoordinatorHeartbeatResponse doHeartbeat(String nodeId, long observedGeneration) {
        Optional<CoordNode> node = store.touchNode(nodeId, System.currentTimeMillis(), observedGeneration);
        if (node.isEmpty()) {
            return CoordinatorHeartbeatResponse.newBuilder().setOk(false).build();
        }
        long generation = store.getPolicy(node.get().namespace()).map(CoordPolicy::revision).orElse(0L);
        return CoordinatorHeartbeatResponse.newBuilder().setOk(true).setConfigGeneration(generation).build();
    }

    /** Removes a node from the roster. */
    public void doDeregister(String nodeId) {
        store.removeNode(nodeId);
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }

    // ---- resolution (clients & workers) ----

    @Override public void resolve(ResolveRequest req, StreamObserver<ResolveResponse> resp) {
        run(resp, () -> doResolve(req));
    }

    @Override public void activeCells(ActiveCellsRequest req, StreamObserver<ActiveCellsResponse> resp) {
        run(resp, () -> doActiveCells(req.getNamespace(), emptyToNull(req.getCallerRegion())));
    }

    /**
     * Resolves a namespace (for a new start) or a specific instance id to a cell {@link Endpoint}.
     *
     * <p>MVP note: one cell per namespace, so the dial address comes from the namespace's live node
     * roster (region-filtered). When a namespace spans cells (T12), this parses the id's epoch/shard,
     * looks up {@code ring[shard]} in the policy, and maps that cell to its registered endpoints.
     */
    public ResolveResponse doResolve(ResolveRequest req) {
        String namespace;
        long epoch;
        if (req.getByCase() == ResolveRequest.ByCase.INSTANCE_ID) {
            IdCodec.Placement p = IdCodec.parse(req.getInstanceId()).orElseThrow(() ->
                    new IllegalArgumentException("cannot route a legacy instance id ('" + req.getInstanceId()
                            + "'); resolve by namespace instead"));
            namespace = p.namespace();
            epoch = p.epoch();
        } else {
            namespace = req.getNamespace();
            epoch = store.getPolicy(namespace).map(CoordPolicy::currentEpoch).orElse(0L);
        }
        Endpoint endpoint = endpointFor(namespace, emptyToNull(req.getCallerRegion()));
        return ResolveResponse.newBuilder()
                .setNamespace(namespace)
                .setEpoch(epoch)
                .setEndpoint(endpoint)
                .setTtlSeconds(RESOLVE_TTL_S)
                .build();
    }

    /** The cells hosting live work for a namespace (MVP: the one cell), plus a change generation. */
    public ActiveCellsResponse doActiveCells(String namespace, String callerRegion) {
        long generation = store.getPolicy(namespace).map(CoordPolicy::revision).orElse(0L);
        return ActiveCellsResponse.newBuilder()
                .setGeneration(generation)
                .addCells(endpointFor(namespace, callerRegion))
                .setTtlSeconds(RESOLVE_TTL_S)
                .build();
    }

    /** Builds a cell endpoint from the namespace's live roster, preferring the caller's region. */
    private Endpoint endpointFor(String namespace, String callerRegion) {
        List<CoordNode> nodes = store.nodes(namespace);
        if (callerRegion != null) {
            List<CoordNode> regional = new ArrayList<>();
            for (CoordNode n : nodes) if (callerRegion.equals(n.region())) regional.add(n);
            if (!regional.isEmpty()) nodes = regional;   // same cell, region-appropriate addresses (R24)
        }
        if (nodes.isEmpty()) {
            throw new IllegalStateException("no live nodes for namespace '" + namespace + "'");
        }
        Endpoint.Builder b = Endpoint.newBuilder()
                .setTarget(nodes.get(0).endpoint())      // TODO: a stable cell DNS name once provisioning records one
                .setTtlSeconds(RESOLVE_TTL_S);
        String region = nodes.get(0).region();
        if (region != null) b.setRegion(region);
        for (CoordNode n : nodes) b.addAddresses(n.endpoint());
        return b.build();
    }

    // ---- logic (directly unit-testable, no gRPC plumbing) ----

    /**
     * Opens a new epoch for {@code namespace}: creates epoch 0 if none exists, else appends
     * {@code currentEpoch + 1} (marking the previous epoch DRAINING). CAS-guarded and retried.
     */
    public Policy doOpenEpoch(String namespace, List<RingSlot> ring) {
        for (int attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
            Optional<CoordPolicy> current = store.getPolicy(namespace);
            if (current.isEmpty()) {
                Map<Long, CoordPolicy.EpochRing> epochs = new LinkedHashMap<>();
                epochs.put(0L, new CoordPolicy.EpochRing(toDomainRing(ring), CoordPolicy.EpochStatus.OPEN));
                if (store.casPolicy(namespace, 0, new CoordPolicy(namespace, 0, 0, epochs)) > 0) {
                    return toProto(store.getPolicy(namespace).orElseThrow());
                }
            } else {
                CoordPolicy c = current.get();
                long newEpoch = c.currentEpoch() + 1;
                Map<Long, CoordPolicy.EpochRing> epochs = new LinkedHashMap<>(c.epochs());
                CoordPolicy.EpochRing prev = epochs.get(c.currentEpoch());
                if (prev != null) {
                    epochs.put(c.currentEpoch(), new CoordPolicy.EpochRing(prev.ring(), CoordPolicy.EpochStatus.DRAINING));
                }
                epochs.put(newEpoch, new CoordPolicy.EpochRing(toDomainRing(ring), CoordPolicy.EpochStatus.OPEN));
                if (store.casPolicy(namespace, c.revision(), new CoordPolicy(namespace, newEpoch, 0, epochs)) > 0) {
                    return toProto(store.getPolicy(namespace).orElseThrow());
                }
            }
        }
        throw new IllegalStateException("openEpoch: concurrent modification, retries exhausted for " + namespace);
    }

    /** Replaces the ring of an existing epoch. CAS-guarded and retried. */
    public Policy doSetRing(String namespace, long epoch, List<RingSlot> ring) {
        for (int attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
            CoordPolicy c = store.getPolicy(namespace)
                    .orElseThrow(() -> new IllegalArgumentException("no policy for namespace " + namespace));
            CoordPolicy.EpochRing existing = c.epochs().get(epoch);
            if (existing == null) throw new IllegalArgumentException("no epoch " + epoch + " in namespace " + namespace);
            Map<Long, CoordPolicy.EpochRing> epochs = new LinkedHashMap<>(c.epochs());
            epochs.put(epoch, new CoordPolicy.EpochRing(toDomainRing(ring), existing.status()));
            if (store.casPolicy(namespace, c.revision(), new CoordPolicy(namespace, c.currentEpoch(), 0, epochs)) > 0) {
                return toProto(store.getPolicy(namespace).orElseThrow());
            }
        }
        throw new IllegalStateException("setRing: concurrent modification, retries exhausted for " + namespace);
    }

    // ---- mapping (domain <-> proto) ----

    private static List<CoordPolicy.RingSlot> toDomainRing(List<RingSlot> ring) {
        List<CoordPolicy.RingSlot> out = new ArrayList<>();
        for (RingSlot s : ring) out.add(new CoordPolicy.RingSlot(s.getShard(), s.getCellId(), s.getRegion()));
        return out;
    }

    private static Policy toProto(CoordPolicy p) {
        Policy.Builder b = Policy.newBuilder()
                .setNamespace(p.namespace()).setCurrentEpoch(p.currentEpoch()).setRevision(p.revision());
        for (Map.Entry<Long, CoordPolicy.EpochRing> e : p.epochs().entrySet()) {
            EpochRing.Builder er = EpochRing.newBuilder()
                    .setStatus(EpochStatus.valueOf(e.getValue().status().name()));
            for (CoordPolicy.RingSlot s : e.getValue().ring()) {
                er.addRing(RingSlot.newBuilder()
                        .setShard(s.shard()).setCellId(s.cellId())
                        .setRegion(s.region() == null ? "" : s.region()).build());
            }
            b.putEpochs(e.getKey(), er.build());
        }
        return b.build();
    }

    private <T> void run(StreamObserver<T> resp, java.util.function.Supplier<T> handler) {
        try {
            resp.onNext(handler.get());
            resp.onCompleted();
        } catch (IllegalArgumentException e) {
            resp.onError(Status.INVALID_ARGUMENT.withDescription(String.valueOf(e.getMessage())).asRuntimeException());
        } catch (IllegalStateException e) {
            resp.onError(Status.ABORTED.withDescription(String.valueOf(e.getMessage())).asRuntimeException());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "coordinator rpc failed", e);
            resp.onError(Status.INTERNAL.withDescription("internal error").asRuntimeException());
        }
    }
}
