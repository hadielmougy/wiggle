package com.wiggle.server.coord;

import com.google.protobuf.Struct;
import com.wiggle.core.IdCodec;
import com.wiggle.core.Json;
import com.wiggle.core.Tls;
import com.wiggle.proto.ActiveCellsRequest;
import com.wiggle.proto.ActiveCellsResponse;
import com.wiggle.proto.AllocatedWorkflow;
import com.wiggle.proto.CellCoordinatorGrpc;
import com.wiggle.proto.DeregisterWorkflowRequest;
import com.wiggle.proto.DeregisterWorkflowResponse;
import com.wiggle.proto.ListWorkflowsRequest;
import com.wiggle.proto.ListWorkflowsResponse;
import com.wiggle.proto.CoordinatorHeartbeatRequest;
import com.wiggle.proto.CoordinatorHeartbeatResponse;
import com.wiggle.proto.DeregisterRequest;
import com.wiggle.proto.Empty;
import com.wiggle.proto.Endpoint;
import com.wiggle.proto.EpochRing;
import com.wiggle.proto.EpochStatus;
import com.wiggle.proto.Expected;
import com.wiggle.proto.FetchConfigRequest;
import com.wiggle.proto.GetWorkflowRequest;
import com.wiggle.proto.NodeConfig;
import com.wiggle.proto.OpenEpochRequest;
import com.wiggle.proto.Policy;
import com.wiggle.proto.ProtoJson;
import com.wiggle.proto.RegisterRequest;
import com.wiggle.proto.RegisterResponse;
import com.wiggle.proto.RegisterWorkflowRequest;
import com.wiggle.proto.RegisterWorkflowResponse;
import com.wiggle.proto.RegisterWorkflowResult;
import com.wiggle.proto.RegisteredNode;
import com.wiggle.proto.ResolveRequest;
import com.wiggle.proto.ResolveResponse;
import com.wiggle.proto.RingSlot;
import com.wiggle.proto.SetRingRequest;
import com.wiggle.proto.WiggleControlPlaneGrpc;
import com.wiggle.proto.WorkflowDefinition;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.InsecureServerCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
    private final LiveCensus census;
    private final Server server;
    private final ExecutorService pool;
    private volatile boolean started;
    // Channels to cells, for definition fan-out / seeding (the coordinator is a client of each cell).
    private final Map<String, ManagedChannel> cellChannels = new ConcurrentHashMap<>();
    private final Map<String, WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub> cellStubs = new ConcurrentHashMap<>();

    public CoordinatorApi(CoordinatorStore store, int port, Tls.Options tls) throws IOException {
        this(store, port, tls, new LiveCensus());
    }

    /** Shares a {@link LiveCensus} with the reconciler, so heartbeat reports drive epoch retire (R21). */
    public CoordinatorApi(CoordinatorStore store, int port, Tls.Options tls, LiveCensus census) throws IOException {
        this.store = store;
        this.census = census;
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
        for (ManagedChannel ch : cellChannels.values()) ch.shutdownNow();
        cellChannels.clear();
        cellStubs.clear();
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
        run(resp, () -> doFetchConfig(req.getNamespace(), cellOrNamespace(req.getNamespace(), req.getNode().getCellId())));
    }

    @Override public void register(RegisterRequest req, StreamObserver<RegisterResponse> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc Register namespace=" + req.getNamespace()
                + " node=" + req.getNode().getName());
        run(resp, () -> doRegister(req.getNamespace(), req.getNode()));
    }

    @Override public void heartbeat(CoordinatorHeartbeatRequest req, StreamObserver<CoordinatorHeartbeatResponse> resp) {
        run(resp, () -> doHeartbeat(req.getNodeId(), req.getConfigGeneration(),
                toLongMap(req.getHealth().getLiveByEpochMap())));
    }

    @Override public void deregister(DeregisterRequest req, StreamObserver<Empty> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc Deregister node=" + req.getNodeId());
        run(resp, () -> {
            doDeregister(req.getNodeId());
            return Empty.getDefaultInstance();
        });
    }

    private static Map<Long, Long> toLongMap(Map<Long, Integer> m) {
        if (m.isEmpty()) return Map.of();
        Map<Long, Long> out = new LinkedHashMap<>();
        for (Map.Entry<Long, Integer> e : m.entrySet()) out.put(e.getKey(), e.getValue().longValue());
        return out;
    }

    // ---- node-lifecycle logic (directly unit-testable) ----

    /** Convenience for the single-cell case (the cell id is the namespace). */
    public NodeConfig doFetchConfig(String namespace) {
        return doFetchConfig(namespace, namespace);
    }

    /**
     * The config for a node in {@code cellId} of {@code namespace}: the current config generation (the
     * policy revision) and the node's placement -- the epoch it mints into and the shards its cell owns.
     * No storage/tuning overlay yet (that arrives with provisioning, T13); {@code generation} lets a node
     * observe policy change (an epoch bump or ring edit) and re-fetch its placement.
     */
    public NodeConfig doFetchConfig(String namespace, String cellId) {
        long generation = store.getPolicy(namespace).map(CoordPolicy::revision).orElse(0L);
        Placement pl = placementFor(namespace, cellId);
        return NodeConfig.newBuilder()
                .setNamespace(namespace)
                .setGeneration(generation)
                .setExpected(Expected.newBuilder().build())
                .setTtlSeconds(DEFAULT_TTL_S)
                .setEpoch(pl.epoch())
                .addAllShards(pl.shards())
                .build();
    }

    /** Records a node in the roster and returns its coordinator-assigned id and initial placement. */
    public RegisterResponse doRegister(String namespace, RegisteredNode node) {
        // Seed the namespace's definitions onto the joining node BEFORE it enters the roster, so it is
        // never resolvable while missing a graph (R23 "seed before eligible").
        seedNewNode(namespace, node.getEndpoint());
        String nodeId = UUID.randomUUID().toString();
        // A node's cell defaults to the namespace itself (the single-cell case), so a node that does not
        // yet know its cell registers into the namespace's one implicit cell.
        String cellId = cellOrNamespace(namespace, node.getCellId());
        String fingerprint = emptyToNull(node.getCellFingerprint());
        rejectCellIdCollision(namespace, cellId, fingerprint);
        store.upsertNode(new CoordNode(nodeId, namespace, cellId, node.getEndpoint(), emptyToNull(node.getRegion()),
                node.getEngineVersion(), fingerprint, 0, System.currentTimeMillis()));
        Placement pl = placementFor(namespace, cellId);
        return RegisterResponse.newBuilder()
                .setNodeId(nodeId)
                .setHeartbeatIntervalSeconds(NODE_HEARTBEAT_INTERVAL_SECONDS)
                .setEpoch(pl.epoch())
                .addAllShards(pl.shards())
                .build();
    }

    private static String cellOrNamespace(String namespace, String cellId) {
        return cellId == null || cellId.isEmpty() ? namespace : cellId;
    }

    /**
     * Rejects a node whose {@code cellId} is already bound to a <em>different</em> cell (a distinct
     * shared storage) in this namespace: nodes of the same cell share a fingerprint and pass, so only a
     * genuine collision -- two independent cells reusing an id -- is refused. Guarded only when both the
     * joining node and an existing one carry a fingerprint, so an in-memory/legacy node (null print)
     * never trips it. The binding is implicit in the live roster, so it clears once a cell fully drains
     * and the id becomes reusable.
     */
    private void rejectCellIdCollision(String namespace, String cellId, String fingerprint) {
        if (fingerprint == null) return;
        for (CoordNode existing : store.nodes(namespace)) {
            if (cellId.equals(existing.cellId()) && existing.cellFingerprint() != null
                    && !fingerprint.equals(existing.cellFingerprint())) {
                throw new IllegalArgumentException("cell id '" + cellId + "' in namespace '" + namespace
                        + "' is already bound to a different cell (fingerprint " + existing.cellFingerprint()
                        + "); a node from another cell must use a distinct WIGGLE_CELL_ID");
            }
        }
    }

    /**
     * The placement a node in {@code cellId} mints into: the current epoch and the shards its cell owns
     * in that epoch's ring. With no ring configured (the single implicit cell) this is epoch 0, shard 0
     * -- identical to pre-ring behaviour (R1). A cell that is not (yet) in the ring also gets the genesis
     * shard so it can still mint; the ring must name it before instances there are addressable.
     */
    private Placement placementFor(String namespace, String cellId) {
        CoordPolicy policy = store.getPolicy(namespace).orElse(null);
        if (policy == null) return new Placement(0, List.of(0));
        long epoch = policy.currentEpoch();
        CoordPolicy.EpochRing er = policy.epochs().get(epoch);
        if (er == null) return new Placement(epoch, List.of(0));
        List<Integer> shards = new ArrayList<>();
        for (CoordPolicy.RingSlot s : er.ring()) if (cellId.equals(s.cellId())) shards.add(s.shard());
        return new Placement(epoch, shards.isEmpty() ? List.of(0) : shards);
    }

    private record Placement(long epoch, List<Integer> shards) {}

    /** Convenience for callers/tests that report no census. */
    public CoordinatorHeartbeatResponse doHeartbeat(String nodeId, long observedGeneration) {
        return doHeartbeat(nodeId, observedGeneration, Map.of());
    }

    /**
     * Touches the node's liveness and records its live-by-epoch census (for retire); returns
     * {@code ok=false} for an unknown node (it should re-register). The returned generation lets the
     * node notice a policy change (e.g. an epoch retire) and re-fetch its placement.
     */
    public CoordinatorHeartbeatResponse doHeartbeat(String nodeId, long observedGeneration, Map<Long, Long> liveByEpoch) {
        Optional<CoordNode> node = store.touchNode(nodeId, System.currentTimeMillis(), observedGeneration);
        if (node.isEmpty()) {
            return CoordinatorHeartbeatResponse.newBuilder().setOk(false).build();
        }
        CoordNode n = node.get();
        census.record(nodeId, n.namespace(), n.cellId(), liveByEpoch, System.currentTimeMillis());
        long generation = store.getPolicy(n.namespace()).map(CoordPolicy::revision).orElse(0L);
        return CoordinatorHeartbeatResponse.newBuilder().setOk(true).setConfigGeneration(generation).build();
    }

    /** Removes a node from the roster and forgets its census report. */
    public void doDeregister(String nodeId) {
        store.removeNode(nodeId);
        census.forget(nodeId);
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
     * <p>By instance id: the id carries its epoch and shard ({@link IdCodec}); the shard is looked up in
     * that epoch's ring to find the owning cell, and the cell's live nodes give the dial address -- so an
     * instance always resolves to the cell that holds it, even across many cells. By namespace (a new
     * start): the current epoch's first ring slot is used. When no ring is configured for the epoch, this
     * falls back to the namespace's whole roster (the single implicit cell), so pre-ring usage is
     * unchanged (R1).
     */
    public ResolveResponse doResolve(ResolveRequest req) {
        String namespace;
        long epoch;
        int shard;   // -1 => "any" (namespace resolve, no specific instance)
        if (req.getByCase() == ResolveRequest.ByCase.INSTANCE_ID) {
            IdCodec.Placement p = IdCodec.parse(req.getInstanceId()).orElseThrow(() ->
                    new IllegalArgumentException("cannot route a legacy instance id ('" + req.getInstanceId()
                            + "'); resolve by namespace instead"));
            namespace = p.namespace();
            epoch = p.epoch();
            shard = (int) p.shard();
        } else {
            namespace = req.getNamespace();
            epoch = store.getPolicy(namespace).map(CoordPolicy::currentEpoch).orElse(0L);
            shard = -1;
        }
        String cellId = cellFor(namespace, epoch, shard);
        Endpoint endpoint = cellId == null
                ? endpointFor(namespace, emptyToNull(req.getCallerRegion()))
                : endpointForCell(namespace, cellId, emptyToNull(req.getCallerRegion()));
        return ResolveResponse.newBuilder()
                .setNamespace(namespace)
                .setEpoch(epoch)
                .setEndpoint(endpoint)
                .setTtlSeconds(RESOLVE_TTL_S)
                .build();
    }

    /**
     * The cells hosting live work for a namespace -- every cell that appears in an OPEN or DRAINING
     * epoch ring -- plus a change generation. A worker polls all of them, so an epoch that is draining
     * keeps being polled until its work finishes and it retires. With no ring configured, this is the
     * single implicit cell (the whole roster), unchanged from pre-ring usage (R1).
     */
    public ActiveCellsResponse doActiveCells(String namespace, String callerRegion) {
        Optional<CoordPolicy> policy = store.getPolicy(namespace);
        long generation = policy.map(CoordPolicy::revision).orElse(0L);
        ActiveCellsResponse.Builder b = ActiveCellsResponse.newBuilder()
                .setGeneration(generation)
                .setTtlSeconds(RESOLVE_TTL_S);
        List<String> activeCells = activeCellIds(policy.orElse(null));
        if (activeCells.isEmpty()) {
            b.addCells(endpointFor(namespace, callerRegion));   // single implicit cell (no ring)
        } else {
            for (String cellId : activeCells) {
                Endpoint e = endpointForCellOrNull(namespace, cellId, callerRegion);
                if (e != null) b.addCells(e);
            }
        }
        return b.build();
    }

    /** Cell ids that appear in any OPEN or DRAINING epoch ring, de-duplicated in ring order. */
    private static List<String> activeCellIds(CoordPolicy policy) {
        List<String> out = new ArrayList<>();
        if (policy == null) return out;
        for (CoordPolicy.EpochRing er : policy.epochs().values()) {
            if (er.status() == CoordPolicy.EpochStatus.RETIRED) continue;
            for (CoordPolicy.RingSlot s : er.ring()) {
                if (!out.contains(s.cellId())) out.add(s.cellId());
            }
        }
        return out;
    }

    /**
     * The cell that owns {@code shard} in {@code epoch}, or {@code null} when no ring is configured (the
     * single-implicit-cell case). {@code shard < 0} (a namespace resolve) selects the first ring slot.
     */
    private String cellFor(String namespace, long epoch, int shard) {
        CoordPolicy policy = store.getPolicy(namespace).orElse(null);
        if (policy == null) return null;
        CoordPolicy.EpochRing er = policy.epochs().get(epoch);
        if (er == null || er.ring().isEmpty()) return null;
        List<CoordPolicy.RingSlot> ring = er.ring();
        if (shard < 0) return ring.get(0).cellId();
        for (CoordPolicy.RingSlot s : ring) if (s.shard() == shard) return s.cellId();
        return ring.get(Math.floorMod(shard, ring.size())).cellId();   // ring smaller than shard space
    }

    // ---- definition fan-out (R23) ----

    @Override public void registerWorkflow(RegisterWorkflowRequest req, StreamObserver<RegisterWorkflowResponse> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc RegisterWorkflow namespace=" + req.getNamespace()
                + " name=" + req.getName());
        run(resp, () -> doRegisterWorkflow(req.getNamespace(), req.getName(), req.getDefinition().toByteArray()));
    }

    @Override public void deregisterWorkflow(DeregisterWorkflowRequest req, StreamObserver<DeregisterWorkflowResponse> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc DeregisterWorkflow namespace=" + req.getNamespace()
                + " name=" + req.getName());
        run(resp, () -> doDeregisterWorkflow(req.getNamespace(), req.getName()));
    }

    @Override public void listWorkflows(ListWorkflowsRequest req, StreamObserver<ListWorkflowsResponse> resp) {
        run(resp, () -> doListWorkflows(req.getNamespace()));
    }

    /**
     * Deallocates a workflow from a namespace: removes its allocation from the coordinator's registry,
     * so it is no longer fanned out to cells that join later. Definitions already compiled on running
     * cells stay (the engine's definition history is append-only); a cell drops it on restart when it
     * is no longer seeded. Idempotent -- {@code removed=false} if it was not allocated.
     */
    public DeregisterWorkflowResponse doDeregisterWorkflow(String namespace, String name) {
        boolean removed = store.removeDefinition(namespace, name);
        if (removed) {
            LOG.log(System.Logger.Level.INFO, () -> "deallocated workflow '" + name + "' from namespace '" + namespace + "'");
        }
        return DeregisterWorkflowResponse.newBuilder().setRemoved(removed).build();
    }

    /** The workflows currently allocated to a namespace. */
    public ListWorkflowsResponse doListWorkflows(String namespace) {
        ListWorkflowsResponse.Builder b = ListWorkflowsResponse.newBuilder();
        for (CoordDefinition d : store.definitions(namespace)) {
            b.addWorkflows(AllocatedWorkflow.newBuilder()
                    .setName(d.name()).setVersion(d.version()).setRegisteredAt(d.registeredAt()).build());
        }
        return b.build();
    }

    /**
     * Registers a workflow across every cell of a namespace and records it in the definition registry.
     * Content-hash versioning makes this idempotent -- the same definition yields the same version on
     * every cell, so a replay is a no-op.
     */
    public RegisterWorkflowResponse doRegisterWorkflow(String namespace, String name, byte[] definitionJson) {
        List<CoordNode> nodes = store.nodes(namespace);
        if (nodes.isEmpty()) {
            throw new IllegalStateException("no cell for namespace '" + namespace + "' to register '" + name + "'");
        }
        Struct struct = ProtoJson.toStruct(Json.parseObject(new String(definitionJson, StandardCharsets.UTF_8)));
        WorkflowDefinition wd = WorkflowDefinition.newBuilder().setDefinition(struct).build();
        int version = 0;
        int seeded = 0;
        for (CoordNode n : nodes) {
            RegisterWorkflowResult r = cellStub(n.endpoint()).registerWorkflow(wd);
            version = Integer.parseInt(r.getVersion());
            seeded++;
        }
        store.putDefinition(new CoordDefinition(namespace, name, version, sha256(definitionJson),
                System.currentTimeMillis()));
        int fanned = seeded;
        int v = version;
        LOG.log(System.Logger.Level.INFO, () -> "fanned out workflow '" + name + "' v" + v
                + " to " + fanned + " cell node(s) of namespace '" + namespace + "'");
        return RegisterWorkflowResponse.newBuilder().setVersion(version).setCellsSeeded(seeded).build();
    }

    /**
     * Copies the namespace's registered definitions onto a joining node from a healthy sibling, so a
     * new cell holds every graph before it can host instances. Best-effort per definition; a first
     * node (no sibling) or an empty registry is a no-op.
     */
    private void seedNewNode(String namespace, String newEndpoint) {
        List<CoordDefinition> defs = store.definitions(namespace);
        if (defs.isEmpty()) return;
        List<CoordNode> siblings = store.nodes(namespace);
        // self is not in the roster yet
        if (siblings.isEmpty()) return;
        String sibling = siblings.getFirst().endpoint();
        for (CoordDefinition d : defs) {
            try {
                WorkflowDefinition wd = cellStub(sibling)
                        .getWorkflow(GetWorkflowRequest.newBuilder().setName(d.name()).build());
                cellStub(newEndpoint).registerWorkflow(wd);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING,
                        "seeding '" + d.name() + "' onto " + newEndpoint + " failed (best-effort): " + e);
            }
        }
    }

    private WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub cellStub(String endpoint) {
        String t = strip(endpoint);
        cellChannels.computeIfAbsent(t, e -> Grpc.newChannelBuilder(e, InsecureChannelCredentials.create()).build());
        return cellStubs.computeIfAbsent(t, e -> WiggleControlPlaneGrpc.newBlockingStub(cellChannels.get(e)));
    }

    private static String strip(String target) {
        int i = target.indexOf("://");
        return i < 0 ? target : target.substring(i + 3);
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder sb = new StringBuilder(h.length * 2);
            for (byte b : h) sb.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(java.util.Arrays.hashCode(bytes));
        }
    }

    /** Builds a cell endpoint from the namespace's live roster, preferring the caller's region. */
    private Endpoint endpointFor(String namespace, String callerRegion) {
        Endpoint e = buildEndpoint(store.nodes(namespace), callerRegion);
        if (e == null) throw new IllegalStateException("no live nodes for namespace '" + namespace + "'");
        return e;
    }

    /** Builds the endpoint for one cell of a namespace; throws if that cell has no live nodes. */
    private Endpoint endpointForCell(String namespace, String cellId, String callerRegion) {
        Endpoint e = endpointForCellOrNull(namespace, cellId, callerRegion);
        if (e == null) {
            throw new IllegalStateException("no live nodes for cell '" + cellId + "' of namespace '" + namespace + "'");
        }
        return e;
    }

    /** Builds the endpoint for one cell of a namespace, or {@code null} if that cell has no live nodes. */
    private Endpoint endpointForCellOrNull(String namespace, String cellId, String callerRegion) {
        List<CoordNode> inCell = new ArrayList<>();
        for (CoordNode n : store.nodes(namespace)) if (cellId.equals(n.cellId())) inCell.add(n);
        return buildEndpoint(inCell, callerRegion);
    }

    /** Region-filters a node set (R24) and renders it as an {@link Endpoint}, or {@code null} if empty. */
    private static Endpoint buildEndpoint(List<CoordNode> nodes, String callerRegion) {
        if (callerRegion != null) {
            List<CoordNode> regional = new ArrayList<>();
            for (CoordNode n : nodes) if (callerRegion.equals(n.region())) regional.add(n);
            if (!regional.isEmpty()) nodes = regional;   // same cell, region-appropriate addresses (R24)
        }
        if (nodes.isEmpty()) return null;
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
