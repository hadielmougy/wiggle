package com.wiggle.client;

import com.google.protobuf.ByteString;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.placement.IdCodec;
import com.wiggle.core.Json;
import com.wiggle.core.Tls;
import com.wiggle.proto.*;
import io.github.shield.internal.RetriesExhaustedException;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A connection to a sharded namespace, routed through the coordinator. Client calls are directed to
 * the cell that owns a namespace or instance via the coordinator's {@code Resolve} / {@code ActiveCells};
 * resolutions are cached by TTL and per-cell {@link WiggleClient}s are reused. It also carries the
 * coordinator-only admin operations (register / open-epoch / list). Obtain one via
 * {@link WiggleConnection#coordinator}.
 *
 * <p>Instance routing is directory-free: an instance id carries its namespace ({@link IdCodec}), so
 * {@link #clientForInstance} resolves by namespace without any per-instance lookup.
 */
public final class CoordinatedConnection implements AutoCloseable {

    private final Tls.Options tls;
    private final ManagedChannel coordChannel;
    private final CellCoordinatorGrpc.CellCoordinatorBlockingStub coord;
    private final String callerRegion;

    private final Map<String, Cached> byShard = new ConcurrentHashMap<>();   // (ns|epoch|shard) -> cell, for instance ops
    private final Map<String, WiggleClient> clients = new ConcurrentHashMap<>();

    // Applied to every resolved cell target before connecting; identity by default (see EndpointRewriter).
    private volatile EndpointRewriter endpointRewriter = EndpointRewriter.fromEnv();

    private record Cached(Endpoint endpoint, long expiryNanos) {}

    CoordinatedConnection(String coordinatorUrl, Tls.Options tls, String callerRegion) {
        this.tls = tls == null ? Tls.Options.DISABLED : tls;
        this.callerRegion = callerRegion;
        this.coordChannel = Grpc.newChannelBuilder(WiggleConnection.strip(coordinatorUrl),
                InsecureChannelCredentials.create()).build();
        this.coord = CellCoordinatorGrpc.newBlockingStub(coordChannel);
    }

    /**
     * Override the address of every resolved cell before connecting -- a testing seam for when the
     * coordinator advertises an address unreachable from where the client runs (e.g. a Kubernetes pod IP,
     * redirected to a {@code kubectl port-forward}). Passing {@code null} restores identity. By default a
     * rewriter is loaded from {@code wiggle.endpointRewrite} / {@code WIGGLE_ENDPOINT_REWRITE}. See
     * {@link EndpointRewriter}.
     */
    public CoordinatedConnection withEndpointRewriter(EndpointRewriter rewriter) {
        this.endpointRewriter = rewriter == null ? EndpointRewriter.identity() : rewriter;
        return this;
    }

    /** A client for the cell that hosts new instances of {@code namespace}. */
    public WiggleClient clientForNamespace(String namespace) {
        return clientFor(resolveNamespace(namespace).getTarget());
    }

    /** A client for the cell that owns {@code instanceId}, routed by the id's own epoch+shard
     *  (self-routing) -- not by namespace, so an instance on any cell resolves to the cell that holds it. */
    public WiggleClient clientForInstance(String instanceId) {
        return clientFor(resolveInstance(instanceId).getTarget());
    }

    /**
     * The address of the cell that owns the observed run of {@code workflow} keyed by {@code key}:
     * where every reporter of that run must send its steps, so they land on one instance. Cached
     * by the key's shard for the coordinator's TTL, like instances are. The observe module opens
     * its own channel to it; this connection's clients are not involved.
     */
    public String targetForRunKey(String namespace, String workflow, String key) {
        String cacheKey = namespace + "|k" + IdCodec.runKeyShard(workflow, key);
        Cached c = byShard.get(cacheKey);
        if (c != null && System.nanoTime() < c.expiryNanos()) return rewriteTarget(c.endpoint().getTarget());
        ResolveResponse r = coordCall(() -> coord.resolve(ResolveRequest.newBuilder()
                .setRunKey(RunKey.newBuilder().setNamespace(namespace).setWorkflow(workflow).setKey(key))
                .setCallerRegion(nz(callerRegion)).build()));
        Endpoint e = r.getEndpoint();
        long ttlNanos = Math.max(1, e.getTtlSeconds()) * 1_000_000_000L;
        byShard.put(cacheKey, new Cached(e, System.nanoTime() + ttlNanos));
        return rewriteTarget(e.getTarget());
    }

    /** Registers a workflow for a namespace: the coordinator fans the definition out to every cell of
     *  the namespace (R23). */
    public void registerWorkflow(String namespace, FlowSpec flowSpec) {
        String json = Json.write(flowSpec.definition().toJson());
        try {
            coordCall(() -> coord.registerWorkflow(RegisterWorkflowRequest.newBuilder()
                    .setNamespace(namespace)
                    .setName(flowSpec.name())
                    .setDefinition(ByteString.copyFromUtf8(json))
                    .build()));
        }  catch (Exception e) {
            throw new WorkflowRegistrationException("Flow can't be registered", e);
        }
    }

    /**
     * Deallocates a workflow from a namespace. Returns whether it was allocated. Definitions already
     * compiled on running cells remain until they restart; this stops the flow being fanned out to
     * cells that join later.
     */
    public boolean deregisterWorkflow(String namespace, String name) {
        return coordCall(() -> coord.deregisterWorkflow(DeregisterWorkflowRequest.newBuilder()
                .setNamespace(namespace).setName(name).build())).getRemoved();
    }

    /**
     * Opens a new placement epoch for a namespace: publishes a {@code shard -> cell} ring (a reshard),
     * marking the previous epoch draining. Returns the resulting policy.
     */
    public Policy openEpoch(String namespace, List<RingSlot> ring) {
        return coordCall(() -> coord.openEpoch(OpenEpochRequest.newBuilder()
                .setNamespace(namespace).addAllRing(ring).build()));
    }

    /** The workflows currently allocated to a namespace. */
    public List<AllocatedWorkflow> listWorkflows(String namespace) {
        return coordCall(() -> coord.listWorkflows(ListWorkflowsRequest.newBuilder()
                .setNamespace(namespace).build())).getWorkflowsList();
    }

    /** The cells hosting live work for a namespace (a worker polls all of them). */
    public List<String> activeCellTargets(String namespace) {
        ActiveCellsResponse r = coordCall(() -> coord.activeCells(ActiveCellsRequest.newBuilder()
                .setNamespace(namespace).setCallerRegion(nz(callerRegion)).build()));
        List<String> targets = new ArrayList<>();
        for (Endpoint e : r.getCellsList()) targets.add(rewriteTarget(e.getTarget()));
        return targets;
    }

    /** Drop cached instance-resolutions for a namespace -- call after a cell RPC fails with
     *  UNAVAILABLE/NOT_FOUND so the next operate-by-id re-resolves. */
    // docs:begin invalidate
    public void invalidate(String namespace) {
        byShard.keySet().removeIf(k -> k.startsWith(namespace + "|"));
    }
    // docs:end invalidate

    /** Resolves where a NEW instance of a namespace should start. Not cached: the coordinator spreads new
     *  starts across the ring, so resolving per start is what distributes them across cells/shards. */
    private Endpoint resolveNamespace(String namespace) {
        return coordCall(() -> coord.resolve(ResolveRequest.newBuilder()
                .setNamespace(namespace).setCallerRegion(nz(callerRegion)).build())).getEndpoint();
    }

    /**
     * Resolves the cell that owns an existing instance, from what its id bakes in. Cached by
     * (namespace, cell, epoch, shard) -- bounded, since every instance sharing those shares one cell.
     *
     * <p>The cell belongs in the key even though a ring makes it derivable. Under a ring,
     * {@code (namespace, epoch, shard)} names exactly one cell and the label adds nothing. But a node
     * that has not yet heard from the coordinator mints into the genesis default -- epoch 0, shard 0
     * -- so two cells in one namespace, started while the coordinator is unreachable, mint ids that
     * differ only by their label. Keyed without it, the first to populate the cache would answer for
     * both until the TTL expired. Unlabelled ids key exactly as they did before.
     */
    // docs:begin resolve
    private Endpoint resolveInstance(String instanceId) {
        IdCodec.Placement p = IdCodec.parse(instanceId).orElseThrow(() -> new IllegalArgumentException(
                "cannot route a legacy instance id ('" + instanceId + "') under a coordinator"));
        String key = p.namespace() + "|c" + (p.cellId() == null ? "" : p.cellId())
                + "|e" + p.epoch() + "|s" + p.shard();
        Cached c = byShard.get(key);
        if (c != null && System.nanoTime() < c.expiryNanos()) return c.endpoint();
        ResolveResponse r = coordCall(() -> coord.resolve(ResolveRequest.newBuilder()
                .setInstanceId(instanceId).setCallerRegion(nz(callerRegion)).build()));
        Endpoint e = r.getEndpoint();
        long ttlNanos = Math.max(1, e.getTtlSeconds()) * 1_000_000_000L;
        byShard.put(key, new Cached(e, System.nanoTime() + ttlNanos));
        return e;
    }
    // docs:end resolve

    /**
     * Runs a coordinator RPC with the shared UNAVAILABLE retry ({@link RpcRetry}) — so resolution and
     * registration ride out a coordinator restart / failover the same way cell calls do. On exhaustion
     * it rethrows the underlying {@link StatusRuntimeException}, so callers see the same exception type
     * as an un-retried call (nothing new leaks out).
     */
    private <T> T coordCall(java.util.function.Supplier<T> op) {
        try {
            return RpcRetry.retrying(op);
        } catch (RetriesExhaustedException e) {
            StatusRuntimeException s = RpcRetry.lastStatus(e);
            if (s != null) throw s;
            throw e;
        }
    }

    private WiggleClient clientFor(String target) {
        return clients.computeIfAbsent(rewriteTarget(target), t -> new WiggleClient(t, tls));
    }

    /** Strip any scheme, apply the endpoint rewriter, strip again (a replacement may carry a scheme). */
    private String rewriteTarget(String target) {
        return WiggleConnection.strip(endpointRewriter.rewrite(WiggleConnection.strip(target)));
    }

    @Override public void close() {
        for (WiggleClient c : clients.values()) c.close();
        clients.clear();
        coordChannel.shutdownNow();
    }

    private static String nz(String s) { return s == null ? "" : s; }
}
