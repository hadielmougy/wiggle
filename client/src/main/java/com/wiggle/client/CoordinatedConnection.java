package com.wiggle.client;

import com.google.protobuf.ByteString;
import com.wiggle.client.dsl.Blueprint;
import com.wiggle.core.IdCodec;
import com.wiggle.core.Json;
import com.wiggle.core.Tls;
import com.wiggle.proto.ActiveCellsRequest;
import com.wiggle.proto.ActiveCellsResponse;
import com.wiggle.proto.AllocatedWorkflow;
import com.wiggle.proto.CellCoordinatorGrpc;
import com.wiggle.proto.DeregisterWorkflowRequest;
import com.wiggle.proto.Endpoint;
import com.wiggle.proto.ListWorkflowsRequest;
import com.wiggle.proto.OpenEpochRequest;
import com.wiggle.proto.Policy;
import com.wiggle.proto.RegisterWorkflowRequest;
import com.wiggle.proto.ResolveRequest;
import com.wiggle.proto.ResolveResponse;
import com.wiggle.proto.RingSlot;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

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

    /** Registers a workflow for a namespace: the coordinator fans the definition out to every cell of
     *  the namespace (R23). */
    public void registerWorkflow(String namespace, Blueprint blueprint) {
        String json = Json.write(blueprint.definition().toJson());
        coord.registerWorkflow(RegisterWorkflowRequest.newBuilder()
                .setNamespace(namespace)
                .setName(blueprint.name())
                .setDefinition(ByteString.copyFromUtf8(json))
                .build());
    }

    /**
     * Deallocates a workflow from a namespace. Returns whether it was allocated. Definitions already
     * compiled on running cells remain until they restart; this stops the flow being fanned out to
     * cells that join later.
     */
    public boolean deregisterWorkflow(String namespace, String name) {
        return coord.deregisterWorkflow(DeregisterWorkflowRequest.newBuilder()
                .setNamespace(namespace).setName(name).build()).getRemoved();
    }

    /**
     * Opens a new placement epoch for a namespace: publishes a {@code shard -> cell} ring (a reshard),
     * marking the previous epoch draining. Returns the resulting policy.
     */
    public Policy openEpoch(String namespace, List<RingSlot> ring) {
        return coord.openEpoch(OpenEpochRequest.newBuilder()
                .setNamespace(namespace).addAllRing(ring).build());
    }

    /** The workflows currently allocated to a namespace. */
    public List<AllocatedWorkflow> listWorkflows(String namespace) {
        return coord.listWorkflows(ListWorkflowsRequest.newBuilder()
                .setNamespace(namespace).build()).getWorkflowsList();
    }

    /** The cells hosting live work for a namespace (a worker polls all of them). */
    public List<String> activeCellTargets(String namespace) {
        ActiveCellsResponse r = coord.activeCells(ActiveCellsRequest.newBuilder()
                .setNamespace(namespace).setCallerRegion(nz(callerRegion)).build());
        List<String> targets = new ArrayList<>();
        for (Endpoint e : r.getCellsList()) targets.add(rewriteTarget(e.getTarget()));
        return targets;
    }

    /** Drop cached instance-resolutions for a namespace -- call after a cell RPC fails with
     *  UNAVAILABLE/NOT_FOUND so the next operate-by-id re-resolves. */
    public void invalidate(String namespace) {
        byShard.keySet().removeIf(k -> k.startsWith(namespace + "|"));
    }

    /** Resolves where a NEW instance of a namespace should start. Not cached: the coordinator spreads new
     *  starts across the ring, so resolving per start is what distributes them across cells/shards. */
    private Endpoint resolveNamespace(String namespace) {
        return coord.resolve(ResolveRequest.newBuilder()
                .setNamespace(namespace).setCallerRegion(nz(callerRegion)).build()).getEndpoint();
    }

    /** Resolves the cell that owns an existing instance, by its baked-in epoch+shard. Cached by
     *  (namespace, epoch, shard) -- bounded, since every instance on a shard shares one cell. */
    private Endpoint resolveInstance(String instanceId) {
        IdCodec.Placement p = IdCodec.parse(instanceId).orElseThrow(() -> new IllegalArgumentException(
                "cannot route a legacy instance id ('" + instanceId + "') under a coordinator"));
        String key = p.namespace() + "|e" + p.epoch() + "|s" + p.shard();
        Cached c = byShard.get(key);
        if (c != null && System.nanoTime() < c.expiryNanos()) return c.endpoint();
        ResolveResponse r = coord.resolve(ResolveRequest.newBuilder()
                .setInstanceId(instanceId).setCallerRegion(nz(callerRegion)).build());
        Endpoint e = r.getEndpoint();
        long ttlNanos = Math.max(1, e.getTtlSeconds()) * 1_000_000_000L;
        byShard.put(key, new Cached(e, System.nanoTime() + ttlNanos));
        return e;
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
