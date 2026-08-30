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
 * Routes client calls to the cell that owns a namespace or instance, via the coordinator's
 * {@code Resolve} / {@code ActiveCells}. Resolutions are cached by TTL and per-cell
 * {@link WiggleClient}s are reused. When no coordinator is configured it is a pass-through to a single
 * static target -- so existing (non-sharded) usage is unchanged (R1).
 *
 * <p>Instance routing is directory-free: an instance id carries its namespace ({@link IdCodec}), so
 * {@link #clientForInstance} resolves by namespace without any per-instance lookup.
 */
public final class CellResolver implements AutoCloseable {

    private final String coordinatorUrl;         // null in direct (no-coordinator) mode
    private final String staticTarget;           // used in direct mode
    private final Tls.Options tls;
    private final ManagedChannel coordChannel;   // null in direct mode
    private final CellCoordinatorGrpc.CellCoordinatorBlockingStub coord;
    private final String callerRegion;

    private final Map<String, Cached> byShard = new ConcurrentHashMap<>();   // (ns|epoch|shard) -> cell, for instance ops
    private final Map<String, WiggleClient> clients = new ConcurrentHashMap<>();

    private record Cached(Endpoint endpoint, long expiryNanos) {}

    private CellResolver(String coordinatorUrl, String staticTarget, Tls.Options tls, String callerRegion) {
        this.coordinatorUrl = coordinatorUrl;
        this.staticTarget = staticTarget;
        this.tls = tls == null ? Tls.Options.DISABLED : tls;
        this.callerRegion = callerRegion;
        if (coordinatorUrl != null) {
            this.coordChannel = Grpc.newChannelBuilder(strip(coordinatorUrl), InsecureChannelCredentials.create()).build();
            this.coord = CellCoordinatorGrpc.newBlockingStub(coordChannel);
        } else {
            this.coordChannel = null;
            this.coord = null;
        }
    }

    /** Coordinator-routed: resolve every call through the coordinator at {@code coordinatorUrl}. */
    public static CellResolver coordinator(String coordinatorUrl, Tls.Options tls, String callerRegion) {
        return new CellResolver(coordinatorUrl, null, tls, callerRegion);
    }

    /** No coordinator: every call goes to {@code staticTarget} (today's behaviour). */
    public static CellResolver direct(String staticTarget, Tls.Options tls) {
        return new CellResolver(null, staticTarget, tls, null);
    }

    /** A client for the cell that hosts new instances of {@code namespace}. */
    public WiggleClient clientForNamespace(String namespace) {
        return clientFor(resolveNamespace(namespace).getTarget());
    }

    /** A client for the cell that owns {@code instanceId}, routed by the id's own epoch+shard
     *  (self-routing) -- not by namespace, so an instance on any cell resolves to the cell that holds it. */
    public WiggleClient clientForInstance(String instanceId) {
        if (coordinatorUrl == null) return clientFor(staticTarget);
        return clientFor(resolveInstance(instanceId).getTarget());
    }

    /**
     * Registers a workflow for a namespace. With a coordinator, this fans the definition out to every
     * cell of the namespace (R23); with no coordinator it registers directly on the static cell.
     */
    public void registerWorkflow(String namespace, Blueprint<?> blueprint) {
        if (coordinatorUrl == null) {
            clientFor(staticTarget).register(blueprint);
            return;
        }
        String json = Json.write(blueprint.definition().toJson());
        coord.registerWorkflow(RegisterWorkflowRequest.newBuilder()
                .setNamespace(namespace)
                .setName(blueprint.name())
                .setDefinition(ByteString.copyFromUtf8(json))
                .build());
    }

    /**
     * Deallocates a workflow from a namespace (coordinator only). Returns whether it was allocated.
     * Definitions already compiled on running cells remain until they restart; this stops the flow
     * being fanned out to cells that join later.
     */
    public boolean deregisterWorkflow(String namespace, String name) {
        requireCoordinator("deregisterWorkflow");
        return coord.deregisterWorkflow(DeregisterWorkflowRequest.newBuilder()
                .setNamespace(namespace).setName(name).build()).getRemoved();
    }

    /**
     * Opens a new placement epoch for a namespace: publishes a {@code shard -> cell} ring (a reshard),
     * marking the previous epoch draining. Coordinator only. Returns the resulting policy.
     */
    public Policy openEpoch(String namespace, List<RingSlot> ring) {
        requireCoordinator("openEpoch");
        return coord.openEpoch(OpenEpochRequest.newBuilder()
                .setNamespace(namespace).addAllRing(ring).build());
    }

    /** The workflows currently allocated to a namespace (coordinator only). */
    public List<AllocatedWorkflow> listWorkflows(String namespace) {
        requireCoordinator("listWorkflows");
        return coord.listWorkflows(ListWorkflowsRequest.newBuilder()
                .setNamespace(namespace).build()).getWorkflowsList();
    }

    private void requireCoordinator(String op) {
        if (coordinatorUrl == null) {
            throw new IllegalStateException(op + " requires a coordinator; this resolver is in direct mode");
        }
    }

    /** The cells hosting live work for a namespace (a worker polls all of them). */
    public List<String> activeCellTargets(String namespace) {
        if (coordinatorUrl == null) return List.of(staticTarget);
        ActiveCellsResponse r = coord.activeCells(ActiveCellsRequest.newBuilder()
                .setNamespace(namespace).setCallerRegion(nz(callerRegion)).build());
        List<String> targets = new ArrayList<>();
        for (Endpoint e : r.getCellsList()) targets.add(e.getTarget());
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
        if (coordinatorUrl == null) {
            return Endpoint.newBuilder().setTarget(staticTarget).build();
        }
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
        return clients.computeIfAbsent(strip(target), t -> new WiggleClient(t, tls));
    }

    @Override public void close() {
        for (WiggleClient c : clients.values()) c.close();
        clients.clear();
        if (coordChannel != null) coordChannel.shutdownNow();
    }

    private static String strip(String target) {
        if (target == null) return null;
        int i = target.indexOf("://");
        return i < 0 ? target : target.substring(i + 3);
    }

    private static String nz(String s) { return s == null ? "" : s; }
}
