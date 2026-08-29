package dev.wiggle.client;

import com.google.protobuf.ByteString;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.core.IdCodec;
import dev.wiggle.core.Json;
import dev.wiggle.core.Tls;
import dev.wiggle.proto.ActiveCellsRequest;
import dev.wiggle.proto.ActiveCellsResponse;
import dev.wiggle.proto.CellCoordinatorGrpc;
import dev.wiggle.proto.Endpoint;
import dev.wiggle.proto.RegisterWorkflowRequest;
import dev.wiggle.proto.ResolveRequest;
import dev.wiggle.proto.ResolveResponse;
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

    private final Map<String, Cached> byNamespace = new ConcurrentHashMap<>();
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

    /** A client for the cell that owns {@code instanceId} (routed by the namespace in its id). */
    public WiggleClient clientForInstance(String instanceId) {
        if (coordinatorUrl == null) return clientFor(staticTarget);
        String namespace = IdCodec.parse(instanceId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "cannot route a legacy instance id ('" + instanceId + "') under a coordinator"))
                .namespace();
        return clientFor(resolveNamespace(namespace).getTarget());
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

    /** The cells hosting live work for a namespace (a worker polls all of them). */
    public List<String> activeCellTargets(String namespace) {
        if (coordinatorUrl == null) return List.of(staticTarget);
        ActiveCellsResponse r = coord.activeCells(ActiveCellsRequest.newBuilder()
                .setNamespace(namespace).setCallerRegion(nz(callerRegion)).build());
        List<String> targets = new ArrayList<>();
        for (Endpoint e : r.getCellsList()) targets.add(e.getTarget());
        return targets;
    }

    /** Drop a cached resolution -- call after a cell RPC fails with UNAVAILABLE/NOT_FOUND. */
    public void invalidate(String namespace) {
        byNamespace.remove(namespace);
    }

    private Endpoint resolveNamespace(String namespace) {
        if (coordinatorUrl == null) {
            return Endpoint.newBuilder().setTarget(staticTarget).build();
        }
        Cached c = byNamespace.get(namespace);
        if (c != null && System.nanoTime() < c.expiryNanos()) return c.endpoint();
        ResolveResponse r = coord.resolve(ResolveRequest.newBuilder()
                .setNamespace(namespace).setCallerRegion(nz(callerRegion)).build());
        Endpoint e = r.getEndpoint();
        long ttlNanos = Math.max(1, e.getTtlSeconds()) * 1_000_000_000L;
        byNamespace.put(namespace, new Cached(e, System.nanoTime() + ttlNanos));
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
