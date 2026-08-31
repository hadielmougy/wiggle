package com.wiggle.server.coord;

import com.wiggle.core.Tls;
import com.wiggle.proto.ActiveCellsRequest;
import com.wiggle.proto.ActiveCellsResponse;
import com.wiggle.proto.CellCoordinatorGrpc;
import com.wiggle.proto.DeregisterWorkflowRequest;
import com.wiggle.proto.DeregisterWorkflowResponse;
import com.wiggle.proto.ListWorkflowsRequest;
import com.wiggle.proto.ListWorkflowsResponse;
import com.wiggle.proto.CoordinatorHeartbeatRequest;
import com.wiggle.proto.CoordinatorHeartbeatResponse;
import com.wiggle.proto.DeregisterRequest;
import com.wiggle.proto.Empty;
import com.wiggle.proto.FetchConfigRequest;
import com.wiggle.proto.NodeConfig;
import com.wiggle.proto.OpenEpochRequest;
import com.wiggle.proto.Policy;
import com.wiggle.proto.RegisterRequest;
import com.wiggle.proto.RegisterResponse;
import com.wiggle.proto.RegisterWorkflowRequest;
import com.wiggle.proto.RegisterWorkflowResponse;
import com.wiggle.proto.ResolveRequest;
import com.wiggle.proto.ResolveResponse;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The {@code CellCoordinator} gRPC service: a thin adapter that unwraps requests, delegates to
 * {@link CoordinatorService} for all business logic, and maps failures to gRPC status. It owns only the
 * transport concerns -- the server socket, its executor, and TLS credentials. See
 * {@link CoordinatorService} for the node-lifecycle, resolution, admin, and fan-out logic.
 */
public final class CoordinatorApi extends CellCoordinatorGrpc.CellCoordinatorImplBase implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(CoordinatorApi.class.getName());

    private final CoordinatorService service;
    private final Server server;
    private final ExecutorService pool;
    private volatile boolean started;

    public CoordinatorApi(CoordinatorStore store, int port, Tls.Options tls) throws IOException {
        this(store, port, tls, new LiveCensus());
    }

    /** Shares a {@link LiveCensus} with the reconciler, so heartbeat reports drive epoch retire (R21). */
    public CoordinatorApi(CoordinatorStore store, int port, Tls.Options tls, LiveCensus census) throws IOException {
        this.service = new CoordinatorService(store, census);
        this.pool = Executors.newVirtualThreadPerTaskExecutor();
        this.server = Grpc.newServerBuilderForPort(port, credentials(tls))
                .executor(pool)
                .addService(this)
                .build();
    }

    /** The business logic behind this gRPC facade (node lifecycle, resolution, admin, fan-out). */
    public CoordinatorService service() {
        return service;
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
        service.close();
        pool.shutdownNow();
    }

    // ---- gRPC handlers (unwrap request -> service -> response) ----

    @Override public void openEpoch(OpenEpochRequest req, StreamObserver<Policy> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc OpenEpoch namespace=" + req.getNamespace());
        run(resp, () -> service.doOpenEpoch(req.getNamespace(), req.getRingList()));
    }

    @Override public void fetchConfig(FetchConfigRequest req, StreamObserver<NodeConfig> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc FetchConfig namespace=" + req.getNamespace());
        run(resp, () -> service.doFetchConfig(req.getNamespace(),
                CoordinatorService.cellOrNamespace(req.getNamespace(), req.getNode().getCellId())));
    }

    @Override public void register(RegisterRequest req, StreamObserver<RegisterResponse> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc Register namespace=" + req.getNamespace()
                + " node=" + req.getNode().getName());
        run(resp, () -> service.doRegister(req.getNamespace(), req.getNode()));
    }

    @Override public void heartbeat(CoordinatorHeartbeatRequest req, StreamObserver<CoordinatorHeartbeatResponse> resp) {
        run(resp, () -> service.doHeartbeat(req.getNodeId(), req.getConfigGeneration(),
                toLongMap(req.getHealth().getLiveByEpochMap())));
    }

    @Override public void deregister(DeregisterRequest req, StreamObserver<Empty> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc Deregister node=" + req.getNodeId());
        run(resp, () -> {
            service.doDeregister(req.getNodeId());
            return Empty.getDefaultInstance();
        });
    }

    @Override public void resolve(ResolveRequest req, StreamObserver<ResolveResponse> resp) {
        run(resp, () -> service.doResolve(req));
    }

    @Override public void activeCells(ActiveCellsRequest req, StreamObserver<ActiveCellsResponse> resp) {
        run(resp, () -> service.doActiveCells(req.getNamespace(), CoordinatorService.emptyToNull(req.getCallerRegion())));
    }

    @Override public void registerWorkflow(RegisterWorkflowRequest req, StreamObserver<RegisterWorkflowResponse> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc RegisterWorkflow namespace=" + req.getNamespace()
                + " name=" + req.getName());
        run(resp, () -> service.doRegisterWorkflow(req.getNamespace(), req.getName(), req.getDefinition().toByteArray()));
    }

    @Override public void deregisterWorkflow(DeregisterWorkflowRequest req, StreamObserver<DeregisterWorkflowResponse> resp) {
        LOG.log(System.Logger.Level.DEBUG, () -> "rpc DeregisterWorkflow namespace=" + req.getNamespace()
                + " name=" + req.getName());
        run(resp, () -> service.doDeregisterWorkflow(req.getNamespace(), req.getName()));
    }

    @Override public void listWorkflows(ListWorkflowsRequest req, StreamObserver<ListWorkflowsResponse> resp) {
        run(resp, () -> service.doListWorkflows(req.getNamespace()));
    }

    private static Map<Long, Long> toLongMap(Map<Long, Integer> m) {
        if (m.isEmpty()) return Map.of();
        Map<Long, Long> out = new LinkedHashMap<>();
        for (Map.Entry<Long, Integer> e : m.entrySet()) out.put(e.getKey(), e.getValue().longValue());
        return out;
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