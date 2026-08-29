package dev.wiggle.server.coord;

import dev.wiggle.core.Tls;
import dev.wiggle.proto.CellCoordinatorGrpc;
import dev.wiggle.proto.EpochRing;
import dev.wiggle.proto.EpochStatus;
import dev.wiggle.proto.OpenEpochRequest;
import dev.wiggle.proto.Policy;
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
