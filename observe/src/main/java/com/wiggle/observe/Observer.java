package com.wiggle.observe;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Tls;
import com.wiggle.proto.ProtoJson;
import com.wiggle.proto.WiggleControlPlaneGrpc;
import com.wiggle.proto.WorkflowDefinition;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.TlsChannelCredentials;

import java.util.concurrent.TimeUnit;

/**
 * Reports flows that run inside this process to a Wiggle server, which checks them against their
 * topology and keeps their timings ({@link ExecutionMode#OBSERVED}). The server dispatches nothing
 * to an observer and an observer never waits on the server.
 *
 * <p>Separate from the client on purpose: an observed application publishes a topology and
 * reports against it, and needs neither a worker nor the instance API to do so.
 *
 * <pre>{@code
 * try (Observer observer = Observer.connect("localhost:8080")) {
 *     ObservedFlow checkout = observer.publish(spec);
 *     checkout.record(orderId, "validate", startedAt, finishedAt);
 * }
 * }</pre>
 */
public final class Observer implements AutoCloseable {

    private final ManagedChannel channel;
    private final WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub stub;
    private final ObserverOptions options;
    private final Reporter reporter;

    private Observer(String target, ObserverOptions options) {
        this.options = options;
        this.channel = Grpc.newChannelBuilder(stripScheme(target),
                credentials(options.tls(), options.requireTls())).build();
        this.stub = WiggleControlPlaneGrpc.newBlockingStub(channel);
        this.reporter = new Reporter(stub, options);
    }

    public static Observer connect(String target) {
        return connect(target, ObserverOptions.defaults());
    }

    public static Observer connect(String target, ObserverOptions options) {
        return new Observer(target, options);
    }

    /**
     * Publishes {@code spec} as an {@link ExecutionMode#OBSERVED} workflow and returns the handle
     * every report of it goes through. The spec names no execution mode of its own: the observer
     * stamps OBSERVED on the definition it publishes, and a spec that asked for a worker mode is
     * refused, since no worker will ever serve it.
     */
    public ObservedFlow publish(FlowSpec spec) {
        if (spec.definition().executionMode() != ExecutionMode.DEFAULT) {
            throw new IllegalArgumentException("workflow '" + spec.definition().key() + "' asks to run in "
                    + spec.definition().executionMode() + "; an observed flow names no execution mode");
        }
        com.wiggle.core.WorkflowDefinition d = spec.definition();
        FlowSpec observed = new FlowSpec(new com.wiggle.core.WorkflowDefinition(d.name(), d.version(), d.startNode(),
                d.nodes(), d.queues(), ExecutionMode.OBSERVED, d.checkpoints()));
        try {
            stub.registerWorkflow(WorkflowDefinition.newBuilder()
                    .setDefinition(ProtoJson.toStruct(observed.definition().toJson()))
                    .build());
        } catch (StatusRuntimeException e) {
            throw new IllegalStateException("could not publish '" + d.key() + "': "
                    + e.getStatus().getCode() + " " + e.getStatus().getDescription(), e);
        }
        return new ObservedFlow(this, observed);
    }

    /** Reports dropped rather than sent: the queue was full, or a call failed or was refused. */
    public long dropped() {
        return reporter.dropped();
    }

    ObserverOptions options() {
        return options;
    }

    Reporter reporter() {
        return reporter;
    }

    /** Sends what is still queued, then closes the connection. */
    @Override
    public void close() {
        reporter.close();
        channel.shutdown();
        try {
            channel.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        channel.shutdownNow();
    }

    private static ChannelCredentials credentials(Tls.Options tls, boolean requireTls) {
        if (!requireTls && !tls.any()) return InsecureChannelCredentials.create();
        TlsChannelCredentials.Builder b = TlsChannelCredentials.newBuilder();
        if (tls.hasTrustStore()) b.trustManager(Tls.trustManagers(tls));
        if (tls.hasKeyStore()) b.keyManager(Tls.keyManagers(tls));
        return b.build();
    }

    private static String stripScheme(String target) {
        int i = target.indexOf("://");
        return i < 0 ? target : target.substring(i + 3);
    }
}
