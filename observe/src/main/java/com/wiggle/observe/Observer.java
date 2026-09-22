package com.wiggle.observe;

import com.wiggle.client.CoordinatedConnection;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Reports flows that run inside this process to a Wiggle server, which checks them against their
 * topology and keeps their timings ({@link ExecutionMode#OBSERVED}). The server dispatches nothing
 * to an observer and an observer never waits on the server: steps run on the application's own
 * threads and are reported behind them.
 *
 * <p>Separate from the client on purpose: an observed application publishes a topology and
 * reports against it, and needs neither a worker nor the instance API to do so.
 *
 * <pre>{@code
 * try (Observer observer = Observer.connect("localhost:8080")) {
 *     Observed<CheckoutSteps> checkout = observer.observe(spec, CheckoutSteps.class, new Checkout());
 *     CheckoutSteps s = checkout.steps();
 *     try (Run run = checkout.begin(orderId)) {
 *         s.validate(order); s.reserve(order); s.charge(order);
 *     }
 * }
 * }</pre>
 */
public final class Observer implements AutoCloseable {

    private final ObserverOptions options;
    private final Reporter reporter;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub> stubs = new ConcurrentHashMap<>();
    /** Publishes a definition wherever this observer's runs will be reported. */
    private final Consumer<com.wiggle.core.WorkflowDefinition> publisher;

    private Observer(ObserverOptions options, Function<Run, String> targetOf,
                     Consumer<com.wiggle.core.WorkflowDefinition> publisher) {
        this.options = options;
        this.publisher = publisher;
        this.reporter = new Reporter(targetOf, this::stubFor, options);
    }

    /** Connects to one server (or one cell). Every run is reported there. */
    public static Observer connect(String target) {
        return connect(target, ObserverOptions.defaults());
    }

    public static Observer connect(String target, ObserverOptions options) {
        Observer[] self = new Observer[1];
        self[0] = new Observer(options, run -> target, def -> self[0].publish(target, def));
        return self[0];
    }

    /**
     * Connects through a coordinator: each run is reported to the cell that owns its key, which
     * the coordinator resolves ({@code targetForRunKey}), so every service reporting one run lands
     * on one instance whichever cell it would otherwise talk to. Definitions are fanned out to
     * every cell of the namespace through the coordinator.
     */
    public static Observer connect(CoordinatedConnection coordinator, String namespace) {
        return connect(coordinator, namespace, ObserverOptions.defaults());
    }

    public static Observer connect(CoordinatedConnection coordinator, String namespace, ObserverOptions options) {
        return new Observer(options,
                run -> coordinator.targetForRunKey(namespace, run.owner().name(), run.correlationId()),
                def -> coordinator.registerWorkflow(namespace, new FlowSpec(def)));
    }

    private WiggleControlPlaneGrpc.WiggleControlPlaneBlockingStub stubFor(String target) {
        return stubs.computeIfAbsent(target, t -> WiggleControlPlaneGrpc.newBlockingStub(
                channels.computeIfAbsent(t, u -> Grpc.newChannelBuilder(stripScheme(u),
                        credentials(options.tls(), options.requireTls())).build())));
    }

    private void publish(String target, com.wiggle.core.WorkflowDefinition def) {
        stubFor(target).registerWorkflow(WorkflowDefinition.newBuilder()
                .setDefinition(ProtoJson.toStruct(def.toJson()))
                .build());
    }

    /**
     * Publishes {@code spec} as an {@link ExecutionMode#OBSERVED} workflow and returns the handle
     * every report of it goes through: {@link ObservedFlow#record} and its siblings, by run key,
     * step name and times. The spec names no execution mode of its own: the observer stamps
     * OBSERVED on the definition it publishes, and a spec that asked for a worker mode is refused,
     * since no worker will ever serve it.
     */
    public ObservedFlow publish(FlowSpec spec) {
        if (spec.definition().executionMode() != ExecutionMode.DEFAULT) {
            throw new IllegalArgumentException("workflow '" + spec.definition().key() + "' asks to run in "
                    + spec.definition().executionMode() + "; an observed flow names no execution mode");
        }
        FlowSpec observed = new FlowSpec(observedCopy(spec.definition()));
        try {
            publisher.accept(observed.definition());
        } catch (StatusRuntimeException e) {
            throw new IllegalStateException("could not publish '" + spec.definition().key() + "': "
                    + e.getStatus().getCode() + " " + e.getStatus().getDescription(), e);
        }
        return new ObservedFlow(this, observed);
    }

    /**
     * {@link #publish} plus code instrumentation: wraps {@code impl}, the application's
     * implementation of the spec's step interface, so that calls on the returned
     * {@link Observed#steps()} are timed and reported under the thread's current {@link Run}.
     */
    public <S> Observed<S> observe(FlowSpec spec, Class<S> contract, S impl) {
        if (!contract.isInstance(impl)) {
            throw new IllegalArgumentException(impl.getClass().getName() + " does not implement " + contract.getName());
        }
        return new Observed<>(publish(spec), contract, impl);
    }

    private static com.wiggle.core.WorkflowDefinition observedCopy(com.wiggle.core.WorkflowDefinition d) {
        return new com.wiggle.core.WorkflowDefinition(d.name(), d.version(), d.startNode(), d.nodes(), d.queues(),
                ExecutionMode.OBSERVED, d.checkpoints());
    }

    /** Steps dropped rather than reported: the queue was full, or a report failed or was refused. */
    public long dropped() {
        return reporter.dropped();
    }

    ObserverOptions options() {
        return options;
    }

    Reporter reporter() {
        return reporter;
    }

    /** Sends what is still queued, then closes every connection it opened. */
    @Override
    public void close() {
        reporter.close();
        for (ManagedChannel channel : channels.values()) {
            channel.shutdown();
            try {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            channel.shutdownNow();
        }
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
