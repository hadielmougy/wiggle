package com.wiggle.observe;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * A published observed workflow. A service reports a step by naming the run's key, the step, and
 * when it ran; nothing is opened or scoped, no thread is involved, and a run's first report
 * creates it wherever it comes from -- a request handled on one thread and its reply on another
 * report with the same key, and so do two services.
 *
 * <pre>{@code
 * ObservedFlow checkout = observer.publish(spec);
 * checkout.record("order-42", "validate", startedAt, finishedAt);
 * checkout.recordPredicate("order-42", "inStock", true, startedAt, finishedAt);
 * checkout.recordError("order-42", "charge", "CardDeclined", startedAt, finishedAt);
 * }</pre>
 *
 * <p>Step names are the spec's; an unknown one is refused here rather than reaching the server as
 * an anomaly. Times are the caller's, epoch millis, measured however the service measures;
 * {@link #start} is a convenience that measures for you. Reports are batched and sent behind the
 * caller; a full queue or a failed call drops and counts, never blocks.
 */
public final class ObservedFlow {

    private final Observer observer;
    private final FlowSpec spec;
    private final Map<String, Node> byName = new HashMap<>();

    ObservedFlow(Observer observer, FlowSpec spec) {
        this.observer = observer;
        this.spec = spec;
        for (Node n : spec.definition().nodes().values()) {
            if (n.isWorkerDispatched()) byName.put(n.name(), n);
        }
    }

    public String name() {
        return spec.name();
    }

    public int version() {
        return spec.version();
    }

    public WorkflowDefinition definition() {
        return spec.definition();
    }

    /** A completed step. */
    public void record(String key, String step, long startedAt, long finishedAt) {
        submit(new Report(name(), version(), key(key), node(step).id(), null, null, startedAt, finishedAt, false));
    }

    /** A completed predicate and the branch it chose. */
    public void recordPredicate(String key, String step, boolean value, long startedAt, long finishedAt) {
        submit(new Report(name(), version(), key(key), node(step).id(), value, null, startedAt, finishedAt, false));
    }

    /** A step that threw: the run fails with {@code step: error}. */
    public void recordError(String key, String step, String error, long startedAt, long finishedAt) {
        submit(new Report(name(), version(), key(key), node(step).id(), null,
                error == null || error.isBlank() ? "failed" : error, startedAt, finishedAt, false));
    }

    /** Declares the run over from the originator's side; a run that never reached END is then incomplete. */
    public void end(String key) {
        submit(Report.end(name(), version(), key(key)));
    }

    /** Starts timing a step; close the timer with its outcome. */
    public StepTimer start(String key, String step) {
        node(step);
        return new StepTimer(this, key(key), step);
    }

    private Node node(String step) {
        Node n = byName.get(step);
        if (n == null) {
            throw new IllegalArgumentException("'" + step + "' is not a step of " + spec.definition().key()
                    + "; steps are " + byName.keySet());
        }
        return n;
    }

    private static String key(String key) {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("a report names its run's key");
        return key;
    }

    private void submit(Report r) {
        observer.reporter().submit(r);
    }
}
