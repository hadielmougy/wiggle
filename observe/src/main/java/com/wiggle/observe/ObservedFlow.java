package com.wiggle.observe;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * A published observed workflow, and the foundation every other way of observing it is built on:
 * a service reports a step by naming the run's key, the step, and when it ran. Nothing has to be
 * opened, joined or scoped, and no thread is involved -- a request handled on one thread and its
 * reply on another report with the same key, and a run's first report creates it wherever it
 * comes from.
 *
 * <pre>{@code
 * ObservedFlow checkout = observer.publish(spec);
 * checkout.record("order-42", "validate", startedAt, finishedAt);
 * checkout.recordPredicate("order-42", "inStock", true, startedAt, finishedAt);
 * checkout.recordUndo("order-42", "reserve", startedAt, finishedAt);   // compensation, when it lands
 * checkout.fail("order-42", "card declined");
 * }</pre>
 *
 * <p>Step names are the spec's; an unknown one is refused here rather than reaching the server
 * as an anomaly. An undo may only name a step the spec declared compensable. Times are the
 * caller's, epoch millis, measured however the service measures; {@link #start} is a convenience
 * that measures for you. Reports are batched and sent behind the caller; a full queue or a
 * failed call drops and counts, never blocks.
 *
 * <p>The proxy over a step interface ({@link Observer#observe}), the thread-bound {@link Run},
 * and the Kafka adapter are all callers of this.
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
        record(key, step, startedAt, finishedAt, null);
    }

    /** A completed step, and the step that caused it ({@code after}, may be null). */
    public void record(String key, String step, long startedAt, long finishedAt, String after) {
        submit(key, StepRecord.step(node(step).id(), null, startedAt, finishedAt, nodeIdOrNull(after)), null);
    }

    /** A completed step whose return value replaces the run's context, when contexts are captured. */
    public void record(String key, String step, Object result, long startedAt, long finishedAt) {
        submit(key, StepRecord.step(node(step).id(), observer.options().captureContext() ? result : null,
                startedAt, finishedAt, null), null);
    }

    public void recordPredicate(String key, String step, boolean value, long startedAt, long finishedAt) {
        recordPredicate(key, step, value, startedAt, finishedAt, null);
    }

    public void recordPredicate(String key, String step, boolean value, long startedAt, long finishedAt, String after) {
        submit(key, StepRecord.predicate(node(step).id(), value, startedAt, finishedAt, nodeIdOrNull(after)), null);
    }

    /** A step that threw: the run is failed with {@code step: error}, and its undos become due. */
    public void recordError(String key, String step, String error, long startedAt, long finishedAt) {
        recordError(key, step, error, startedAt, finishedAt, null);
    }

    public void recordError(String key, String step, String error, long startedAt, long finishedAt, String after) {
        submit(key, StepRecord.error(node(step).id(), error, startedAt, finishedAt, nodeIdOrNull(after)), null);
    }

    /** The undo of a compensable step ran. */
    public void recordUndo(String key, String step, long startedAt, long finishedAt) {
        recordUndo(key, step, startedAt, finishedAt, null);
    }

    public void recordUndo(String key, String step, long startedAt, long finishedAt, String after) {
        submit(key, StepRecord.undo(compensable(step).id(), null, startedAt, finishedAt, nodeIdOrNull(after)), null);
    }

    /** The undo of a compensable step threw: the run's compensation has failed. */
    public void recordUndoError(String key, String step, String error, long startedAt, long finishedAt) {
        recordUndoError(key, step, error, startedAt, finishedAt, null);
    }

    public void recordUndoError(String key, String step, String error, long startedAt, long finishedAt, String after) {
        submit(key, StepRecord.undo(compensable(step).id(), error, startedAt, finishedAt, nodeIdOrNull(after)), null);
    }

    /** Declares the run failed: the undos of its completed compensable steps are now expected. */
    public void fail(String key, String reason) {
        observer.reporter().submit(new Run.Batch(apiRun(key), java.util.List.of(), false,
                reason == null || reason.isBlank() ? "failed" : reason));
    }

    /** Declares the run over from the originator's side; a run that never reached END is then incomplete. */
    public void end(String key) {
        observer.reporter().submit(new Run.Batch(apiRun(key), java.util.List.of(), true, null));
    }

    /** Starts timing a step; close the timer with its outcome. */
    public StepTimer start(String key, String step) {
        node(step);
        return new StepTimer(this, key, step, false);
    }

    /** Starts timing an undo; close the timer with its outcome. */
    public StepTimer startUndo(String key, String step) {
        compensable(step);
        return new StepTimer(this, key, step, true);
    }

    Node node(String step) {
        Node n = byName.get(step);
        if (n == null) {
            throw new IllegalArgumentException("'" + step + "' is not a step of " + spec.definition().key()
                    + "; steps are " + byName.keySet());
        }
        return n;
    }

    /** The node for {@code step}, or null for a null name: a step named by id passes through too. */
    private String nodeIdOrNull(String step) {
        if (step == null) return null;
        Node n = byName.get(step);
        if (n != null) return n.id();
        return spec.definition().nodes().containsKey(step) ? step : null;
    }

    private Node compensable(String step) {
        Node n = node(step);
        if (!n.compensable()) {
            throw new IllegalArgumentException("'" + step + "' of " + spec.definition().key() + " declares no undo");
        }
        return n;
    }

    Node nodeById(String id) {
        return spec.definition().nodes().get(id);
    }

    Map<String, Node> byName() {
        return byName;
    }

    ObserverOptions options() {
        return observer.options();
    }

    Reporter reporter() {
        return observer.reporter();
    }

    private void submit(String key, StepRecord rec, String failure) {
        observer.reporter().submit(new Run.Batch(apiRun(key), java.util.List.of(rec), false, failure));
    }

    /** A run object for one report: the reporter needs the flow and the key, nothing more. */
    private Run apiRun(String key) {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("a report names its run's key");
        return new Run(this, Run.Kind.API, key);
    }
}
