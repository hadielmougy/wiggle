package com.wiggle.observe;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Handles;
import com.wiggle.core.GraphTraversal;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;

import com.wiggle.core.Ids;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

/**
 * One observed flow: the spec's step interface, implemented by the application, wrapped so that
 * every call on it is timed and reported. {@link #steps()} is the wrapped implementation; call it
 * exactly as you would the original. A method of the interface that names no step of the graph
 * passes straight through.
 *
 * <p>Calls are grouped into runs per thread. {@link #begin} opens one as the originator,
 * {@link #join} opens one as a participant in a run another service started, and a step called
 * with none open opens an implicit one that ends at END. A run also ends when its step throws or
 * its {@link Run} is closed; see {@link Run} for what each kind's close means.
 */
public final class Observed<S> {

    private final Observer observer;
    private final WorkflowDefinition def;
    private final S steps;
    private final Map<String, Node> byName = new HashMap<>();

    Observed(Observer observer, FlowSpec spec, Class<S> contract, S impl) {
        this.observer = observer;
        this.def = spec.definition();
        for (Node n : def.nodes().values()) {
            if (n.isWorkerDispatched()) byName.put(n.name(), n);
        }
        this.steps = contract.cast(Proxy.newProxyInstance(contract.getClassLoader(),
                new Class<?>[] {contract}, (proxy, method, args) -> invoke(impl, method, args)));
    }

    public S steps() {
        return steps;
    }

    public String name() {
        return def.name();
    }

    public int version() {
        return def.version();
    }

    /**
     * Opens a run on this thread as its originator, under a business key (a blank key mints one).
     * The thread's later step calls belong to it until it is closed; closing it before END
     * reports the run as over.
     */
    public Run begin(String correlationId) {
        return open(Run.Kind.BEGUN, correlationId);
    }

    /**
     * Opens a run on this thread as a participant: the run was started elsewhere, under this key,
     * and this service runs some of its steps. Closing it flushes what it holds and says nothing
     * about the whole.
     */
    public Run join(String correlationId) {
        if (correlationId == null || correlationId.isBlank()) {
            throw new IllegalArgumentException("joining a run needs its key");
        }
        return open(Run.Kind.JOINED, correlationId);
    }

    /** {@link #join(String)} from a context another service sent, which must name this flow. */
    public Run join(RunContext context) {
        if (!def.name().equals(context.workflow())) {
            throw new IllegalArgumentException("run context is for workflow '" + context.workflow()
                    + "', this observes '" + def.name() + "'");
        }
        if (context.version() != 0 && context.version() != def.version()) {
            throw new IllegalArgumentException("run context is for " + context.workflow() + " v" + context.version()
                    + ", this observes v" + def.version());
        }
        return openJoined(context.correlationId(), context.after());
    }

    /** Binds an open run to this thread, for a hand-off {@link Run#wrap} cannot express. */
    public void attach(Run run) {
        Observation.attach(run);
    }

    /** Unbinds this thread's run without closing it. */
    public void detach() {
        Run r = Observation.current();
        if (r != null) Observation.detach(r);
    }

    /** The run this thread's step calls currently belong to, or null. */
    public Run current() {
        return Observation.current();
    }

    private Run open(Run.Kind kind, String correlationId) {
        return open(kind, correlationId, null);
    }

    private Run openJoined(String correlationId, String after) {
        return open(Run.Kind.JOINED, correlationId, after);
    }

    private Run open(Run.Kind kind, String correlationId, String after) {
        Run open = Observation.current();
        if (open != null && open.owner() == this) open.end();
        Run run = new Run(this, kind, correlationId == null || correlationId.isBlank() ? Ids.token() : correlationId, after);
        Observation.attach(run);
        return run;
    }

    ObserverOptions options() {
        return observer.options();
    }

    Reporter reporter() {
        return observer.reporter();
    }

    private Object invoke(S impl, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) return objectMethod(method, args);
        Node node = byName.get(stepName(method));
        if (node == null) return call(impl, method, args);
        Run run = Observation.current();
        if (run == null || run.owner() != this) {
            run = new Run(this, Run.Kind.IMPLICIT, Ids.token());
            Observation.attach(run);
        }
        String after = run.lastCompleted();
        long startedAt = System.currentTimeMillis();
        long t0 = System.nanoTime();
        Object result;
        try {
            result = call(impl, method, args);
        } catch (Throwable t) {
            long finishedAt = startedAt + (System.nanoTime() - t0) / 1_000_000;
            run.record(new StepRecord(node.id(), null, null, describe(t), startedAt, finishedAt, after), false);
            run.failed();
            throw t;
        }
        long finishedAt = startedAt + (System.nanoTime() - t0) / 1_000_000;
        boolean predicate = node.kind() == NodeKind.PREDICATE;
        Boolean value = predicate && result instanceof Boolean b ? b : null;
        Object merge = !predicate && options().captureContext() ? result : null;
        Node next = def.nodes().get(GraphTraversal.successor(node, value != null && value));
        boolean atEnd = next == null || next.kind() == NodeKind.END;
        run.completed(node.id());
        run.record(new StepRecord(node.id(), merge, value, null, startedAt, finishedAt, after), atEnd);
        return result;
    }

    /** Invokes through the interface method the proxy hands over, opened once for a
     *  non-public interface: the proxy reuses the same Method object per method. */
    private static Object call(Object impl, Method method, Object[] args) throws Throwable {
        if (!method.canAccess(impl)) method.trySetAccessible();
        try {
            return method.invoke(impl, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private Object objectMethod(Method method, Object[] args) {
        return switch (method.getName()) {
            case "toString" -> "Observed(" + def.key() + ")";
            case "hashCode" -> System.identityHashCode(this);
            case "equals" -> args[0] == steps;
            default -> throw new UnsupportedOperationException(method.getName());
        };
    }

    /** The name a spec gives this method's step: its {@link Handles} value, else the method name. */
    private static String stepName(Method m) {
        Handles handles = m.getAnnotation(Handles.class);
        return handles != null ? handles.value() : m.getName();
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }
}
