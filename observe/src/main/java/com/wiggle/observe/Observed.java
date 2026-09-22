package com.wiggle.observe;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Handles;
import com.wiggle.core.GraphTraversal;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;

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
 * <p>Calls are grouped into runs per thread: {@link #begin} opens one explicitly, and a step
 * called with none open opens one implicitly. A run ends when a step's successor is END, when the
 * step throws, or when its {@link Run} is closed.
 */
public final class Observed<S> {

    private final Observer observer;
    private final WorkflowDefinition def;
    private final S steps;
    private final Map<String, Node> byName = new HashMap<>();
    private final ThreadLocal<Run> current = new ThreadLocal<>();

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

    /** Opens a run on this thread under a business key; the thread's later step calls belong to it. */
    public Run begin(String correlationId) {
        Run open = current.get();
        if (open != null) open.end();
        Run run = new Run(this, correlationId);
        current.set(run);
        return run;
    }

    /** The run this thread's step calls currently belong to, or null. */
    public Run current() {
        return current.get();
    }

    ObserverOptions options() {
        return observer.options();
    }

    Reporter reporter() {
        return observer.reporter();
    }

    void detach(Run run) {
        if (current.get() == run) current.remove();
    }

    private Object invoke(S impl, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) return objectMethod(method, args);
        Node node = byName.get(stepName(method));
        if (node == null) return call(impl, method, args);
        Run run = current.get();
        if (run == null) {
            run = new Run(this, null);
            current.set(run);
        }
        long startedAt = System.currentTimeMillis();
        long t0 = System.nanoTime();
        Object result;
        try {
            result = call(impl, method, args);
        } catch (Throwable t) {
            long finishedAt = startedAt + (System.nanoTime() - t0) / 1_000_000;
            run.record(new StepRecord(node.id(), null, null, describe(t), startedAt, finishedAt), true);
            throw t;
        }
        long finishedAt = startedAt + (System.nanoTime() - t0) / 1_000_000;
        boolean predicate = node.kind() == NodeKind.PREDICATE;
        Boolean value = predicate && result instanceof Boolean b ? b : null;
        Object merge = !predicate && options().captureContext() ? result : null;
        Node next = def.nodes().get(GraphTraversal.successor(node, value != null && value));
        boolean atEnd = next == null || next.kind() == NodeKind.END;
        run.record(new StepRecord(node.id(), merge, value, null, startedAt, finishedAt), atEnd);
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
