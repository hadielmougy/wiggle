package com.wiggle.observe;

import com.wiggle.client.worker.Activity;
import com.wiggle.client.worker.Compensable;
import com.wiggle.client.worker.Handles;
import com.wiggle.core.GraphTraversal;
import com.wiggle.core.Ids;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Code instrumentation over an {@link ObservedFlow}: the spec's step interface, implemented by the
 * application, wrapped so that every call on it is timed and reported. {@link #steps()} is the
 * wrapped implementation; call it exactly as you would the original. A method of the interface
 * that names no step of the graph passes straight through.
 *
 * <p>A step declared as a factory ({@code CompensableActivity<A, B> reserve()}) is wrapped one
 * level deeper: the activity the factory returns is itself wrapped, so its {@code execute} is
 * reported as the step and its {@code compensate} as the step's undo. The application calls
 * {@code steps.reserve().execute(order)}, and later {@code steps.reserve().compensate(snapshot)},
 * as it would with no observation at all.
 *
 * <p>Calls are grouped into runs per thread. {@link #begin} opens one as the originator,
 * {@link #join} opens one as a participant in a run another service started, and a step called
 * with none open opens an implicit one that ends at END. A run also ends when its step throws or
 * its {@link Run} is closed; see {@link Run} for what each kind's close means.
 */
public final class Observed<S> {

    private final ObservedFlow flow;
    private final WorkflowDefinition def;
    private final S steps;

    Observed(ObservedFlow flow, Class<S> contract, S impl) {
        this.flow = flow;
        this.def = flow.definition();
        this.steps = contract.cast(Proxy.newProxyInstance(contract.getClassLoader(),
                new Class<?>[] {contract}, (proxy, method, args) -> invoke(impl, method, args)));
    }

    public S steps() {
        return steps;
    }

    /** The published flow this instruments: the explicit report API for the same workflow. */
    public ObservedFlow flow() {
        return flow;
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
        return open(Run.Kind.BEGUN, correlationId, null);
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
        return open(Run.Kind.JOINED, correlationId, null);
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
        return open(Run.Kind.JOINED, context.correlationId(), context.after());
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

    private Run open(Run.Kind kind, String correlationId, String after) {
        Run open = Observation.current();
        if (open != null && open.owner() == flow) open.end();
        Run run = new Run(flow, kind, correlationId == null || correlationId.isBlank() ? Ids.token() : correlationId, after);
        Observation.attach(run);
        return run;
    }

    private Object invoke(S impl, Method method, Object[] args) throws Throwable {
        if (method.getDeclaringClass() == Object.class) return objectMethod(method, args);
        Node node = flow.byName().get(stepName(method));
        if (node == null) return call(impl, method, args);
        if (method.getParameterCount() == 0 && isActivityType(method.getReturnType())) {
            Object activity = call(impl, method, args);
            return activity == null ? null : wrapActivity(node, activity);
        }
        return recorded(node, false, () -> call(impl, method, args));
    }

    /** The activity a factory step returned, wrapped so its execute is the step and its compensate the undo. */
    private Object wrapActivity(Node node, Object activity) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> c = activity.getClass(); c != null; c = c.getSuperclass()) {
            for (Class<?> i : c.getInterfaces()) collectInterfaces(i, interfaces);
        }
        return Proxy.newProxyInstance(activity.getClass().getClassLoader(), interfaces.toArray(new Class<?>[0]),
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) return objectMethod(method, args);
                    boolean execute = method.getName().equals("execute") && method.getParameterCount() == 1
                            && Activity.class.isAssignableFrom(method.getDeclaringClass());
                    boolean compensate = method.getName().equals("compensate") && method.getParameterCount() == 1
                            && Compensable.class.isAssignableFrom(method.getDeclaringClass());
                    if (!execute && !compensate) return call(activity, method, args);
                    return recorded(node, compensate, () -> call(activity, method, args));
                });
    }

    private static void collectInterfaces(Class<?> i, Set<Class<?>> out) {
        if (out.add(i)) for (Class<?> parent : i.getInterfaces()) collectInterfaces(parent, out);
    }

    private static boolean isActivityType(Class<?> type) {
        return Activity.class.isAssignableFrom(type) || Compensable.class.isAssignableFrom(type);
    }

    /** What a step call does, allowed to throw whatever the application's code throws. */
    private interface Body {
        Object run() throws Throwable;
    }

    /** Runs {@code body} as {@code node}'s step (or its undo) under the thread's run, timing it. */
    private Object recorded(Node node, boolean undo, Body body) throws Throwable {
        Run run = Observation.current();
        if (run == null || run.owner() != flow) {
            run = new Run(flow, Run.Kind.IMPLICIT, Ids.token());
            Observation.attach(run);
        }
        String after = run.lastCompleted();
        long startedAt = System.currentTimeMillis();
        long t0 = System.nanoTime();
        Object result;
        try {
            result = body.run();
        } catch (Throwable t) {
            long finishedAt = startedAt + (System.nanoTime() - t0) / 1_000_000;
            run.record(undo ? StepRecord.undo(node.id(), describe(t), startedAt, finishedAt, after)
                    : StepRecord.error(node.id(), describe(t), startedAt, finishedAt, after), false);
            run.failed();
            throw t;
        }
        long finishedAt = startedAt + (System.nanoTime() - t0) / 1_000_000;
        if (undo) {
            run.record(StepRecord.undo(node.id(), null, startedAt, finishedAt, after), false);
            return result;
        }
        boolean predicate = node.kind() == NodeKind.PREDICATE;
        Boolean value = predicate && result instanceof Boolean b ? b : null;
        Object merge = !predicate && flow.options().captureContext() ? result : null;
        Node next = def.nodes().get(GraphTraversal.successor(node, value != null && value));
        boolean atEnd = next == null || next.kind() == NodeKind.END;
        run.completed(node.id());
        run.record(predicate ? StepRecord.predicate(node.id(), value != null && value, startedAt, finishedAt, after)
                : StepRecord.step(node.id(), merge, startedAt, finishedAt, after), atEnd);
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

    static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }
}
