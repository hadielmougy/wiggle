package com.wiggle.client.worker;

import com.wiggle.client.dsl.ActivityHandler;
import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RecordMapper;
import com.wiggle.core.WorkflowDefinition;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The worker's reflective seam: turns a {@link Handlers @Handlers}-annotated object into
 * executable {@link ActivityHandler}s. Two pure operations, deliberately free of I/O and of the
 * worker's runtime state so every signature rule here is unit-testable against a compiled graph:
 *
 * <ul>
 *   <li>{@link #scan(Object)} — inventory an object's step methods (by canonical name) and its
 *       {@link Decode @Decode} decoders, rejecting ambiguous names.</li>
 *   <li>{@link #bind(HandlerSet, WorkflowDefinition)} — resolve an inventory against a graph,
 *       node by node, validating each matched method's signature against the node's kind and
 *       building the invocation wrapper. Returns the bindings plus the steps this object does
 *       not serve; the {@link Worker} decides what to do with both.</li>
 * </ul>
 *
 * A method's signature defines its step: one input parameter (decoded from JSON into its type),
 * plus an optional {@link Context @Context} parameter for the frozen base where one exists; a
 * {@code boolean} return is a gate, {@code void} an effect, anything else a task whose return
 * REPLACES the context. Combine methods bind {@link Arm @Arm} parameters (fork) or a collection
 * parameter (forEach); their return is the complete post-join context.
 */
final class HandlerBinder {

    private HandlerBinder() {}

    /** One step candidate: the object to invoke on and its handler method — a plain
     *  {@code @Handlers} method (target = the handlers object) or a factory-produced typed
     *  activity (target = the activity instance, method = its execute/test/apply; compensate
     *  set when the instance implements {@link Compensable}). */
    record Candidate(Object target, Method method, Method compensate) {}

    /** A handler source's inventory: its step candidates keyed by canonical name, and its
     *  {@link Decode @Decode} decoders (living on the handlers object) keyed by decoded type. */
    record HandlerSet(String workflow, Object target, Map<String, Candidate> byName,
                      Map<Class<?>, Method> decoders) {}

    /** Invokes a step's undo with both of its snapshots (raw JSON-shaped objects); the wrapper
     *  decodes them into the activity's context type and hands a {@link Compensation} to
     *  {@link Compensable#compensate}. */
    @FunctionalInterface
    interface Compensator {
        void invoke(Object input, Object result) throws Exception;
    }

    /** One resolved binding: the executable wrapper plus where it plugs into the worker.
     *  {@code compensator} is non-null only for a typed activity implementing {@link Compensable};
     *  it receives the step's input and result snapshots (wired to the engine's compensation
     *  phase when that lands — see docs/saga-compensation.md). */
    record Binding(String activity, String step, String queue, ActivityHandler handler,
                   Compensator compensator) {
        Binding(String activity, String step, String queue, ActivityHandler handler) {
            this(activity, step, queue, handler, null);
        }
    }

    /** Everything {@link #bind} decided: the bindings, plus the steps this object doesn't serve
     *  (informational — another worker may serve them). */
    record Result(List<Binding> bindings, List<String> unserved) {}

    /**
     * Inventories a {@link Handlers @Handlers}-annotated object. Each public instance method with
     * parameters is a step candidate keyed by its canonical name; {@link Decode @Decode} methods
     * are collected as custom decoders; zero-parameter methods are helpers and ignored. Two
     * methods whose names collide under case-folding are rejected as ambiguous.
     */
    static HandlerSet scan(Object handlerObject) {
        String workflow = getWorkflowName(handlerObject);
        Map<String, Candidate> byName = new LinkedHashMap<>();
        Map<String, String> sourceNames = new LinkedHashMap<>();   // canonical -> method, for errors
        Map<Class<?>, Method> decoders = new LinkedHashMap<>();
        for (Method m : handlerObject.getClass().getMethods()) {
            if (m.isSynthetic() || m.isBridge() || Modifier.isStatic(m.getModifiers())) continue;
            if (m.getDeclaringClass() == Object.class) continue;
            m.setAccessible(true);
            if (m.isAnnotationPresent(Decode.class)) {
                decoders.put(m.getReturnType(), m);
                continue;
            }
            boolean factoryReturn = isActivityType(m.getReturnType());
            if (m.getParameterCount() == 0 && !factoryReturn) continue;   // a helper, not a handler
            if (m.getParameterCount() > 0 && factoryReturn) {
                throw new IllegalArgumentException("method '" + m.getName() + "' returns "
                        + m.getReturnType().getSimpleName() + " but takes parameters — an activity "
                        + "factory must be zero-parameter (its result serves the step)");
            }
            String canon = stepName(m);
            if (canon.isEmpty()) continue;
            Candidate cand = factoryReturn
                    ? factoryCandidate(handlerObject, m)
                    : new Candidate(handlerObject, m, null);
            if (byName.putIfAbsent(canon, cand) != null) {
                throw new IllegalArgumentException("methods '" + sourceNames.get(canon) + "' and '"
                        + m.getName() + "' map to the same step name '" + canon + "'; names differing "
                        + "only in case/style are ambiguous -- rename one (or use @Handles)");
            }
            sourceNames.put(canon, m.getName());
        }
        return new HandlerSet(workflow, handlerObject, byName, decoders);
    }

    /** The canonical step name a method serves: {@link Handles @Handles} when present, else the
     *  method's own name — both under the same case/style folding. */
    private static String stepName(Method m) {
        Handles h = m.getAnnotation(Handles.class);
        if (h != null) {
            String canon = canonicalName(h.value());
            if (canon.isEmpty()) {
                throw new IllegalArgumentException("@Handles on '" + m.getName() + "' names nothing");
            }
            return canon;
        }
        return canonicalName(m.getName());
    }

    private static boolean isActivityType(Class<?> t) {
        return Activity.class.isAssignableFrom(t) || GateActivity.class.isAssignableFrom(t)
                || EffectActivity.class.isAssignableFrom(t);
    }

    /** Invokes a factory method once and inventories its typed activity: the instance's concrete
     *  execute/test/apply is the handler, its {@link Compensable#compensate} the undo. */
    private static Candidate factoryCandidate(Object handlerObject, Method factory) {
        Object typed;
        try {
            typed = factory.invoke(handlerObject);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("activity factory '" + factory.getName() + "' failed", e);
        }
        if (typed == null) {
            throw new IllegalStateException("activity factory '" + factory.getName() + "' returned null");
        }
        int roles = (typed instanceof Activity ? 1 : 0) + (typed instanceof GateActivity ? 1 : 0)
                + (typed instanceof EffectActivity ? 1 : 0);
        if (roles != 1) {
            throw new IllegalArgumentException("factory '" + factory.getName() + "' returned "
                    + typed.getClass().getName() + ", which must implement exactly one of "
                    + "Activity, GateActivity, EffectActivity (found " + roles + ")");
        }
        String primary = typed instanceof Activity ? "execute"
                : typed instanceof GateActivity ? "test" : "apply";
        Method compensate = typed instanceof Compensable ? concreteMethod(typed, "compensate") : null;
        return new Candidate(typed, concreteMethod(typed, primary), compensate);
    }

    private static @NonNull String getWorkflowName(Object handlerObject) {
        if (handlerObject == null) throw new IllegalArgumentException("handlers object is required");
        Handlers ann = handlerObject.getClass().getAnnotation(Handlers.class);
        if (ann == null) {
            throw new IllegalArgumentException(handlerObject.getClass().getName()
                    + " is not annotated @Handlers(\"<workflow>\")");
        }
        String workflow = ann.value();
        if (workflow == null || workflow.isBlank()) {
            throw new IllegalArgumentException("@Handlers on " + handlerObject.getClass().getName()
                    + " needs the workflow name");
        }
        return workflow;
    }

    /**
     * Resolves an inventory against a compiled graph, node by node: each worker-dispatched step is
     * bound to the method whose name matches (case/style-insensitive), its signature checked
     * against the node's kind. A step (or combine) with no method is reported as unserved —
     * combines have no default fold, so one must be bound on some worker; a method matching no
     * step is a helper (ignored). Pure: no I/O, no worker state.
     */
    static Result bind(HandlerSet set, WorkflowDefinition def) {
        List<Binding> bindings = new ArrayList<>();
        TreeSet<String> unserved = new TreeSet<>();
        for (Node node : def.nodes().values()) {
            if (!node.isWorkerDispatched() || node.name() == null) continue;
            Candidate c = set.byName().get(canonicalName(node.name()));
            if (c == null) {
                // No handler on this worker for this step -- another worker may serve it. This
                // includes combine nodes: there is NO default fold; a combine served by no worker
                // fails its task at claim time ("no handler registered"), never merges implicitly.
                unserved.add(node.name());
                continue;
            }
            bindings.add(new Binding(node.activity(), node.name(),
                    node.queue() != null ? node.queue() : set.workflow(),
                    buildHandler(set, node, c),
                    compensatorHandler(set, c)));
        }
        return new Result(List.copyOf(bindings), List.copyOf(unserved));
    }

    /** The compensator wrapper for a step, when its typed activity implements {@link Compensable}:
     *  decodes the post-step snapshot into the method's parameter type and invokes it (an effect —
     *  no return). Null when the step has no compensator. */
    private static Compensator compensatorHandler(HandlerSet set, Candidate c) {
        if (c.compensate() == null) return null;
        Class<?> ctxType = c.method().getParameterTypes()[0];   // the activity's C, from execute/test/apply
        return (input, result) -> {
            Object in = decode(input, ctxType, set.target(), set.decoders());
            Object out = decode(result, ctxType, set.target(), set.decoders());
            call(c.compensate(), c.target(), new Object[]{new Snapshots(in, out)});
        };
    }

    /** The {@link Compensation} handed to a compensator: both snapshots, already decoded. */
    private record Snapshots(Object input, Object result) implements Compensation<Object> {}

    /** The concrete (non-bridge) single-parameter implementation of an interface method, with its
     *  reified parameter type — what the decode machinery needs. */
    private static Method concreteMethod(Object typed, String methodName) {
        for (Method m : typed.getClass().getMethods()) {
            if (!m.getName().equals(methodName) || m.isBridge() || m.isSynthetic()) continue;
            if (m.getParameterCount() != 1) continue;
            m.setAccessible(true);
            return m;
        }
        throw new IllegalStateException(typed.getClass().getName() + " has no concrete " + methodName
                + "(C) method");
    }

    /**
     * Folds a name to a case/style-independent key: its lowercase alphanumerics, in order. So
     * {@code in-stock}, {@code in_stock}, {@code inStock}, {@code InStock}, and {@code instock} all
     * yield {@code instock}.
     */
    static String canonicalName(String s) {
        StringBuilder b = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            if (Character.isLetterOrDigit(ch)) b.append(Character.toLowerCase(ch));
        }
        return b.toString();
    }

    // ------------------------------------------------------------------ wrapper construction

    /** Builds the handler for a graph node from its matched candidate, validating signature vs kind. */
    private static ActivityHandler buildHandler(HandlerSet set, Node node, Candidate c) {
        Method m = c.method();
        Object target = c.target();          // the invocation target: handlers object OR activity
        Object decoderOwner = set.target();  // @Decode methods always live on the handlers object
        Map<Class<?>, Method> decoders = set.decoders();
        if (isCombine(node)) return combineHandler(node, m, target, decoderOwner, decoders);

        Class<?> ret = m.getReturnType();
        boolean returnsBool = (ret == boolean.class || ret == Boolean.class);
        Args args = splitArgs(node, m);   // one input parameter + an optional @Context parameter
        if (node.kind() == NodeKind.PREDICATE) {
            if (!returnsBool) {
                throw new IllegalStateException("gate '" + node.name() + "' handler '" + m.getName()
                        + "' must take the context and return boolean");
            }
            return ctx -> call(m, target, args.build(ctx, node, m, decoderOwner, decoders));
        }
        if (returnsBool) {
            throw new IllegalStateException("step '" + node.name() + "' handler '" + m.getName()
                    + "' must take the context and return the next context (or void for an effect)");
        }
        boolean effect = (ret == void.class || ret == Void.class);
        if (effect) {
            return ctx -> { call(m, target, args.build(ctx, node, m, decoderOwner, decoders)); return null; };
        }
        return ctx -> {
            // The return IS the next context: it is sent whole and REPLACES the previous value
            // server-side (no diff, no merge). A null return leaves the context untouched.
            Object out = call(m, target, args.build(ctx, node, m, decoderOwner, decoders));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    /** A handler's parameter layout: the input's position and type, plus an optional @Context slot.
     *  Either access style works — declare {@code @Context} to receive the frozen base as a
     *  parameter, or call {@code Step.base()} inside the method; both read the same value. */
    private record Args(int inputAt, Class<?> inputType, int contextAt, Class<?> contextType) {

        Object[] build(Object ctx, Node node, Method m, Object decoderOwner, Map<Class<?>, Method> decoders)
                throws Exception {
            Object[] out = new Object[contextAt < 0 ? 1 : 2];
            out[inputAt] = decode(ctx, inputType, decoderOwner, decoders);
            if (contextAt >= 0) {
                Map<String, Object> base;
                try {
                    base = Step.base();
                } catch (IllegalStateException e) {
                    throw new IllegalStateException("step '" + node.name() + "' handler '" + m.getName()
                            + "' declares a @Context parameter, but this step has no base context — "
                            + "@Context is only meaningful inside a forEach body (or a combine)", e);
                }
                out[contextAt] = decode(base, contextType, decoderOwner, decoders);
            }
            return out;
        }
    }

    /** Splits a handler's parameters into the single input + an optional @Context parameter. */
    private static Args splitArgs(Node node, Method m) {
        java.lang.reflect.Parameter[] params = m.getParameters();
        int inputAt = -1;
        int contextAt = -1;
        for (int i = 0; i < params.length; i++) {
            if (params[i].isAnnotationPresent(Context.class)) {
                if (contextAt >= 0) inputAt = -2;   // two @Context params: invalid
                contextAt = i;
            } else if (inputAt == -1) {
                inputAt = i;
            } else {
                inputAt = -2;                        // two plain params: invalid
            }
        }
        if (inputAt < 0 || params.length > 2) {
            throw new IllegalStateException("step '" + node.name() + "' handler '" + m.getName()
                    + "' must take the input (plus at most one @Context parameter for the frozen base)");
        }
        return new Args(inputAt, params[inputAt].getType(), contextAt,
                contextAt < 0 ? null : params[contextAt].getType());
    }

    /** A combine node carries its fork arm names (a JSON array) on its itemsKey; a plain task does not. */
    private static boolean isCombine(Node node) {
        return node.kind() == NodeKind.TASK && node.itemsKey() != null;
    }

    private static List<String> armNames(Node node) {
        return Json.asArray(Json.parse(node.itemsKey())).stream().map(String::valueOf).toList();
    }

    /**
     * A combine method; its return is the COMPLETE post-join context — the engine replaces the
     * context with it (nothing from before the join survives unless the handler returned it, and
     * staged scratch keys are stripped). Two flavors, told apart by the node's itemsKey:
     * <ul>
     *   <li><b>fork</b> (itemsKey = arm-name array): each {@link Arm @Arm} parameter gets that
     *       branch's final context decoded to its type; an optional {@link Context @Context}
     *       parameter gets the pre-fork context.</li>
     *   <li><b>forEach</b> (itemsKey = a scratch-key string): one collection parameter receives
     *       every item's final context — a {@code List} (ordered by item index) or {@code Set} for
     *       a list input, or a {@code Map} keyed like the input for a map input — with elements
     *       decoded to the collection's element type; an optional {@link Context @Context}
     *       parameter gets the pre-forEach context.</li>
     * </ul>
     */
    private static ActivityHandler combineHandler(Node node, Method m, Object target, Object decoderOwner,
                                                  Map<Class<?>, Method> decoders) {
        Object parsedKey = Json.parse(node.itemsKey());
        if (parsedKey instanceof String scratch) return forEachCombineHandler(node, m, target, decoderOwner, decoders, scratch);
        List<String> arms = armNames(node);
        java.lang.reflect.Parameter[] params = m.getParameters();
        return ctx -> {
            Map<String, Object> map = Json.asObject(ctx);
            Map<String, Object> base = new LinkedHashMap<>(map);
            arms.forEach(base::remove);
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                java.lang.reflect.Parameter p = params[i];
                Arm arm = p.getAnnotation(Arm.class);
                if (arm != null) {
                    args[i] = decode(map.get(arm.value()), p.getType(), decoderOwner, decoders);
                } else if (p.isAnnotationPresent(Context.class)) {
                    args[i] = decode(base, p.getType(), decoderOwner, decoders);
                } else {
                    throw new IllegalStateException("combine '" + node.name() + "' handler '" + m.getName()
                            + "' parameter " + i + " must be @Arm(\"branch\") or @Context");
                }
            }
            // Both access styles work: the @Context parameter above, or Step.base() inside the method.
            Object out = Step.withBase(base, () -> call(m, target, args));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    /** The forEach flavor: bind the staged collection (list or map of item results) plus @Context. */
    private static ActivityHandler forEachCombineHandler(Node node, Method m, Object target, Object decoderOwner,
                                                         Map<Class<?>, Method> decoders, String scratch) {
        java.lang.reflect.Parameter[] params = m.getParameters();
        return ctx -> {
            Map<String, Object> map = Json.asObject(ctx);
            Object staged = map.get(scratch);
            Object[] args = new Object[params.length];
            boolean itemsBound = false;
            for (int i = 0; i < params.length; i++) {
                java.lang.reflect.Parameter p = params[i];
                if (p.isAnnotationPresent(Context.class)) {
                    Map<String, Object> base = new LinkedHashMap<>(map);
                    base.remove(scratch);
                    args[i] = decode(base, p.getType(), decoderOwner, decoders);
                } else if (!itemsBound) {
                    args[i] = decodeCollection(staged, p, decoderOwner, decoders, node.name());
                    itemsBound = true;
                } else {
                    throw new IllegalStateException("forEach combine '" + node.name() + "' handler '"
                            + m.getName() + "' takes an optional @Context parameter and exactly one "
                            + "collection parameter (List/Set/Map) for the item results");
                }
            }
            if (!itemsBound) {
                throw new IllegalStateException("forEach combine '" + node.name() + "' handler '"
                        + m.getName() + "' needs a collection parameter (List/Set/Map) for the item results");
            }
            Map<String, Object> base = new LinkedHashMap<>(map);
            base.remove(scratch);
            // Both access styles work: a @Context parameter, or Step.base() inside the method.
            Object out = Step.withBase(base, () -> call(m, target, args));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    /** Decodes the staged item results into the handler's declared collection type: a List keeps the
     *  item order, a Set deduplicates (LinkedHashSet, order-preserving), a Map is keyed like the
     *  input map. Elements decode to the collection's generic element type. */
    private static Object decodeCollection(Object staged, java.lang.reflect.Parameter p,
                                           Object target, Map<Class<?>, Method> decoders, String nodeName)
            throws Exception {
        Class<?> type = p.getType();
        Class<?> elem = elementType(p);
        if (Map.class.isAssignableFrom(type)) {
            Map<String, Object> out = new LinkedHashMap<>();
            if (staged instanceof Map<?, ?> mm) {
                for (Map.Entry<?, ?> e : mm.entrySet()) {
                    out.put(String.valueOf(e.getKey()), decode(e.getValue(), elem, target, decoders));
                }
            }
            return out;
        }
        List<Object> decoded = new java.util.ArrayList<>();
        if (staged instanceof List<?> list) {
            for (Object v : list) decoded.add(decode(v, elem, target, decoders));
        } else if (staged instanceof Map<?, ?> mm) {   // map input bound to a List/Set param: values, in key order
            for (Object v : mm.values()) decoded.add(decode(v, elem, target, decoders));
        }
        if (java.util.Set.class.isAssignableFrom(type)) return new java.util.LinkedHashSet<>(decoded);
        if (List.class.isAssignableFrom(type) || type == Object.class || type == java.util.Collection.class) {
            return decoded;
        }
        throw new IllegalStateException("forEach combine '" + nodeName + "': unsupported collection "
                + "parameter type " + type.getName() + " (use List, Set, or Map)");
    }

    /** The collection parameter's element type from its generics; Object (raw maps) when unknown. */
    private static Class<?> elementType(java.lang.reflect.Parameter p) {
        if (p.getParameterizedType() instanceof java.lang.reflect.ParameterizedType pt) {
            java.lang.reflect.Type[] args = pt.getActualTypeArguments();
            java.lang.reflect.Type t = args[args.length - 1];   // List<T>/Set<T> -> T; Map<K,V> -> V
            if (t instanceof Class<?> c) return c;
        }
        return Object.class;
    }

    // ------------------------------------------------------------------ invocation

    /** Decodes JSON into {@code type}: a class's {@link Decode @Decode} method if one is registered,
     *  otherwise a raw {@code Map} for Map types, else the record type via reflection. */
    private static Object decode(Object json, Class<?> type, Object target, Map<Class<?>, Method> decoders) throws Exception {
        Method dec = decoders.get(type);
        if (dec != null) return call(dec, target, Json.asObject(json));
        // Object = a type-erased lambda (activity/gate/effect registered by name): hand it the raw map.
        if (type == Object.class || Map.class.isAssignableFrom(type)) return Json.asObject(json);
        return RecordMapper.fromJson(json, type);
    }

    /** Invokes a handler method, unwrapping the reflective exception to the real cause. */
    private static Object call(Method m, Object target, Object... args) throws Exception {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) throw ex;   // preserves PermanentActivityException etc.
            if (cause instanceof Error err) throw err;
            throw e;
        }
    }
}
