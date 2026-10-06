package com.wiggle.client.worker;

import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.RecordMapper;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.ScratchKeys;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * The worker's reflective seam: turns a {@link ForFlow @ForFlow}-annotated object into
 * executable {@link ActivityHandler}s. Two pure operations -- no I/O, no worker runtime state:
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
 * A method's signature defines its step: one input parameter (decoded from JSON into its type); a
 * {@code boolean} return is a gate, {@code void} an effect, anything else a task whose return
 * REPLACES the context. A combine's parameters are found by type, in any order: a fork's arms, or a
 * forEach's or created branches' results as collections, and the frozen base for a parameter no
 * result matches. Its return is the complete post-join context.
 */
final class HandlerBinder {

    private HandlerBinder() {}

    /** One step candidate: the object to invoke on and its handler method — a plain
     *  {@code @ForFlow} method (target = the handlers object) or a factory-produced typed
     *  activity (target = the activity instance, method = its execute/test/apply; compensate
     *  set when the instance implements {@link Compensable}). */
    record Candidate(Object target, Method method, Method compensate) {}

    /** A handler source's inventory: its step candidates keyed by canonical name, and its
     *  {@link Decode @Decode} decoders (living on the handlers object) keyed by decoded type. */
    record HandlerSet(String workflow, Object target, Map<String, Candidate> byName,
                      Map<Class<?>, Method> decoders) {
        public HandlerSet withFlowName(String flowName) {
            return new HandlerSet(flowName, target, byName, decoders);
        }
    }

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
     * Inventories a {@link ForFlow @ForFlow}-annotated object. Each public instance method with
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

    private static @Nullable String getWorkflowName(Object handlerObject) {
        if (handlerObject == null) throw new IllegalArgumentException("handlers object is required");
        ForFlow ann = handlerObject.getClass().getAnnotation(ForFlow.class);
        if (ann == null) {
            return null;
        }
        String workflow = ann.value();
        if (workflow == null || workflow.isBlank()) {
            throw new IllegalArgumentException("@ForFlow on " + handlerObject.getClass().getName()
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
            if (node.compensable() && c.compensate() == null) {
                throw new IllegalStateException("step '" + node.name() + "' declares an undo "
                        + "but its handler is not Compensable — implement Compensable on the "
                        + "activity (or drop the declaration)");
            }
            if (!node.compensable() && c.compensate() != null) {
                throw new IllegalStateException("activity for step '" + node.name() + "' is "
                        + "Compensable but the step does not declare an undo — a silently "
                        + "unused undo is a lie; declare it in the topology (or drop Compensable)");
            }
            bindings.add(new Binding(node.activity(), node.name(),
                    node.queue() != null ? node.queue() : set.workflow(),
                    buildHandler(set, node, c),
                    compensatorHandler(set, c)));
        }
        return new Result(List.copyOf(bindings), List.copyOf(unserved));
    }

    /**
     * Binds one step a handler created at run time ({@code Step.create}), the first time it is
     * dispatched: it is in no graph, so it is matched by name alone and its signature checked against
     * {@code kind} here rather than at startup. A non-null {@code collectKey} makes it a combine of
     * branches. Null when the set has no method of that name.
     */
    static Binding bindCreated(HandlerSet set, String activity, String step, NodeKind kind, String collectKey) {
        Candidate c = set.byName().get(canonicalName(step));
        if (c == null) return null;
        Node node = kind == NodeKind.PREDICATE
                ? Node.predicate(activity, step, activity, set.workflow(), null)
                : Node.task(activity, step, activity, set.workflow(), null);
        if (collectKey != null) node = node.withCollectKey(collectKey);
        return new Binding(activity, step, set.workflow(), buildHandler(set, node, c), compensatorHandler(set, c));
    }

    /** The compensator wrapper for a step, when its typed activity implements {@link Compensable}:
     *  decodes the post-step snapshot into the method's parameter type and invokes it (an effect —
     *  no return). Null when the step has no compensator. */
    private static Compensator compensatorHandler(HandlerSet set, Candidate c) {
        if (c.compensate() == null) return null;
        // An activity maps A -> B, so the two snapshots decode into different types: the input into
        // execute's parameter, the result into its return.
        Class<?> inType = c.method().getParameterTypes()[0];
        Class<?> outType = c.method().getReturnType();
        Class<?> resultType = outType == void.class || outType.isPrimitive() ? inType : outType;
        return (input, result) -> {
            Object in = decode(input, inType, set.target(), set.decoders());
            Object out = decode(result, resultType, set.target(), set.decoders());
            call(c.compensate(), c.target(), new Object[]{new Snapshots(in, out)});
        };
    }

    /** The {@link Compensation} handed to a compensator: both snapshots, already decoded. */
    private record Snapshots(Object input, Object result) implements Compensation<Object, Object> {}

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


    /** Builds the handler for a graph node from its matched candidate, validating signature vs kind. */
    private static ActivityHandler buildHandler(HandlerSet set, Node node, Candidate c) {
        Method m = c.method();
        Object target = c.target();          // the invocation target: handlers object OR activity
        Object decoderOwner = set.target();  // @Decode methods always live on the handlers object
        Map<Class<?>, Method> decoders = set.decoders();
        if (node.isCombine()) {
            return node.collectKey() != null
                    ? collectionCombineHandler(set, node, m, target)
                    : forkCombineHandler(set, node, m, target);
        }

        Class<?> ret = m.getReturnType();
        boolean returnsBool = (ret == boolean.class || ret == Boolean.class);
        Class<?> input = inputType(node, m);
        if (node.kind() == NodeKind.PREDICATE) {
            if (!returnsBool) {
                throw new IllegalStateException("gate '" + node.name() + "' handler '" + m.getName()
                        + "' must take the context and return boolean");
            }
            return ctx -> call(m, target, decode(ctx, input, decoderOwner, decoders));
        }
        if (returnsBool) {
            throw new IllegalStateException("step '" + node.name() + "' handler '" + m.getName()
                    + "' must take the context and return the next context (or void for an effect)");
        }
        boolean effect = (ret == void.class || ret == Void.class);
        if (effect) {
            return ctx -> { call(m, target, decode(ctx, input, decoderOwner, decoders)); return null; };
        }
        return ctx -> {
            // The return IS the next context: it is sent whole and REPLACES the previous value
            // server-side (no diff, no merge). A null return leaves the context untouched.
            Object out = call(m, target, decode(ctx, input, decoderOwner, decoders));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    /** A step's one parameter: its input. The frozen base, where there is one, is {@code Step.base()}. */
    private static Class<?> inputType(Node node, Method m) {
        if (m.getParameterCount() != 1) {
            throw new IllegalStateException("step '" + node.name() + "' handler '" + m.getName()
                    + "' must take exactly one parameter, the context; inside a forEach item or a created "
                    + "branch, read the frozen base with Step.base(Type.class)");
        }
        return m.getParameterTypes()[0];
    }

    /**
     * The type a step leaves the context in: what it returns, or -- for an effect or a gate, which
     * pass their input on -- what it takes. This is the type a combine matches the step's result by.
     */
    static Class<?> producedType(Method m) {
        Class<?> ret = m.getReturnType();
        boolean passesInputOn = ret == void.class || ret == Void.class || ret == boolean.class || ret == Boolean.class;
        if (!passesInputOn) return ret;
        return m.getParameterCount() == 0 ? Object.class : m.getParameterTypes()[0];
    }

    /** The type the step named {@code step} leaves the context in, or null when this set has no such step. */
    private static Class<?> producedType(HandlerSet set, String step) {
        Candidate c = step == null ? null : set.byName().get(canonicalName(step));
        return c == null ? null : producedType(c.method());
    }

    /** What a combine parameter receives: the base, one fork arm, or every result its element type takes. */
    private sealed interface Arg {
        record Base() implements Arg {}
        record Arm(int index) implements Arg {}
        record Results(Class<?> element) implements Arg {}
    }

    /**
     * Whether a combine parameter takes results rather than one value: a {@code List}, a {@code Set},
     * or a {@code Map} whose value type says what it holds ({@code Map<String, Shipment>}). A raw
     * {@code Map} or a {@code Map<String, Object>} is a context, the shape most contexts have.
     */
    private static boolean isCollection(java.lang.reflect.Parameter p) {
        Class<?> t = p.getType();
        if (Collection.class.isAssignableFrom(t)) return true;
        return Map.class.isAssignableFrom(t) && elementType(p) != Object.class;
    }

    /** Whether a value the step typed {@code produced} can be passed as {@code wanted}. */
    private static boolean assignable(Class<?> wanted, Class<?> produced) {
        return box(wanted).isAssignableFrom(box(produced));
    }

    private static Class<?> box(Class<?> t) {
        return t.isPrimitive() ? java.lang.invoke.MethodType.methodType(t).wrap().returnType() : t;
    }

    /**
     * A fork's combine. Each parameter is found by its type, in any order: a parameter takes the
     * arm whose step produces its type, and a collection parameter takes every arm its element type
     * matches. Parameters sharing a type take that type's arms in fork order, matched from the last
     * parameter back, so a parameter left over at the front receives the pre-fork context; at most
     * one may. A combine need not take every arm.
     *
     * <p>When this worker cannot tell every arm's type, because it holds no handler for some arm's
     * step, the parameters take the arms in fork order and the base is {@code Step.base()}.
     */
    private static ActivityHandler forkCombineHandler(HandlerSet set, Node node, Method m, Object target) {
        List<String> arms = node.armNames();
        java.lang.reflect.Parameter[] params = m.getParameters();
        List<Class<?>> armTypes = new ArrayList<>(arms.size());
        for (String arm : arms) armTypes.add(producedType(set, arm));
        Arg[] plan = armTypes.contains(null)
                ? armsInOrder(node, m, params, arms)
                : armsByType(node, m, params, arms, armTypes);
        return ctx -> {
            Map<String, Object> map = Json.asObject(ctx);
            Map<String, Object> stripped = new LinkedHashMap<>(map);
            arms.stream().map(ScratchKeys::arm).forEach(stripped::remove);
            Object base = combineBase(stripped);
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                java.lang.reflect.Parameter p = params[i];
                args[i] = switch (plan[i]) {
                    case Arg.Base b -> decode(base, p.getType(), set.target(), set.decoders());
                    case Arg.Arm a -> decode(map.get(ScratchKeys.arm(arms.get(a.index()))), p.getType(),
                            set.target(), set.decoders());
                    case Arg.Results r -> {
                        List<Object> values = new ArrayList<>();
                        Map<String, Object> byArm = new LinkedHashMap<>();
                        for (int a = 0; a < arms.size(); a++) {
                            if (!assignable(r.element(), armTypes.get(a))) continue;
                            Object v = map.get(ScratchKeys.arm(arms.get(a)));
                            values.add(v);
                            byArm.put(arms.get(a), v);
                        }
                        yield decodeCollection(Map.class.isAssignableFrom(p.getType()) ? byArm : values, p,
                                set.target(), set.decoders(), node.name());
                    }
                };
            }
            Object out = Step.withBase(base, () -> call(m, target, args));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    private static Arg[] armsByType(Node node, Method m, java.lang.reflect.Parameter[] params,
                                    List<String> arms, List<Class<?>> armTypes) {
        Arg[] plan = new Arg[params.length];
        boolean[] taken = new boolean[arms.size()];
        int baseAt = -1;
        for (int i = params.length - 1; i >= 0; i--) {
            Class<?> type = params[i].getType();
            if (isCollection(params[i])) {
                plan[i] = new Arg.Results(elementType(params[i]));
                continue;
            }
            int pick = -1;
            for (int a = arms.size() - 1; a >= 0; a--) {
                if (taken[a] || !assignable(type, armTypes.get(a))) continue;
                if (pick < 0 || (armTypes.get(a) == type && armTypes.get(pick) != type)) pick = a;
            }
            if (pick >= 0) {
                taken[pick] = true;
                plan[i] = new Arg.Arm(pick);
                continue;
            }
            if (baseAt >= 0) {
                throw new IllegalStateException(combineWhat(node, m) + ": parameters " + (i + 1)
                        + " and " + (baseAt + 1) + " match no arm, so both would take the pre-fork context; "
                        + "the arms produce " + describe(arms, armTypes));
            }
            baseAt = i;
            plan[i] = new Arg.Base();
        }
        return plan;
    }

    private static Arg[] armsInOrder(Node node, Method m, java.lang.reflect.Parameter[] params, List<String> arms) {
        if (params.length != arms.size()) {
            throw new IllegalStateException(combineWhat(node, m) + " takes " + params.length
                    + " parameter(s), but this worker holds no handler for some of the fork's arms " + arms
                    + ", so it cannot match them by type: bind those steps here too, or take one "
                    + "parameter per arm in fork order");
        }
        Arg[] plan = new Arg[params.length];
        for (int i = 0; i < params.length; i++) plan[i] = new Arg.Arm(i);
        return plan;
    }

    private static String describe(List<String> arms, List<Class<?>> types) {
        StringBuilder b = new StringBuilder();
        for (int a = 0; a < arms.size(); a++) {
            if (a > 0) b.append(", ");
            b.append(arms.get(a)).append(": ").append(types.get(a).getSimpleName());
        }
        return b.toString();
    }

    private static String combineWhat(Node node, Method m) {
        return "combine '" + node.name() + "' handler '" + m.getName() + "'";
    }

    /** The combine's base (its {@code Step.base()} view): the dispatched context minus the
     *  staged keys — or, when that leaves nothing and the activation carried a base, the
     *  activation's base: a combine nested inside a scope whose view is not a JSON object gets
     *  the staged inputs alone as its context, with the enclosing view riding on the base. */
    private static Object combineBase(Map<String, Object> stripped) {
        if (!stripped.isEmpty()) return stripped;
        Object ambient = Step.current().base();
        return ambient != null ? ambient : stripped;
    }

    /**
     * The combine of a forEach or of a step's created branches: results arrive as one collection,
     * a {@code List} in order, a {@code Set}, or a {@code Map} by key. Parameters are found by type,
     * in any order. With one collection parameter it takes every result; with several, each takes
     * the results whose step produces its element type. A parameter that is not a collection
     * receives the context from before the fan-out; at most one may.
     */
    private static ActivityHandler collectionCombineHandler(HandlerSet set, Node node, Method m, Object target) {
        String scratch = node.collectKey();
        java.lang.reflect.Parameter[] params = m.getParameters();
        int collections = 0;
        int baseAt = -1;
        // A lone untyped Map is the results, as it always was, when nothing else can be.
        int onlyMap = params.length == 1 && Map.class.isAssignableFrom(params[0].getType()) ? 0 : -1;
        for (int i = 0; i < params.length; i++) {
            if (i == onlyMap || isCollection(params[i])) {
                collections++;
            } else if (baseAt >= 0) {
                throw new IllegalStateException(combineWhat(node, m) + ": parameters " + (baseAt + 1)
                        + " and " + (i + 1) + " are not collections, so both would take the context "
                        + "from before the fan-out; the results arrive as a List, Set or Map");
            } else {
                baseAt = i;
            }
        }
        if (collections == 0) {
            throw new IllegalStateException(combineWhat(node, m) + " needs a collection parameter "
                    + "(List/Set/Map) for the results");
        }
        boolean partition = collections > 1;
        int baseParam = baseAt;
        return ctx -> {
            Map<String, Object> map = Json.asObject(ctx);
            Object staged = map.get(scratch);
            Object steps = map.get(ScratchKeys.steps(scratch));
            Map<String, Object> stripped = new LinkedHashMap<>(map);
            stripped.remove(scratch);
            stripped.remove(ScratchKeys.steps(scratch));
            Object base = combineBase(stripped);
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                java.lang.reflect.Parameter p = params[i];
                if (i == baseParam) {
                    args[i] = decode(base, p.getType(), set.target(), set.decoders());
                } else {
                    Object results = partition ? ofType(set, node, staged, steps, elementType(p)) : staged;
                    args[i] = decodeCollection(results, p, set.target(), set.decoders(), node.name());
                }
            }
            Object out = Step.withBase(base, () -> call(m, target, args));
            return out == null ? null : RecordMapper.toJson(out);
        };
    }

    /** The staged results whose producing step leaves the context as {@code element}, shaped as staged. */
    private static Object ofType(HandlerSet set, Node node, Object staged, Object steps, Class<?> element) {
        if (staged instanceof Map<?, ?> byKey) {
            Map<?, ?> stepsByKey = steps instanceof Map<?, ?> s ? s : Map.of();
            Map<String, Object> out = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : byKey.entrySet()) {
                if (assignable(element, resultType(set, node, stepsByKey.get(e.getKey())))) {
                    out.put(String.valueOf(e.getKey()), e.getValue());
                }
            }
            return out;
        }
        List<?> values = staged instanceof List<?> l ? l : List.of();
        List<?> stepList = steps instanceof List<?> s ? s : List.of();
        List<Object> out = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            Object step = i < stepList.size() ? stepList.get(i) : null;
            if (assignable(element, resultType(set, node, step))) out.add(values.get(i));
        }
        return out;
    }

    private static Class<?> resultType(HandlerSet set, Node node, Object step) {
        Class<?> type = producedType(set, step == null ? null : String.valueOf(step));
        if (type == null) {
            throw new PermanentActivityException("combine '" + node.name() + "' splits its results by type, "
                    + "but this worker holds no handler for the step that produced one of them ("
                    + (step == null ? "a branch that ran no step" : "'" + step + "'") + "), so it cannot "
                    + "tell its type; bind that step here too, or take every result in one collection");
        }
        return type;
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
        throw new IllegalStateException("combine '" + nodeName + "': unsupported collection "
                + "parameter type " + type.getName() + " (use List, Set, or Map)");
    }

    /** The collection parameter's element type from its generics; Object (raw maps) when unknown. */
    private static Class<?> elementType(java.lang.reflect.Parameter p) {
        if (p.getParameterizedType() instanceof java.lang.reflect.ParameterizedType pt) {
            java.lang.reflect.Type[] args = pt.getActualTypeArguments();
            java.lang.reflect.Type t = args[args.length - 1];   // List<T>/Set<T> -> T; Map<K,V> -> V
            if (t instanceof Class<?> c) return c;
            if (t instanceof java.lang.reflect.ParameterizedType inner && inner.getRawType() instanceof Class<?> c) return c;
        }
        return Object.class;
    }


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
