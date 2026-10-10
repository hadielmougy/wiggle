package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.core.CreatedBranch;
import com.wiggle.core.Json;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.core.WorkflowVersion;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * What a worker serves: the handler bound to each activity, the queues those bindings imply, the
 * compiled graphs local execution traverses, and the (workflow, version) pairs the worker claims.
 * Registrations accumulate before start(); {@link #reconcile} resolves them against the registered
 * graphs and installs the bindings.
 */
final class Registrations {

    private static final System.Logger LOG = System.getLogger(Registrations.class.getName());
    private static final String COMPENSATE_SUFFIX = "#compensate";

    /** One {@code registerHandler(...)} call: the scanned methods, and the version they were bound for. */
    private record Registration(HandlerBinder.HandlerSet set, Integer version) {}

    private final Map<String, ActivityHandler> handlers = new ConcurrentHashMap<>();
    private final Set<String> queues = ConcurrentHashMap.newKeySet();
    /** Compiled graphs by "name:version", for local-execution traversal. */
    private final Map<String, WorkflowDefinition> graphs = new ConcurrentHashMap<>();
    private final List<Registration> registrations = new CopyOnWriteArrayList<>();
    /** The (workflow, version) pairs this worker claims; empty while any registration is unversioned. */
    private final Set<WorkflowVersion> servedVersions = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean servesEveryVersion = new AtomicBoolean();

    void register(String flowName, Object handlerObject) {
        HandlerBinder.HandlerSet handlerSet;
        try {
            handlerSet = HandlerBinder.scan(handlerObject);
        } catch (IllegalArgumentException e) {
            LOG.log(System.Logger.Level.ERROR, () -> "Error registering handler " + handlerObject, e);
            throw e;
        }
        if (handlerSet.workflow() == null && flowName == null) {
            throw new IllegalArgumentException("Handler " + handlerObject + " has no flow name or registered with flow name");
        }
        handlerSet = flowName == null ? handlerSet : handlerSet.withFlowName(flowName);
        registrations.add(new Registration(handlerSet, null));
        servesEveryVersion.set(true);
    }

    void register(Object handlerObject, int version) {
        HandlerBinder.HandlerSet set = HandlerBinder.scan(handlerObject);
        registrations.add(new Registration(set, version));
        servedVersions.add(new WorkflowVersion(set.workflow(), version));
    }

    boolean isEmpty() {
        return registrations.isEmpty();
    }

    ActivityHandler handlerFor(String activity) {
        return handlers.get(activity);
    }

    /**
     * {@link #handlerFor(String)}, binding on first use a step a handler created at run time: such a
     * step's activity is in no graph, so it is matched by name against the scanned methods of its
     * workflow -- and so is its undo, which may run on a worker that never ran the step. Any other
     * step binds against the graph only, as at startup.
     */
    ActivityHandler handlerFor(String activity, String nodeId, NodeKind kind) {
        ActivityHandler bound = handlers.get(activity);
        if (bound != null || !CreatedBranch.isCreatedNode(nodeId)) return bound;
        boolean undo = activity.endsWith(COMPENSATE_SUFFIX);
        String stepActivity = undo ? activity.substring(0, activity.length() - COMPENSATE_SUFFIX.length()) : activity;
        int hash = stepActivity.indexOf('#');
        if (hash < 0) return null;
        String workflow = stepActivity.substring(0, hash);
        String name = stepActivity.substring(hash + 1);
        for (Registration r : registrations) {
            if (!workflow.equals(r.set().workflow())) continue;
            HandlerBinder.Binding b = HandlerBinder.bindCreated(r.set(), stepActivity, name, undo ? NodeKind.TASK : kind);
            if (b == null) continue;
            install(b);
            return handlers.get(activity);
        }
        return null;
    }

    WorkflowDefinition graphFor(String key) {
        return graphs.get(key);
    }

    Set<String> queues() {
        return queues;
    }

    /**
     * The versions this worker claims. Empty means every version -- which is the case whenever any
     * registration was unversioned, since the scoping can only be as narrow as the least specific
     * binding.
     */
    Set<WorkflowVersion> claimedVersions() {
        return servesEveryVersion.get() ? Set.of() : Set.copyOf(servedVersions);
    }

    void reconcile(WiggleClient client, WorkerOptions options) {
        for (Registration r : registrations) match(r, client, options);
    }

    /**
     * Resolves a {@link ForFlow @ForFlow} object against the registered graph (fetched here — the
     * binder itself is pure) and installs the resulting bindings. See {@link HandlerBinder}.
     */
    private void match(Registration registration, WiggleClient client, WorkerOptions options) {
        HandlerBinder.HandlerSet set = registration.set();
        WorkflowDefinition def = fetchGraph(client, options, set.workflow(), registration.version());
        HandlerBinder.Result result = HandlerBinder.bind(set, def);
        for (HandlerBinder.Binding b : result.bindings()) {
            if (handlers.containsKey(b.activity())) {
                throw new IllegalStateException("duplicate handler for activity '" + b.activity() + "'");
            }
            install(b);
            queues.add(b.queue());
        }
        graphs.put(def.key(), def);
        if (!result.unserved().isEmpty()) {   // info, not an error: this worker may serve a subset
            LOG.log(System.Logger.Level.INFO, () -> "workflow '" + set.workflow()
                    + "' has steps served by no handler on this worker: " + result.unserved());
        }
    }

    private void install(HandlerBinder.Binding b) {
        handlers.putIfAbsent(b.activity(), b.handler());
        if (b.compensator() != null) {
            // The undo is a normal claimable activity under "<activity>#compensate"; its
            // context is the {input, result} snapshot pair the engine staged.
            HandlerBinder.Compensator comp = b.compensator();
            handlers.putIfAbsent(b.activity() + COMPENSATE_SUFFIX, ctx -> {
                Map<String, Object> snaps = Json.asObject(ctx);
                comp.invoke(snaps.get("input"), snaps.get("result"));
                return null;
            });
        }
    }

    /** Fetches the registered graph, waiting out a registration race up to {@code awaitRegistration}. */
    private static WorkflowDefinition fetchGraph(WiggleClient client, WorkerOptions options,
                                                 String workflow, Integer version) {
        long deadline = System.nanoTime() + options.awaitRegistration().toNanos();
        while (true) {
            try {
                return client.getWorkflow(workflow, version);
            } catch (WiggleClient.WiggleApiException e) {
                boolean notFound = e.status() == 404;
                if (notFound && System.nanoTime() < deadline) {
                    Worker.sleep(250);
                    continue;
                }
                if (notFound) {
                    throw new IllegalStateException("workflow '" + workflow + "' is not registered; register "
                            + "its graph before starting a worker that binds handlers to it (or set "
                            + "WorkerOptions.withAwaitRegistration)", e);
                }
                throw e;
            }
        }
    }
}
