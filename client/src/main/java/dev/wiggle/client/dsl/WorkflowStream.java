package dev.wiggle.client.dsl;

import dev.wiggle.core.ContextCodec;
import dev.wiggle.core.Node;
import dev.wiggle.core.RetryPolicy;
import dev.wiggle.core.WorkflowDefinition;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

/**
 * A lazily-built workflow pipeline with a Stream-shaped API: intermediate operations
 * append nodes and return a stream, and the terminal {@link #build()} produces the
 * artifact. Nothing executes at definition time.
 *
 * One deliberate departure from {@code java.util.stream}: the context type does not
 * change from step to step. A workflow context is a durable document that survives
 * process restarts and is merged across parallel branches, so a single type for the
 * whole pipeline is what actually models the storage. {@code map} is therefore closer
 * to {@code UnaryOperator<T>} than to {@code Function<T,R>}.
 */
public final class WorkflowStream<T> {

    private static final int NEXT = 0;
    private static final int ALT_NEXT = 1;

    private final Pipeline<T> pipeline;
    private final Consumer<String> startSink;
    /** Non-null when this stream is a fork branch: where a short-circuited branch must land. */
    private final String enclosingJoinId;
    private List<int[]> openSlots = new ArrayList<>();   // parallel arrays with openNodes
    private List<String> openNodes = new ArrayList<>();
    private String lastStepId;
    private boolean consumed;

    private WorkflowStream(Pipeline<T> pipeline, Consumer<String> startSink, String enclosingJoinId) {
        this.pipeline = pipeline;
        this.startSink = startSink;
        this.enclosingJoinId = enclosingJoinId;
    }

    static <T> WorkflowStream<T> root(Pipeline<T> pipeline) {
        return new WorkflowStream<>(pipeline, id -> pipeline.startNode = id, null);
    }

    // ------------------------------------------------- intermediate operations

    /** Runs {@code fn} on a worker and stores the result as the new context. */
    public WorkflowStream<T> map(String stepName, Activity<T> fn) {
        Objects.requireNonNull(fn, "activity");
        String activity = pipeline.activityFor(stepName);
        ContextCodec<T> codec = pipeline.codec;
        pipeline.handlers.put(activity,
                json -> dev.wiggle.core.Json.shallowDiff(json, codec.encode(fn.apply(codec.decode(json)))));
        String id = pipeline.nextId("n");
        pipeline.put(Node.task(id, stepName, activity, pipeline.defaultQueue, pipeline.defaultRetry));
        pipeline.queues.add(pipeline.defaultQueue);
        attach(id);
        lastStepId = id;
        return this;
    }

    /** Alias for {@link #map} that reads better for steps run for their effect. */
    public WorkflowStream<T> then(String stepName, Activity<T> fn) {
        return map(stepName, fn);
    }

    /** Runs {@code fn} on a worker but keeps the context unchanged. */
    public WorkflowStream<T> peek(String stepName, SideEffect<T> fn) {
        Objects.requireNonNull(fn, "side effect");
        String activity = pipeline.activityFor(stepName);
        ContextCodec<T> codec = pipeline.codec;
        pipeline.handlers.put(activity, json -> {
            fn.accept(codec.decode(json));
            return null;
        });
        String id = pipeline.nextId("n");
        pipeline.put(Node.task(id, stepName, activity, pipeline.defaultQueue, pipeline.defaultRetry));
        pipeline.queues.add(pipeline.defaultQueue);
        attach(id);
        lastStepId = id;
        return this;
    }

    /**
     * Continues only while the predicate holds. A false result ends the instance
     * successfully with the termination reason {@code "filtered:<stepName>"} -- the
     * workflow equivalent of an empty stream, not an error.
     *
     * Inside a fork branch the false path short-circuits to the enclosing join instead,
     * so the branch still arrives at the barrier and its siblings are not stranded.
     */
    public WorkflowStream<T> filter(String stepName, Predicate<T> test) {
        Objects.requireNonNull(test, "predicate");
        String activity = pipeline.activityFor(stepName);
        ContextCodec<T> codec = pipeline.codec;
        pipeline.handlers.put(activity, json -> test.test(codec.decode(json)));
        String id = pipeline.nextId("n");
        pipeline.put(Node.predicate(id, stepName, activity, pipeline.defaultQueue, pipeline.defaultRetry));
        pipeline.queues.add(pipeline.defaultQueue);
        attach(id);

        if (enclosingJoinId != null) {
            pipeline.wire(id, ALT_NEXT, enclosingJoinId);
        } else {
            String stop = pipeline.nextId("n");
            pipeline.put(Node.end(stop, true, "filtered:" + stepName));
            pipeline.wire(id, ALT_NEXT, stop);
        }

        openNodes = new ArrayList<>(List.of(id));
        openSlots = new ArrayList<>(List.of(new int[]{NEXT}));
        lastStepId = id;
        return this;
    }

    /** Server-side timer. No worker is occupied while the instance waits. */
    public WorkflowStream<T> sleep(Duration duration) {
        return sleep("sleep-" + duration.toMillis() + "ms", duration);
    }

    public WorkflowStream<T> sleep(String stepName, Duration duration) {
        if (duration.isNegative()) throw new IllegalArgumentException("sleep duration must not be negative");
        pipeline.stepNames.add(stepName);
        String id = pipeline.nextId("n");
        pipeline.put(Node.sleep(id, stepName, duration.toMillis()));
        attach(id);
        lastStepId = null;
        return this;
    }

    /**
     * Fans out into parallel branches and waits for all of them. Branches execute
     * independently and may be picked up by different workers. Each step writes back
     * only the context fields it actually changed, so branches touching different
     * fields merge cleanly; if two branches write the same field, the later write wins.
     */
    @SafeVarargs
    public final WorkflowStream<T> fork(Branch<T>... branches) {
        if (branches.length < 2) throw new IllegalArgumentException("fork needs at least two branches");
        String forkId = pipeline.nextId("fork");
        pipeline.put(Node.fork(forkId, "fork"));
        attach(forkId);

        String joinId = pipeline.nextId("join");
        pipeline.put(Node.join(joinId, "join", branches.length));

        List<String> starts = new ArrayList<>(branches.length);
        for (Branch<T> branch : branches) {
            String[] start = new String[1];
            WorkflowStream<T> sub = new WorkflowStream<>(pipeline, id -> start[0] = id, joinId);
            WorkflowStream<T> tail = branch.body().apply(sub);
            if (start[0] == null) {
                throw new IllegalArgumentException("branch '" + branch.name() + "' defines no steps");
            }
            starts.add(start[0]);
            tail.wireOpenEndsTo(joinId);
        }
        pipeline.put(pipeline.get(forkId).withBranches(starts));

        openNodes = new ArrayList<>(List.of(joinId));
        openSlots = new ArrayList<>(List.of(new int[]{NEXT}));
        lastStepId = null;
        return this;
    }

    /** Overrides the retry policy of the step that was just added. */
    public WorkflowStream<T> retry(RetryPolicy policy) {
        if (lastStepId == null) {
            throw new IllegalStateException("retry() must directly follow map(), peek() or filter()");
        }
        Node n = pipeline.get(lastStepId);
        pipeline.put(new Node(n.id(), n.kind(), n.name(), n.activity(), n.queue(), policy, n.sleepMillis(),
                n.next(), n.altNext(), n.branches(), n.expected(), n.success(), n.reason()));
        return this;
    }

    /** Routes the step that was just added to a dedicated queue (for worker specialisation). */
    public WorkflowStream<T> onQueue(String queue) {
        if (lastStepId == null) {
            throw new IllegalStateException("onQueue() must directly follow map(), peek() or filter()");
        }
        Node n = pipeline.get(lastStepId);
        pipeline.put(new Node(n.id(), n.kind(), n.name(), n.activity(), queue, n.retry(), n.sleepMillis(),
                n.next(), n.altNext(), n.branches(), n.expected(), n.success(), n.reason()));
        pipeline.queues.add(queue);
        return this;
    }

    /** Sets the queue used by every subsequently defined step. */
    public WorkflowStream<T> defaultQueue(String queue) {
        pipeline.defaultQueue = Objects.requireNonNull(queue);
        return this;
    }

    // ------------------------------------------------------ terminal operation

    public Blueprint<T> build() {
        if (consumed) throw new IllegalStateException("this workflow has already been built");
        consumed = true;
        if (pipeline.startNode == null) throw new IllegalStateException("workflow defines no steps");

        String endId = pipeline.nextId("end");
        pipeline.put(Node.end(endId, true, null));
        wireOpenEndsTo(endId);

        int version = WorkflowDefinition.contentVersion(pipeline.name, pipeline.startNode, pipeline.nodes.values());
        WorkflowDefinition def = new WorkflowDefinition(pipeline.name, version, pipeline.startNode,
                new LinkedHashMap<>(pipeline.nodes), pipeline.queues);
        validate(def);
        return new Blueprint<>(def, pipeline.handlers, pipeline.codec);
    }

    private static void validate(WorkflowDefinition def) {
        for (Node n : def.nodes().values()) {
            if (n.next() != null && !def.nodes().containsKey(n.next())) {
                throw new IllegalStateException("node " + n.id() + " points at unknown node " + n.next());
            }
            if (n.altNext() != null && !def.nodes().containsKey(n.altNext())) {
                throw new IllegalStateException("node " + n.id() + " points at unknown node " + n.altNext());
            }
            switch (n.kind()) {
                case TASK, PREDICATE -> {
                    if (n.next() == null) throw new IllegalStateException("node " + n.id() + " has no successor");
                    if (n.kind() == dev.wiggle.core.NodeKind.PREDICATE && n.altNext() == null) {
                        throw new IllegalStateException("predicate " + n.id() + " has no false branch");
                    }
                }
                case SLEEP, FORK, JOIN -> {
                    if (n.kind() != dev.wiggle.core.NodeKind.FORK && n.next() == null) {
                        throw new IllegalStateException("node " + n.id() + " has no successor");
                    }
                    if (n.kind() == dev.wiggle.core.NodeKind.FORK && n.branches().size() < 2) {
                        throw new IllegalStateException("fork " + n.id() + " has fewer than two branches");
                    }
                }
                case END -> { }
            }
        }
    }

    // ---------------------------------------------------------------- plumbing

    private void attach(String id) {
        if (openNodes.isEmpty()) {
            startSink.accept(id);
        } else {
            wireOpenEndsTo(id);
        }
        openNodes = new ArrayList<>(List.of(id));
        openSlots = new ArrayList<>(List.of(new int[]{NEXT}));
    }

    private void wireOpenEndsTo(String target) {
        for (int i = 0; i < openNodes.size(); i++) {
            pipeline.wire(openNodes.get(i), openSlots.get(i)[0], target);
        }
        openNodes = new ArrayList<>();
        openSlots = new ArrayList<>();
    }
}
