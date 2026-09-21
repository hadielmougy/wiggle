package com.wiggle.client.worker;

import com.wiggle.core.Json;
import com.wiggle.core.RecordMapper;

import java.util.Map;

/**
 * Execution metadata for the activity currently running on this thread. A worker publishes
 * it around each invocation, so an activity can read {@code Step.attempt()} (or the step
 * name / instance id) without threading extra parameters through its signature. The values
 * belong to the task the calling thread is executing right now.
 *
 * <p>Inside a {@code forEach} item step, {@link #base()} exposes the frozen pre-forEach context
 * (read-only — items can never write it; only the combine can), and {@link #itemIndex()} /
 * {@link #itemMapKey()} locate the element in its input collection. The handler's parameter is the
 * item itself.
 *
 * <p>Only valid inside an activity body; calling these anywhere else throws.
 */
public final class Step {

    /** Immutable snapshot of the running task's identity (plus base/item scope, when present). */
    public record Info(int attempt, String name, String instanceId,
                       Object base, boolean itemScope, long itemIndex, String itemMapKey) {
        public Info(int attempt, String name, String instanceId) {
            this(attempt, name, instanceId, null, false, 0, null);
        }
    }

    private static final ThreadLocal<Info> CURRENT = new ThreadLocal<>();

    /** Events emitted so far by the activity running on this thread; drained at report time. */
    private static final ThreadLocal<java.util.List<EmittedEvent>> EMITTED = new ThreadLocal<>();

    private Step() {}

    /** A handler-emitted event, buffered until the step's completion report ships it. */
    public record EmittedEvent(String type, Object payload) { }

    /**
     * Emits a domain event onto the instance's event log. The event is buffered on the worker
     * and rides the step's completion report, where the server appends it to the log in the
     * same transaction that settles the token — so it is committed if and only if this step
     * attempt completes. An attempt that throws after emitting leaves nothing behind; the
     * retry emits fresh. Events surface to consumers at step completion (for LOCAL_ASYNC, at
     * the next batch flush), not at the call.
     *
     * <p>{@code type} is the consumer-facing name; the {@code wf.} prefix is reserved for
     * engine lifecycle events. {@code payload} is any JSON-mappable value, versioned by the
     * emitter's own convention. Only valid inside an activity body.
     */
    public static void emit(String type, Object payload) {
        current();
        if (type == null || type.isBlank() || type.startsWith("wf.")) {
            throw new IllegalArgumentException("event type must be non-blank and not start with "
                    + "the reserved \"wf.\" prefix: " + type);
        }
        java.util.List<EmittedEvent> buffer = EMITTED.get();
        if (buffer == null) {
            buffer = new java.util.ArrayList<>(4);
            EMITTED.set(buffer);
        }
        buffer.add(new EmittedEvent(type, payload));
    }

    /** Takes (and clears) the events this step has emitted; the worker ships them with its report. */
    static java.util.List<EmittedEvent> drainEmitted() {
        java.util.List<EmittedEvent> buffer = EMITTED.get();
        EMITTED.remove();
        return buffer == null ? java.util.List.of() : buffer;
    }

    /** The engine-global attempt number: 1 on the first try, incremented on every retry. */
    public static int attempt() { return current().attempt(); }

    /** The step's name, as given to {@code map}/{@code filter}/{@code peek}. */
    public static String name() { return current().name(); }

    /** The workflow instance this task belongs to. */
    public static String instanceId() { return current().instanceId(); }

    /**
     * The frozen base context, as a JSON map — available wherever a base exists, so handlers can
     * take it ambiently instead of (or as well as) declaring a {@link Context @Context} parameter:
     * <ul>
     *   <li>inside a forEach item step — the pre-forEach context;</li>
     *   <li>inside a fork combine — the pre-fork context (staged arm results excluded);</li>
     *   <li>inside a forEach combine — the pre-forEach context (the collected results excluded).</li>
     * </ul>
     * Read-only by contract: writing it changes nothing; only a handler's return reaches the engine.
     */
    public static Map<String, Object> base() {
        Info info = current();
        if (info.base() == null) {
            throw new IllegalStateException("Step.base() is only available inside a forEach item step "
                    + "or a fork/forEach combine (elsewhere the context IS the handler's parameter)");
        }
        return Json.asObject(info.base());
    }

    /** {@link #base()} decoded into {@code type} (a record or map-compatible class). */
    public static <T> T base(Class<T> type) {
        return type.cast(RecordMapper.fromJson(base(), type));
    }

    /** Inside a forEach item step: this element's position in the input collection. */
    public static long itemIndex() { return requireItemScope().itemIndex(); }

    /** Inside a forEach item step over a MAP input: this element's source key; null for a list. */
    public static String itemMapKey() { return requireItemScope().itemMapKey(); }

    private static Info requireItemScope() {
        Info info = current();
        if (!info.itemScope()) {
            throw new IllegalStateException("Step.itemIndex()/itemMapKey() are only available inside "
                    + "a forEach item step");
        }
        return info;
    }

    /** Runs {@code body} with the base swapped in (a combine wrapper exposing its computed base). */
    static Object withBase(Object base, java.util.concurrent.Callable<Object> body) throws Exception {
        Info prev = CURRENT.get();
        CURRENT.set(new Info(prev.attempt(), prev.name(), prev.instanceId(), base, false,
                prev.itemIndex(), prev.itemMapKey()));
        try {
            return body.call();
        } finally {
            CURRENT.set(prev);
        }
    }

    /** The full snapshot, if a caller wants more than one field. */
    public static Info current() {
        Info info = CURRENT.get();
        if (info == null) {
            throw new IllegalStateException("Step metadata is only available inside a running activity");
        }
        return info;
    }

    // -- worker-internal lifecycle; package-private on purpose --

    static void begin(Info info) { CURRENT.set(info); }

    static void end() {
        CURRENT.remove();
        EMITTED.remove();   // an attempt that failed before draining must not leak into the next task
    }
}
