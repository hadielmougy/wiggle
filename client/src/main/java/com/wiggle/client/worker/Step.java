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

    /** Immutable snapshot of the running task's identity (plus forEach item scope, when present). */
    public record Info(int attempt, String name, String instanceId,
                       Object base, long itemIndex, String itemMapKey) {
        public Info(int attempt, String name, String instanceId) {
            this(attempt, name, instanceId, null, 0, null);
        }
    }

    private static final ThreadLocal<Info> CURRENT = new ThreadLocal<>();

    private Step() {}

    /** The engine-global attempt number: 1 on the first try, incremented on every retry. */
    public static int attempt() { return current().attempt(); }

    /** The step's name, as given to {@code map}/{@code filter}/{@code peek}. */
    public static String name() { return current().name(); }

    /** The workflow instance this task belongs to. */
    public static String instanceId() { return current().instanceId(); }

    /** Inside a forEach item step: the frozen pre-forEach context, as a JSON map. Read-only —
     *  an item can never write the base; only the combine's return reaches the shared context. */
    public static Map<String, Object> base() {
        Object b = requireItemScope().base();
        return Json.asObject(b);
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
        if (info.base() == null) {
            throw new IllegalStateException("Step.base()/itemIndex()/itemMapKey() are only available "
                    + "inside a forEach item step (elsewhere the context IS the handler's parameter)");
        }
        return info;
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

    static void end() { CURRENT.remove(); }
}
