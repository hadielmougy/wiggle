package dev.wiggle.client.worker;

/**
 * Execution metadata for the activity currently running on this thread. A worker publishes
 * it around each invocation, so an activity can read {@code Step.attempt()} (or the step
 * name / instance id) without threading extra parameters through its signature. The values
 * belong to the task the calling thread is executing right now.
 *
 * <p>Only valid inside an activity body; calling these anywhere else throws.
 */
public final class Step {

    /** Immutable snapshot of the running task's identity. */
    public record Info(int attempt, String name, String instanceId) {}

    private static final ThreadLocal<Info> CURRENT = new ThreadLocal<>();

    private Step() {}

    /** The engine-global attempt number: 1 on the first try, incremented on every retry. */
    public static int attempt() { return current().attempt(); }

    /** The step's name, as given to {@code map}/{@code filter}/{@code peek}. */
    public static String name() { return current().name(); }

    /** The workflow instance this task belongs to. */
    public static String instanceId() { return current().instanceId(); }

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
