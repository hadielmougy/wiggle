package dev.wiggle.client.worker;

import dev.wiggle.client.dsl.ActivityHandler;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.core.NodeKind;
import dev.wiggle.core.TaskActivation;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The data plane. A worker registers its blueprints, then pulls work: it only ever
 * asks for as many tasks as it has free slots, so the server never overwhelms it and
 * backpressure is a property of the protocol rather than a thing to configure.
 *
 * Workers hold no workflow state. Losing one loses only the in-flight leases, which
 * the server's leader reclaims once they expire.
 */
public final class Worker implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Worker.class.getName());

    private final WiggleClient client;
    private final String workerId;
    private final WorkerOptions options;
    private final Map<String, ActivityHandler> handlers = new ConcurrentHashMap<>();
    private final Map<String, NodeKind> kinds = new ConcurrentHashMap<>();
    private final Set<String> queues = ConcurrentHashMap.newKeySet();
    private final List<Blueprint<?>> blueprints = new CopyOnWriteArrayList<>();

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger inFlight = new AtomicInteger();
    private ExecutorService executor;
    private ScheduledExecutorService heartbeats;
    private Thread pollThread;

    private ThreadFactory heartbeatThreadFactory = new ThreadFactory() {
        private final AtomicInteger n = new AtomicInteger();
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "wiggle-heartbeat-" + workerId + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    };

    public Worker(WiggleClient client, String workerId) {
        this(client, workerId, WorkerOptions.defaults());
    }

    public Worker(WiggleClient client, String workerId, WorkerOptions options) {
        this.client = client;
        this.workerId = workerId;
        this.options = options;
    }

    public String workerId() { return workerId; }

    public int inFlight() { return inFlight.get(); }

    /** Adds a workflow's handlers to this worker's dispatch table. */
    public Worker register(Blueprint<?> blueprint) {
        blueprints.add(blueprint);
        handlers.putAll(blueprint.handlers());
        blueprint.definition().nodes().values().stream()
                .filter(n -> n.isWorkerDispatched())
                .forEach(n -> {
                    kinds.put(n.activity(), n.kind());
                    if (n.queue() != null) queues.add(n.queue());
                });
        return this;
    }

    public Worker start() {
        if (!running.compareAndSet(false, true)) return this;
        if (options.registerOnStart()) {
            for (Blueprint<?> bp : blueprints) client.register(bp);
        }
        executor = Executors.newVirtualThreadPerTaskExecutor();
        heartbeats = Executors.newScheduledThreadPool(heartbeatThreads(), heartbeatThreadFactory);
        pollThread = new Thread(this::pollLoop, "wiggle-worker-" + workerId);
        pollThread.setDaemon(true);
        pollThread.start();
        LOG.log(System.Logger.Level.INFO, () -> "worker " + workerId + " polling queues " + queues
                + " with concurrency " + options.concurrency());
        return this;
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                int free = options.concurrency() - inFlight.get();
                if (free <= 0) {
                    sleep(options.idleBackoff().toMillis());
                    continue;
                }
                List<TaskActivation> tasks = client.poll(workerId, queues, free,
                        options.lease().toMillis(), options.longPollWait().toMillis());
                if (tasks.isEmpty()) {
                    sleep(options.idleBackoff().toMillis());
                    continue;
                }
                for (TaskActivation task : tasks) {
                    inFlight.incrementAndGet();
                    executor.submit(() -> {
                        try {
                            execute(task);
                        } finally {
                            inFlight.decrementAndGet();
                        }
                    });
                }
            } catch (RuntimeException e) {
                if (!running.get()) return;
                LOG.log(System.Logger.Level.WARNING, "poll failed: " + e.getMessage());
                sleep(options.errorBackoff().toMillis());
            }
        }
    }

    private void execute(TaskActivation task) {
        ActivityHandler handler = handlers.get(task.activity());
        if (handler == null) {
            reportFailure(task, "no handler registered for activity '" + task.activity() + "'", false);
            return;
        }
        ScheduledFuture<?> lease = scheduleHeartbeat(task);
        Step.begin(new Step.Info(task.attempt(), task.stepName(), task.instanceId()));
        try {
            Object result = handler.invoke(task.context());
            if (task.kind() == NodeKind.PREDICATE && !(result instanceof Boolean)) {
                reportFailure(task, "predicate '" + task.stepName() + "' returned "
                        + (result == null ? "null" : result.getClass().getSimpleName()), false);
                return;
            }
            client.complete(task.taskId(), task.leaseOwner(),
                    task.kind() == NodeKind.PREDICATE ? Map.of("value", result) : result);
        } catch (PermanentActivityException e) {
            reportFailure(task, describe(e), false);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.DEBUG,
                    () -> "step " + task.stepName() + " of " + task.instanceId() + " failed: " + e);
            reportFailure(task, describe(e), true);
        } catch (Throwable t) {
            reportFailure(task, describe(t), false);
            throw t;
        } finally {
            // Task is settled (completed or failed); stop extending its lease.
            lease.cancel(false);
            Step.end();
        }
    }

    /** A small pool: heartbeats are brief RPCs, so a handful of threads covers any concurrency. */
    private int heartbeatThreads() {
        return Math.max(1, Math.min(4, options.concurrency()));
    }

    /**
     * Keeps the task's lease alive for as long as the handler runs. Fires at a third of
     * the lease so a single missed beat is not fatal, and each beat extends by a full
     * lease. The returned future is cancelled once the task is settled.
     */
    private ScheduledFuture<?> scheduleHeartbeat(TaskActivation task) {
        long leaseMillis = options.lease().toMillis();
        long period = Math.max(1, leaseMillis / 3);
        return heartbeats.scheduleWithFixedDelay(() -> {
            try {
                client.heartbeat(task.taskId(), task.leaseOwner(), leaseMillis);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.DEBUG,
                        () -> "heartbeat for task " + task.taskId() + " failed: " + e.getMessage());
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    private void reportFailure(TaskActivation task, String message, boolean retryable) {
        try {
            client.fail(task.taskId(), task.leaseOwner(), message, retryable);
        } catch (RuntimeException e) {
            // The lease will expire and the leader will reclaim the task; nothing else to do.
            LOG.log(System.Logger.Level.WARNING,
                    "could not report failure of task " + task.taskId() + ": " + e.getMessage());
        }
    }

    private static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    private static void sleep(long millis) {
        if (millis <= 0) return;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        if (pollThread != null) pollThread.interrupt();
        if (executor != null) {
            executor.shutdown();
            try {
                executor.awaitTermination(options.lease().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (heartbeats != null) heartbeats.shutdownNow();
    }
}
