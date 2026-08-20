package dev.wiggle.client.worker;

import dev.wiggle.client.dsl.ActivityHandler;
import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.worker.WiggleClient.WiggleApiException;
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
 * A worker discovers the live servers (see {@link ServerDirectory}) and polls them
 * round-robin -- one server per cycle, for exactly its free slots -- which reaches the
 * whole shared work pool while keeping the single-poll backpressure invariant intact.
 *
 * Workers hold no workflow state. Losing one loses only the in-flight leases, which
 * the server's leader reclaims once they expire.
 */
public final class Worker implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Worker.class.getName());

    /** How many other servers to try when a poll to the chosen server fails transiently. */
    private static final int MAX_POLL_FAILOVER = 3;

    private final ServerDirectory directory;
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

    private final ThreadFactory heartbeatThreadFactory = new ThreadFactory() {
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

    /**
     * Single-seed worker. The client's target is used as the discovery seed; the worker
     * still learns and rotates across whatever siblings that server reports.
     */
    public Worker(WiggleClient client, String workerId, WorkerOptions options) {
        this(List.of(client.target()), workerId, options);
    }

    /** Multi-seed worker: any of the seeds bootstraps discovery of the full cluster. */
    public Worker(List<String> seeds, String workerId, WorkerOptions options) {
        this.workerId = workerId;
        this.options = options;
        this.directory = new ServerDirectory(seeds, workerId, () -> queues, options.discoveryInterval());
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
        directory.start();
        if (options.registerOnStart()) registerBlueprints();
        executor = Executors.newVirtualThreadPerTaskExecutor();
        heartbeats = Executors.newScheduledThreadPool(heartbeatThreads(), heartbeatThreadFactory);
        pollThread = new Thread(this::pollLoop, "wiggle-worker-" + workerId);
        pollThread.setDaemon(true);
        pollThread.start();
        LOG.log(System.Logger.Level.INFO, () -> "worker " + workerId + " polling queues " + queues
                + " with concurrency " + options.concurrency());
        return this;
    }

    /** Registers each blueprint on a live server, failing over across the cluster on transport errors. */
    private void registerBlueprints() {
        for (Blueprint<?> bp : blueprints) {
            boolean registered = false;
            int attempts = Math.max(1, Math.min(directory.size(), MAX_POLL_FAILOVER));
            for (int i = 0; i < attempts && !registered; i++) {
                ServerDirectory.Server s = directory.next();
                if (s == null) break;
                try {
                    s.client().register(bp);
                    registered = true;
                } catch (WiggleApiException e) {
                    if (e.status() != 0) throw e;   // real rejection, not a transport blip
                    directory.onFailure(s.address());
                }
            }
            if (!registered) {
                LOG.log(System.Logger.Level.WARNING, "could not register blueprint " + bp.name() + " on start");
            }
        }
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                // Backpressure: compute free slots once, then poll exactly that many from
                // ONE server. Never split or fan out this budget, or leases could exceed slots.
                int free = options.concurrency() - inFlight.get();
                if (free <= 0) {
                    sleep(options.idleBackoff().toMillis());
                    continue;
                }
                ServerDirectory.Server server = directory.next();
                if (server == null) {
                    sleep(options.errorBackoff().toMillis());
                    continue;
                }
                PollResult polled = pollWithFailover(server, free);
                if (polled.tasks().isEmpty()) {
                    sleep(options.idleBackoff().toMillis());
                    continue;
                }
                for (TaskActivation task : polled.tasks()) {
                    inFlight.incrementAndGet();
                    executor.submit(() -> {
                        try {
                            execute(task, polled.address());
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

    /**
     * Polls one server for {@code free} tasks, failing over to other servers on transport
     * errors. The budget {@code free} is identical on every attempt and only one poll is
     * ever outstanding, so failover cannot over-lease. Long-poll waits are applied only to
     * the first attempt; retries use a zero wait so the loop returns promptly.
     */
    private PollResult pollWithFailover(ServerDirectory.Server first, int free) {
        int attempts = Math.max(1, Math.min(directory.size(), MAX_POLL_FAILOVER));
        ServerDirectory.Server current = first;
        for (int i = 0; i < attempts; i++) {
            long wait = i == 0 ? options.longPollWait().toMillis() : 0;
            try {
                List<TaskActivation> tasks = current.client().poll(
                        workerId, queues, free, options.lease().toMillis(), wait);
                return new PollResult(current.address(), tasks);
            } catch (WiggleApiException e) {
                if (e.status() != 0) {
                    // A client error (bad request, etc.) -- retrying elsewhere won't help.
                    LOG.log(System.Logger.Level.WARNING,
                            "poll rejected by " + current.address() + ": " + e.getMessage());
                    return PollResult.empty(current.address());
                }
                directory.onFailure(current.address());
                ServerDirectory.Server nextServer = directory.next();
                if (nextServer == null) return PollResult.empty(current.address());
                current = nextServer;
            }
        }
        return PollResult.empty(current.address());
    }

    private record PollResult(String address, List<TaskActivation> tasks) {
        static PollResult empty(String address) { return new PollResult(address, List.of()); }
    }

    private void execute(TaskActivation task, String sourceAddress) {
        // Route this task's results (and its lease heartbeat) back to the server it came from.
        WiggleClient rc = resolveResultClient(sourceAddress);
        if (rc == null) {
            LOG.log(System.Logger.Level.WARNING,
                    "no server to run task " + task.taskId() + "; its lease will expire and be reclaimed");
            return;
        }
        ActivityHandler handler = handlers.get(task.activity());
        if (handler == null) {
            reportFailure(rc, task, "no handler registered for activity '" + task.activity() + "'", false);
            return;
        }
        ScheduledFuture<?> lease = scheduleHeartbeat(task, rc);
        try {
            Object result = handler.invoke(task.context());
            if (task.kind() == NodeKind.PREDICATE && !(result instanceof Boolean)) {
                reportFailure(rc, task, "predicate '" + task.stepName() + "' returned "
                        + (result == null ? "null" : result.getClass().getSimpleName()), false);
                return;
            }
            complete(rc, task, task.kind() == NodeKind.PREDICATE ? Map.of("value", result) : result);
        } catch (PermanentActivityException e) {
            reportFailure(rc, task, describe(e), false);
        } catch (Exception e) {
            LOG.log(System.Logger.Level.DEBUG,
                    () -> "step " + task.stepName() + " of " + task.instanceId() + " failed: " + e);
            reportFailure(rc, task, describe(e), true);
        } catch (Throwable t) {
            reportFailure(rc, task, describe(t), false);
            throw t;
        } finally {
            // Task is settled (completed or failed); stop extending its lease.
            lease.cancel(false);
        }
    }

    private WiggleClient resolveResultClient(String sourceAddress) {
        ServerDirectory.Server origin = directory.clientFor(sourceAddress);
        if (origin != null) return origin.client();
        ServerDirectory.Server fallback = directory.next();
        return fallback != null ? fallback.client() : null;
    }

    /** A small pool: heartbeats are brief RPCs, so a handful of threads covers any concurrency. */
    private int heartbeatThreads() {
        return Math.max(1, Math.min(4, options.concurrency()));
    }

    /**
     * Keeps the task's lease alive for as long as the handler runs, heartbeating the server
     * the task came from. Fires at a third of the lease so a single missed beat is not fatal,
     * and each beat extends by a full lease. Cancelled once the task is settled.
     */
    private ScheduledFuture<?> scheduleHeartbeat(TaskActivation task, WiggleClient rc) {
        long leaseMillis = options.lease().toMillis();
        long period = Math.max(1, leaseMillis / 3);
        return heartbeats.scheduleWithFixedDelay(() -> {
            try {
                rc.heartbeat(task.taskId(), task.leaseOwner(), leaseMillis);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.DEBUG,
                        () -> "heartbeat for task " + task.taskId() + " failed: " + e.getMessage());
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    private void complete(WiggleClient rc, TaskActivation task, Object result) {
        try {
            rc.complete(task.taskId(), task.leaseOwner(), result);
        } catch (WiggleApiException e) {
            if (e.status() != 0) throw e;      // e.g. 409 stale lease -- nothing to retry
            WiggleClient alt = failoverClient(rc);
            if (alt == null) throw e;
            alt.complete(task.taskId(), task.leaseOwner(), result);
        }
    }

    private void reportFailure(WiggleClient rc, TaskActivation task, String message, boolean retryable) {
        try {
            rc.fail(task.taskId(), task.leaseOwner(), message, retryable);
        } catch (WiggleApiException e) {
            WiggleClient alt = e.status() == 0 ? failoverClient(rc) : null;
            if (alt != null) {
                try { alt.fail(task.taskId(), task.leaseOwner(), message, retryable); return; }
                catch (RuntimeException ignored) { /* fall through to log */ }
            }
            // The lease will expire and the leader will reclaim the task; nothing else to do.
            LOG.log(System.Logger.Level.WARNING,
                    "could not report failure of task " + task.taskId() + ": " + e.getMessage());
        }
    }

    private WiggleClient failoverClient(WiggleClient failed) {
        ServerDirectory.Server alt = directory.next();
        return alt != null && alt.client() != failed ? alt.client() : null;
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
        directory.close();
    }
}
