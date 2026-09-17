package com.wiggle.client.worker;

import com.wiggle.client.WiggleClient;
import com.wiggle.core.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * The data plane. A worker binds handlers, then pulls work: it only ever
 * asks for as many tasks as it has free slots, so the server never overwhelms it and
 * backpressure is a property of the protocol rather than a thing to configure.
 *
 * Workers hold no durable state -- nothing survives a crash, and nothing needs to,
 * since every commit lands on the server. A crash loses at most the current step
 * (SERVER/LOCAL_SYNC) or the current local-execution batch (LOCAL_ASYNC); either way
 * the leader reclaims the lease and it re-runs. A graceful {@link #close()} does
 * better: it drains any buffered LOCAL_ASYNC steps to the server before exiting, so an
 * orderly shutdown (a rolling deploy, a scale-down) does not even pay that replay cost.
 */
public final class Worker implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Worker.class.getName());

    private final WiggleClient client;
    private final String workerId;
    private final WorkerOptions options;
    private final Registrations registrations = new Registrations();

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicInteger inFlight = new AtomicInteger();
    /** Signalled whenever a task finishes, so a saturated poll loop resumes the moment capacity frees
     *  instead of napping a fixed idle-backoff (which used to gate throughput to one wave per nap). */
    private final Object capacityFreed = new Object();
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

    public Worker(WiggleClient client, String workerId, WorkerOptions options) {
        this.client = client;
        this.workerId = workerId;
        this.options = options;
    }

    public String workerId() { return workerId; }

    /** The queues this worker actually serves: the explicit restriction, or everything registered. */
    Set<String> servedQueues() {
        return options.queues().isEmpty() ? registrations.queues() : options.queues();
    }

    public int inFlight() { return inFlight.get(); }

    WiggleClient client() { return client; }

    WorkerOptions options() { return options; }

    ScheduledExecutorService heartbeatPool() { return heartbeats; }

    Registrations registrations() { return registrations; }

    /** Whether the worker is currently running (started and not yet closed). */
    public boolean isRunning() { return running.get(); }

    /**
     * Binds a {@link ForFlow @ForFlow}-annotated object's methods as this worker's step
     * implementations. The annotation names the workflow; each method whose name matches a step
     * (case/style-insensitive, so {@code inStock} binds {@code in-stock}) is a handler, its signature
     * defining the step: one parameter is the input (decoded from JSON into that type), a
     * {@code boolean} return is a gate, {@code void} an effect, any other return type a task whose
     * value becomes the next context. A method taking one parameter per fork arm is the combine for the
     * matching {@code combine} node -- each branch's result decoded into its parameter's type, plus an
     * optional {@link Context @Context} parameter for the pre-fork context. A {@link Decode @Decode}
     * method is a custom decoder for its return type (versioning / upcasts / bespoke codecs).
     *
     * <p>Matched against the registered graph on {@link #start()}: a name collision here, or a
     * signature that clashes with the graph node's kind, fails fast; a step with no matching method is
     * simply served by no handler on this worker (logged). This includes combine nodes: there is no
     * default fold — every combine must have an explicit handler on some worker, and its return is
     * the complete post-join context.
     */
    public Worker registerHandler(Object handlerObject) {
        return registerHandler(null, handlerObject);
    }


    public Worker registerHandler(String flowName, Object handlerObject) {
        registrations.register(flowName, handlerObject);
        return this;
    }

    /**
     * Binds handlers for <em>one version</em> of the workflow. The signatures are checked against that
     * exact graph, and the worker claims only that version's tasks -- so a version can be allocated to
     * a service, and a capability handed from one service to another by publishing a new version and
     * letting the old one drain.
     *
     * <p>Without this, a worker serves every version of the workflow it binds, which is the default
     * and usually right: step names are stable across versions, so one implementation covers them all.
     * It is also the only thing that keeps an old worker from claiming a newly-published version's
     * tasks and running them with its own older code -- the activity a handler binds is
     * {@code workflow#step}, which carries no version, so the names match either way.
     *
     * <p>A worker may mix: several versions of one workflow (each with its own handler object, during
     * a migration), and other workflows entirely. If <em>any</em> registration is unversioned, the
     * worker claims every version -- the scoping is only as narrow as its least specific binding.
     */
    public Worker registerHandler(Object handlerObject, int version) {
        registrations.register(handlerObject, version);
        return this;
    }

    public Worker start() {
        if (!running.compareAndSet(false, true)) return this;
        if (!registrations.isEmpty()) registrations.reconcile(client, options);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        heartbeats = Executors.newScheduledThreadPool(heartbeatThreads(), heartbeatThreadFactory);
        pollThread = new Thread(this::pollLoop, "wiggle-worker-" + workerId);
        pollThread.setDaemon(false);
        pollThread.start();
        LOG.log(System.Logger.Level.INFO, () -> "worker " + workerId + " polling queues " + servedQueues()
                + " with concurrency " + options.concurrency());
        return this;
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                pollOnce();
            } catch (RuntimeException e) {
                if (!running.get()) return;
                LOG.log(System.Logger.Level.WARNING, "poll failed: " + e.getMessage());
                sleep(options.errorBackoff().toMillis());
            }
        }
    }

    private void pollOnce() {
        if (options.concurrency() - inFlight.get() <= 0) {
            // Saturated: wait for a task to finish (signalled below), not a fixed nap. A fixed nap
            // gated throughput to one concurrency-sized wave per idle-backoff, because fast steps all
            // finished while the poll thread was still asleep.
            awaitCapacity(options.idleBackoff().toMillis());
            return;
        }
        int free = options.concurrency() - inFlight.get();
        long polledAt = System.currentTimeMillis();
        PollResult result = client.poll(workerId, servedQueues(), registrations.claimedVersions(), free,
                options.lease().toMillis(), options.longPollWait().toMillis());
        List<TaskActivation> tasks = result.tasks();
        if (tasks.isEmpty()) {
            long shedFor = result.retryAfterMillis();
            if (shedFor > 0) {
                // The server is shedding under load (memory pressure); honour its hold-off hint.
                LOG.log(System.Logger.Level.WARNING, "poll shed by server under load; backing off " + shedFor + "ms");
                sleep(shedFor);
            } else if (System.currentTimeMillis() - polledAt < MIN_LONG_POLL_MILLIS) {
                // The server answered instantly instead of holding the long-poll open (old server, or
                // waitMillis clamped to 0): back off so an idle worker doesn't spin.
                LOG.log(System.Logger.Level.DEBUG, () -> "empty short poll; idle backoff");
                sleep(options.idleBackoff().toMillis());
            }
            // else: the server held the long-poll to its deadline and found nothing -- re-poll
            // immediately; the server-side long-poll (with wake-on-produce) IS the idle wait.
            return;
        }
        for (TaskActivation task : tasks) {
            submit(task);
        }
    }

    /** An empty poll that returns faster than this didn't long-poll server-side; back off client-side. */
    private static final long MIN_LONG_POLL_MILLIS = 5;

    /** Parks until a task slot frees (or {@code maxWaitMillis} as a safety net). */
    private void awaitCapacity(long maxWaitMillis) {
        long deadline = System.currentTimeMillis() + maxWaitMillis;
        synchronized (capacityFreed) {
            while (running.get() && options.concurrency() - inFlight.get() <= 0) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) return;
                try {
                    capacityFreed.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void submit(TaskActivation task) {
        inFlight.incrementAndGet();
        executor.submit(() -> {
            try {
                execute(task);
            } finally {
                inFlight.decrementAndGet();
                synchronized (capacityFreed) {
                    capacityFreed.notifyAll();   // resume a saturated poll loop immediately
                }
            }
        });
    }

    private void execute(TaskActivation task) {
        WorkflowDefinition def = registrations.graphFor(task.workflow() + ":" + task.version());
        // Local execution needs the graph to traverse; without it (unregistered version) fall back
        // to server-driven, one step at a time.
        if (task.executionMode() != ExecutionMode.SERVER && def != null) {
            new LocalRun(this, task, def).run();
        } else {
            new ServerRun(this, task).run();
        }
    }

    Heartbeat newHeartbeat(String taskId, String leaseOwner) {
        return new Heartbeat(heartbeats,
                extend -> client.heartbeat(taskId, leaseOwner, extend),
                options.lease().toMillis(), taskId);
    }

    /** A small pool: heartbeats are brief RPCs, so a handful of threads covers any concurrency. */
    private int heartbeatThreads() {
        return Math.max(1, Math.min(4, options.concurrency()));
    }

    static String typeName(Object result) {
        return result == null ? "null" : result.getClass().getSimpleName();
    }

    static String describe(Throwable t) {
        String msg = t.getMessage();
        return t.getClass().getSimpleName() + (msg == null ? "" : ": " + msg);
    }

    static void sleep(long millis) {
        if (millis <= 0) return;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stops polling and lets in-flight steps finish. Flips {@link #running} first, so any
     * {@link LocalRun} in progress sees it at its next between-steps check and drains instead of
     * continuing to chain (see {@link LocalRun#drainOnShutdown()}) -- a graceful shutdown loses
     * nothing already computed. The {@code executor.shutdown()} below does not interrupt the
     * current step; it only stops new submissions, so that in-flight step (and, for a local run,
     * its drain flush) gets to complete.
     */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) return;
        if (pollThread != null) pollThread.interrupt();
        awaitExecutor();
        if (heartbeats != null) heartbeats.shutdownNow();
    }

    private void awaitExecutor() {
        if (executor == null) return;
        executor.shutdown();
        try {
            // Bounded by one step plus one flush RPC now that a local run drains rather than
            // chaining to a natural boundary, so the lease duration is ample headroom.
            executor.awaitTermination(options.lease().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
