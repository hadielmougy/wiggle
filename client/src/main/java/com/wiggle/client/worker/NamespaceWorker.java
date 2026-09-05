package com.wiggle.client.worker;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A coordinator-aware worker: it serves a whole namespace by running one {@link Worker} per <em>active
 * cell</em> and reconciling that set over time. Cells of a namespace have disjoint databases, so a
 * single worker polling one cell would only ever see that cell's work; this fans polling out across
 * every cell the coordinator reports active (OPEN or DRAINING) and drops a cell's worker when it
 * retires (T12). For a standalone (non-sharded) server, use a plain {@link Worker} against
 * {@code WiggleConnection.direct(url).client()} instead.
 *
 * <p>Node-level HA <em>within</em> a cell is a separate concern: a cell is one logical endpoint (its
 * members share a database, so polling one is enough); this reconciles at the <em>cell</em> level.
 *
 * <p>Handlers are declared once via a {@code configurator} applied to each per-cell worker, so the same
 * blueprints/handlers run on every cell.
 */
public final class NamespaceWorker implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(NamespaceWorker.class.getName());

    private record Cell(Worker worker, WiggleClient client) {}

    private final Supplier<List<String>> cellSource;
    private final Function<String, WiggleClient> clientFactory;
    private final String workerId;
    private final WorkerOptions options;
    private final Consumer<Worker> configurator;

    private final Map<String, Cell> cells = new ConcurrentHashMap<>();   // target -> running worker
    private final Object lock = new Object();
    private final AtomicBoolean running = new AtomicBoolean();
    private volatile Duration reconcileInterval = Duration.ofSeconds(10);
    private ScheduledExecutorService scheduler;

    /**
     * The general form: {@code cellSource} supplies the current set of cell targets, {@code clientFactory}
     * builds a client for one (e.g. to add TLS), and {@code configurator} registers the handlers on each
     * per-cell worker. {@code workerId} is the prefix; each cell's worker gets a distinct id.
     */
    public NamespaceWorker(Supplier<List<String>> cellSource, Function<String, WiggleClient> clientFactory,
                           String workerId, WorkerOptions options, Consumer<Worker> configurator) {
        this.cellSource = cellSource;
        this.clientFactory = clientFactory;
        this.workerId = workerId;
        this.options = options == null ? WorkerOptions.defaults() : options;
        this.configurator = configurator;
    }

    /** Coordinator-wired: serve {@code namespace}'s active cells, resolved through {@code connection}. */
    public NamespaceWorker(CoordinatedConnection connection, String namespace, String workerId, Consumer<Worker> configurator) {
        this(() -> connection.activeCellTargets(namespace), WiggleClient::new,
                workerId, WorkerOptions.defaults(), configurator);
    }

    /** How often to re-resolve the active-cell set (default 10s). Set before {@link #start()}. */
    public NamespaceWorker reconcileEvery(Duration interval) {
        if (interval != null && !interval.isZero() && !interval.isNegative()) this.reconcileInterval = interval;
        return this;
    }

    /** Starts serving: one worker per currently-active cell, then a periodic reconcile. */
    public NamespaceWorker start() {
        if (!running.compareAndSet(false, true)) return this;
        reconcile();   // stand the initial workers up before returning
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wiggle-namespace-worker");
            t.setDaemon(true);
            return t;
        });
        long ms = Math.max(500, reconcileInterval.toMillis());
        scheduler.scheduleAtFixedRate(this::reconcile, ms, ms, TimeUnit.MILLISECONDS);
        return this;
    }

    /** The cell targets currently being served (for observability/tests). */
    public Set<String> activeCells() {
        return Set.copyOf(cells.keySet());
    }

    public boolean isRunning() {
        return running.get();
    }

    /**
     * One reconcile pass: start a worker for each newly-active cell, stop the worker of any cell that is
     * no longer active. Package-visible so tests can drive it deterministically. A transient resolver
     * failure is logged and skipped -- existing workers keep serving.
     */
    void reconcile() {
        List<String> targets;
        try {
            targets = cellSource.get();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.DEBUG, "namespace worker: cell resolution failed, keeping current set: " + e);
            return;
        }
        Set<String> desired = Set.copyOf(targets);
        synchronized (lock) {
            if (!running.get()) return;
            for (String target : desired) {
                if (cells.containsKey(target)) continue;
                try {
                    cells.put(target, startCell(target));
                } catch (RuntimeException e) {   // one bad cell must not stop the others; retry next pass
                    LOG.log(System.Logger.Level.WARNING, "namespace worker: cell " + target + " not ready, will retry: " + e);
                }
            }
            cells.keySet().removeIf(target -> {
                if (desired.contains(target)) return false;
                stopCell(target, cells.get(target));
                return true;
            });
        }
    }

    private Cell startCell(String target) {
        WiggleClient client = clientFactory.apply(target);
        try {
            Worker worker = new Worker(client, workerId + "@" + target, options);
            configurator.accept(worker);
            worker.start();
            LOG.log(System.Logger.Level.INFO, () -> "namespace worker: serving cell " + target);
            return new Cell(worker, client);
        } catch (RuntimeException e) {
            client.close();   // failed to start (e.g. cell not seeded yet) -> retry next reconcile
            throw e;
        }
    }

    private void stopCell(String target, Cell cell) {
        if (cell == null) return;
        LOG.log(System.Logger.Level.INFO, () -> "namespace worker: dropping retired cell " + target);
        try { cell.worker().close(); } finally { cell.client().close(); }
    }

    @Override public void close() {
        running.set(false);
        if (scheduler != null) scheduler.shutdownNow();
        synchronized (lock) {
            cells.forEach(this::stopCell);
            cells.clear();
        }
    }
}
