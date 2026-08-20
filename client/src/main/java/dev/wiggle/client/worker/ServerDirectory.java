package dev.wiggle.client.worker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Knows which servers are live and hands them out round-robin.
 *
 * A worker heartbeats any known node ({@link WiggleClient#discover}); the node replies
 * with the whole live set, which becomes the dial list. The worker then polls one server
 * per cycle in rotation -- never all at once -- so its "ask for exactly my free slots,
 * from one place" backpressure invariant is preserved (see {@link Worker}).
 *
 * The directory owns one {@link WiggleClient} per address and closes them all on
 * {@link #close()}. It caches the seed list as a last-known-good fallback, so a transient
 * discovery failure never leaves the worker with nowhere to poll.
 */
final class ServerDirectory implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ServerDirectory.class.getName());

    /** A live server: the address to dial and the (pooled) client that dials it. */
    record Server(String address, WiggleClient client) {}

    private final List<String> seeds;
    private final String workerId;
    private final Supplier<? extends java.util.Collection<String>> queues;
    private final Duration refreshInterval;

    private final ConcurrentHashMap<String, WiggleClient> pool = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<String> active = new CopyOnWriteArrayList<>();
    private final AtomicInteger cursor = new AtomicInteger();

    private volatile boolean running;
    private Thread refreshThread;

    ServerDirectory(List<String> seeds, String workerId,
                    Supplier<? extends java.util.Collection<String>> queues, Duration refreshInterval) {
        this.seeds = List.copyOf(dedup(seeds));
        if (this.seeds.isEmpty()) throw new IllegalArgumentException("at least one seed server is required");
        this.workerId = workerId;
        this.queues = queues;
        this.refreshInterval = refreshInterval;
        this.active.addAll(this.seeds);
    }

    void start() {
        running = true;
        refresh();                       // bootstrap: learn the real member set before first poll
        refreshThread = new Thread(this::refreshLoop, "wiggle-discovery-" + workerId);
        refreshThread.setDaemon(true);
        refreshThread.start();
    }

    /** Next server in round-robin order, or null when nothing is reachable. */
    Server next() {
        List<String> snapshot = active.isEmpty() ? seeds : List.copyOf(active);
        if (snapshot.isEmpty()) return null;
        String address = snapshot.get(Math.floorMod(cursor.getAndIncrement(), snapshot.size()));
        return new Server(address, clientFor(address).client());
    }

    /** The server for a specific address (for routing a task's results back to its origin). */
    Server clientFor(String address) {
        if (address == null) return null;
        return new Server(address, pool.computeIfAbsent(address, WiggleClient::new));
    }

    /** Drop a server locally after a failed call, ahead of the next discovery refresh. */
    void onFailure(String address) {
        active.remove(address);
    }

    int size() {
        int n = active.size();
        return n > 0 ? n : seeds.size();
    }

    private void refreshLoop() {
        while (running) {
            sleep(refreshInterval.toMillis());
            if (!running) return;
            refresh();
        }
    }

    private void refresh() {
        Server via = next();
        if (via == null) return;
        try {
            List<String> discovered = dedup(via.client().discover(workerId, queues.get()));
            if (!discovered.isEmpty()) {
                // Swap in the fresh set; keep the cursor so rotation continues smoothly.
                active.clear();
                active.addAll(discovered);
            }
        } catch (RuntimeException e) {
            // Keep the current (or seed) set; a blip must not strand the worker.
            LOG.log(System.Logger.Level.DEBUG, () -> "discovery via " + via.address() + " failed: " + e.getMessage());
            onFailure(via.address());
        }
    }

    private static List<String> dedup(List<String> in) {
        Set<String> seen = new LinkedHashSet<>();
        for (String s : in) if (s != null && !s.isBlank()) seen.add(s);
        return new ArrayList<>(seen);
    }

    private static void sleep(long millis) {
        if (millis <= 0) return;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override public void close() {
        running = false;
        if (refreshThread != null) refreshThread.interrupt();
        for (WiggleClient c : pool.values()) {
            try { c.close(); } catch (RuntimeException ignored) { }
        }
        pool.clear();
    }
}
