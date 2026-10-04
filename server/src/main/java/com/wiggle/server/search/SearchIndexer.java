package com.wiggle.server.search;

import com.wiggle.core.EventView;
import com.wiggle.server.cluster.ClusterManager;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Freshness;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Feeds the search shards from the event log, on the leader only. It is a named consumer of the log
 * ({@value #CONSUMER}): for each instance an event names it reads the instance from its shard,
 * builds the document and writes it to its search shard, then acknowledges the batch. Delivery is
 * at-least-once and a write never replaces a newer document, so a batch repeated after a failure
 * or a change of leader is harmless. A search shard that fails leaves the batch unacknowledged, to
 * be retried; the engine never waits on any of this.
 *
 * <p>Once a minute it also deletes documents past their retention and takes a step of the
 * rebalance.
 */
public final class SearchIndexer implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(SearchIndexer.class.getName());
    public static final String CONSUMER = "wiggle.search";
    static final int BATCH = 500;
    /** The most text one document carries; a longer context is cut, so one instance cannot bloat a shard. */
    static final int MAX_TEXT = 64 * 1024;

    private final WorkflowEngine engine;
    private final Storage storage;
    private final SearchIndex index;
    private final ClusterManager cluster;
    private final long retentionMillis;
    private final Set<String> workflows;
    private final LongSupplier clock;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "wiggle-search-indexer");
        t.setDaemon(true);
        return t;
    });

    /** @param workflows the workflows indexed; empty indexes all */
    public SearchIndexer(WorkflowEngine engine, Storage storage, SearchIndex index, ClusterManager cluster,
                         long retentionMillis, Set<String> workflows, LongSupplier clock) {
        this.engine = engine;
        this.storage = storage;
        this.index = index;
        this.cluster = cluster;
        this.retentionMillis = retentionMillis;
        this.workflows = Set.copyOf(workflows);
        this.clock = clock;
    }

    public SearchIndexer start(long intervalMillis) {
        scheduler.scheduleWithFixedDelay(() -> quietly(this::drain, "indexing"), intervalMillis, intervalMillis,
                TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(() -> quietly(this::maintain, "search upkeep"), 60_000, 60_000,
                TimeUnit.MILLISECONDS);
        return this;
    }

    private void quietly(Runnable work, String what) {
        if (!cluster.isLeader()) return;
        try {
            work.run();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, () -> what + " failed; retrying on the next tick: " + e);
        }
    }

    /** Indexes everything the log holds for this consumer, a batch at a time; returns how many documents it wrote. */
    public int drain() {
        int written = 0;
        while (true) {
            List<EventView> events = engine.pollEvents(CONSUMER, BATCH, -1, clock.getAsLong(), () -> false);
            if (events.isEmpty()) return written;
            Map<String, Long> latest = new LinkedHashMap<>();
            for (EventView e : events) latest.merge(e.instanceId(), e.createdAt(), Math::max);
            List<Rows.SearchDoc> docs = new ArrayList<>();
            latest.forEach((id, at) -> {
                Rows.Instance inst = read(id, at);
                if (inst != null && (workflows.isEmpty() || workflows.contains(inst.workflow))) docs.add(doc(inst));
            });
            index.index(docs);
            engine.ackEvents(CONSUMER, events.getLast().cursor());
            written += docs.size();
            if (events.size() < BATCH) return written;
        }
    }

    /**
     * The instance as of at least {@code eventAt}: from a replica when it has caught up that far, else
     * from the primary. Null when it is gone (purged); its document, if any, stays until its own retention.
     */
    private Rows.Instance read(String id, long eventAt) {
        Rows.Instance inst = storage.readFor(id, Freshness.REPLICA_OK, tx -> tx.findInstance(id)).orElse(null);
        if (inst != null && inst.updatedAt >= eventAt) return inst;
        return storage.readFor(id, Freshness.PRIMARY, tx -> tx.findInstance(id)).orElse(null);
    }

    /** The document for {@code inst}: its identity, and as text its correlation id, context, reason and error. */
    static Rows.SearchDoc doc(Rows.Instance inst) {
        StringBuilder text = new StringBuilder();
        if (inst.correlationId != null) text.append(inst.correlationId).append('\n');
        if (inst.context != null) text.append(inst.context.json()).append('\n');
        if (inst.terminationReason != null) text.append(inst.terminationReason).append('\n');
        if (inst.error != null) text.append(inst.error);
        String t = text.length() > MAX_TEXT ? text.substring(0, MAX_TEXT) : text.toString();
        return new Rows.SearchDoc(inst.id, inst.workflow, inst.version, inst.status.name(), inst.correlationId, t,
                inst.createdAt, inst.updatedAt);
    }

    /** Deletes documents past retention and moves a batch of misplaced ones. */
    public void maintain() {
        int removed = index.retain(clock.getAsLong() - retentionMillis, BATCH);
        int moved = index.rebalance(BATCH);
        if (removed > 0 || moved > 0) {
            LOG.log(System.Logger.Level.INFO, () -> "search upkeep: " + removed + " document(s) past retention, "
                    + moved + " moved to their shard");
        }
    }

    @Override public void close() {
        scheduler.shutdownNow();
    }
}
