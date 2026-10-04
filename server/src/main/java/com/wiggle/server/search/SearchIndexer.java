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
 * <p>With an {@link Embedder}, it then embeds the documents it wrote and writes their vectors; if
 * that fails, the documents are already searchable by text, and the backfill embeds them later.
 *
 * <p>Once a minute it also deletes documents past their retention, takes a step of the rebalance,
 * backfills vectors, and moves the embedding model through the registry on the home shard: a new
 * model is {@code BUILDING} until every document indexed before it started has a vector, then
 * {@code READY}, retiring the one it replaces, whose vectors are then deleted.
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
    private final Embedder embedder;
    private final LongSupplier clock;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "wiggle-search-indexer");
        t.setDaemon(true);
        return t;
    });

    /**
     * @param workflows the workflows indexed; empty indexes all
     * @param embedder  the model new vectors are made with; null indexes text only
     */
    public SearchIndexer(WorkflowEngine engine, Storage storage, SearchIndex index, ClusterManager cluster,
                         long retentionMillis, Set<String> workflows, Embedder embedder, LongSupplier clock) {
        this.embedder = embedder;
        this.engine = engine;
        this.storage = storage;
        this.index = index;
        this.cluster = cluster;
        this.retentionMillis = retentionMillis;
        this.workflows = Set.copyOf(workflows);
        this.clock = clock;
    }

    /** Drains the log every {@code intervalMillis}, and runs the upkeep every {@code upkeepMillis}, first soon after start. */
    public SearchIndexer start(long intervalMillis, long upkeepMillis) {
        scheduler.scheduleWithFixedDelay(() -> quietly(this::drain, "indexing"), intervalMillis, intervalMillis,
                TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(() -> quietly(this::maintain, "search upkeep"),
                Math.min(upkeepMillis, 5_000), upkeepMillis, TimeUnit.MILLISECONDS);
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
            if (embedder != null && !docs.isEmpty()) {
                try {
                    index.indexVectors(SearchIndex.embed(embedder, docs));
                } catch (RuntimeException e) {
                    LOG.log(System.Logger.Level.WARNING, () -> "embedding " + docs.size() + " document(s) failed; "
                            + "they are searchable by text, and the backfill will embed them: " + e.getMessage());
                }
            }
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

    /** Deletes documents past retention, moves a batch of misplaced ones, and keeps the vectors current. */
    public void maintain() {
        int removed = index.retain(clock.getAsLong() - retentionMillis, BATCH);
        int moved = index.rebalance(BATCH);
        if (removed > 0 || moved > 0) {
            LOG.log(System.Logger.Level.INFO, () -> "search upkeep: " + removed + " document(s) past retention, "
                    + moved + " moved to their shard");
        }
        if (embedder != null) maintainVectors();
    }

    /** Vectors embedded per backfill pass; a batch is one call to the embedder. */
    static final int EMBED_BATCH = 64;
    private static final int EMBED_BATCHES_PER_PASS = 20;

    private void maintainVectors() {
        long now = clock.getAsLong();
        Map<String, Rows.SearchModel> models = new LinkedHashMap<>();
        storage.inHome(tx -> tx.searchModels()).forEach(m -> models.put(m.model(), m));
        Rows.SearchModel current = models.get(embedder.model());
        if (current == null || current.state().equals(Rows.SearchModel.RETIRED)) {
            index.prepare(embedder.model(), embedder.dimension());
            current = new Rows.SearchModel(embedder.model(), embedder.dimension(), Rows.SearchModel.BUILDING, now, null);
            Rows.SearchModel starting = current;
            storage.inHome(tx -> { tx.putSearchModel(starting); return null; });
            LOG.log(System.Logger.Level.INFO, () -> "building the vector index for model " + embedder.model());
        }
        int embedded = 0;
        for (int i = 0; i < EMBED_BATCHES_PER_PASS; i++) {
            int n = index.vectorize(embedder, EMBED_BATCH);
            embedded += n;
            if (n == 0) break;
        }
        if (current.state().equals(Rows.SearchModel.BUILDING) && index.pending(embedder.model(), current.startedAt()) == 0) {
            Rows.SearchModel ready = new Rows.SearchModel(current.model(), current.dimension(), Rows.SearchModel.READY,
                    current.startedAt(), now);
            storage.inHome(tx -> {
                tx.putSearchModel(ready);
                for (Rows.SearchModel m : tx.searchModels()) {
                    if (!m.model().equals(ready.model()) && m.state().equals(Rows.SearchModel.READY)) {
                        tx.putSearchModel(new Rows.SearchModel(m.model(), m.dimension(), Rows.SearchModel.RETIRED,
                                m.startedAt(), m.readyAt()));
                    }
                }
                return null;
            });
            LOG.log(System.Logger.Level.INFO, () -> "vector index for model " + ready.model() + " is complete; "
                    + "semantic queries use it from now on");
        }
        for (Rows.SearchModel m : models.values()) {
            if (m.state().equals(Rows.SearchModel.RETIRED) && !m.model().equals(embedder.model())) {
                index.dropVectors(m.model(), BATCH);
            }
        }
        int e = embedded;
        if (e > 0) LOG.log(System.Logger.Level.DEBUG, () -> "vector backfill embedded " + e + " document(s)");
    }

    @Override public void close() {
        scheduler.shutdownNow();
    }
}
