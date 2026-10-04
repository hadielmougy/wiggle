package com.wiggle.server.search;

import com.wiggle.server.store.Freshness;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.SearchText;
import com.wiggle.server.store.ShardState;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * The search documents, spread over the search shards. A document lives on the shard that wins the
 * rendezvous hash of its instance id among the {@code ACTIVE} search shards; a {@code DRAINING}
 * shard is still queried, and {@link #rebalance} moves documents to their winner after the set of
 * search shards changes. Until it has, a query keeps the newest copy of each instance.
 */
public final class SearchIndex {

    private static final System.Logger LOG = System.getLogger(SearchIndex.class.getName());

    /** A search shard and its state. */
    public record Target(int shard, ShardState state) { }

    /** What a query found, best first, and whether a shard could not answer. */
    public record Result(List<Rows.SearchHit> hits, boolean partial) { }

    private final Storage storage;
    private final List<Integer> active;
    private final List<Integer> queried;
    /** Where each shard's rebalance pass has got to, by instance id. */
    private final Map<Integer, String> rebalanceFrom = new ConcurrentHashMap<>();

    public SearchIndex(Storage storage, List<Target> shards) {
        this.storage = storage;
        this.active = shards.stream().filter(t -> t.state() == ShardState.ACTIVE).map(Target::shard).toList();
        this.queried = shards.stream().filter(t -> t.state() != ShardState.RETIRED).map(Target::shard).toList();
        if (active.isEmpty()) throw new IllegalArgumentException("search needs at least one ACTIVE search shard");
    }

    /** The search shards a query reads. */
    public List<Integer> shards() {
        return queried;
    }

    /** The shard {@code instanceId}'s document belongs on: the highest rendezvous score among the ACTIVE shards. */
    public int winner(String instanceId) {
        int best = active.getFirst();
        long bestScore = Long.MIN_VALUE;
        for (int shard : active) {
            long score = score(instanceId, shard);
            if (score > bestScore || (score == bestScore && shard < best)) {
                best = shard;
                bestScore = score;
            }
        }
        return best;
    }

    static long score(String instanceId, int shard) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256")
                    .digest((instanceId + "#" + shard).getBytes(StandardCharsets.UTF_8));
            long v = 0;
            for (int i = 0; i < 8; i++) v = (v << 8) | (d[i] & 0xff);
            return v;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable on this JVM", e);
        }
    }

    /** Writes each document to its winner, one transaction per shard; an older copy never replaces a newer one. */
    public void index(List<Rows.SearchDoc> docs) {
        Map<Integer, List<Rows.SearchDoc>> byShard = new LinkedHashMap<>();
        for (Rows.SearchDoc d : docs) byShard.computeIfAbsent(winner(d.instanceId()), k -> new ArrayList<>()).add(d);
        byShard.forEach((shard, batch) -> storage.inShard(shard, tx -> {
            batch.forEach(tx::upsertSearchDoc);
            return null;
        }));
    }

    /**
     * Runs {@code query} on every search shard at once, replicas allowed, and keeps the global best.
     * A shard that fails makes the result partial when {@code partialOk}; otherwise the search fails.
     */
    public Result search(Rows.SearchQuery query, boolean partialOk) {
        List<Future<List<Rows.SearchHit>>> answers = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int shard : queried) {
                answers.add(pool.submit(() -> storage.readShard(shard, Freshness.REPLICA_OK, tx -> tx.searchDocs(query))));
            }
            Map<String, Rows.SearchHit> newest = new LinkedHashMap<>();
            boolean partial = false;
            for (int i = 0; i < answers.size(); i++) {
                List<Rows.SearchHit> hits;
                try {
                    hits = answers.get(i).get();
                } catch (ExecutionException e) {
                    int shard = queried.get(i);
                    if (!partialOk) {
                        throw new StorageException("search shard " + shard + " did not answer", e.getCause(),
                                StorageException.Classification.TRANSIENT);
                    }
                    LOG.log(System.Logger.Level.WARNING, () -> "search shard " + shard + " did not answer; the result is partial: "
                            + e.getCause());
                    partial = true;
                    continue;
                }
                for (Rows.SearchHit h : hits) {
                    newest.merge(h.doc().instanceId(), h, (a, b) -> a.doc().updatedAt() >= b.doc().updatedAt() ? a : b);
                }
            }
            List<Rows.SearchHit> merged = new ArrayList<>(newest.values());
            merged.sort(SearchText.BEST_FIRST);
            return new Result(merged.size() > query.limit() ? List.copyOf(merged.subList(0, query.limit())) : merged,
                    partial);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException("search interrupted", e, StorageException.Classification.TRANSIENT);
        }
    }

    /** Deletes up to {@code max} documents per shard whose instance last changed before {@code updatedBefore}. */
    public int retain(long updatedBefore, int max) {
        int removed = 0;
        for (int shard : queried) removed += storage.inShard(shard, tx -> tx.deleteSearchDocsBefore(updatedBefore, max));
        return removed;
    }

    /**
     * One step of the rebalance: reads up to {@code max} documents per shard from where its pass got
     * to, and moves each whose winner is another shard there. A shard's pass starts over once it has
     * read to the end. Returns how many documents moved.
     */
    public int rebalance(int max) {
        int moved = 0;
        for (int shard : queried) {
            String from = rebalanceFrom.getOrDefault(shard, "");
            List<Rows.SearchDoc> docs = storage.inShard(shard, tx -> tx.searchDocsAfter(from, max));
            rebalanceFrom.put(shard, docs.size() < max ? "" : docs.getLast().instanceId());
            List<Rows.SearchDoc> misplaced = docs.stream().filter(d -> winner(d.instanceId()) != shard).toList();
            if (misplaced.isEmpty()) continue;
            index(misplaced);
            storage.inShard(shard, tx -> {
                misplaced.forEach(d -> tx.deleteSearchDoc(d.instanceId()));
                return null;
            });
            moved += misplaced.size();
        }
        return moved;
    }
}
