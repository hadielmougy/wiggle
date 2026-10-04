package com.wiggle.server.search;

import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.ShardState;
import com.wiggle.server.store.ShardedStorage;
import com.wiggle.server.store.Storage;
import com.wiggle.server.store.StorageException;
import com.wiggle.server.store.Tx;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Where search documents live, how a query reads them back, and how they move when the search shards change. */
class SearchIndexTest {

    /** An in-memory shard that can be taken down. */
    private static final class Shard implements Storage {
        final InMemoryStorage mem = new InMemoryStorage();
        volatile boolean down;

        @Override public void migrate() { }
        @Override public <R> R inTx(Function<Tx, R> work) {
            if (down) throw new StorageException("search shard down", null, StorageException.Classification.TRANSIENT);
            return mem.inTx(work);
        }
        @Override public void close() { }

        int docs() {
            return mem.inTx(tx -> tx.searchDocsAfter("", 10_000)).size();
        }
    }

    /** Home (0) holds no documents; search shards 1, 2 and 3. */
    private final Map<Integer, Shard> shards = new HashMap<>();

    private ShardedStorage storage() {
        List<ShardedStorage.Member> members = new ArrayList<>();
        members.add(new ShardedStorage.Member(0, ShardState.ACTIVE, true, new InMemoryStorage()));
        for (int i = 1; i <= 3; i++) {
            Shard s = shards.computeIfAbsent(i, k -> new Shard());
            members.add(new ShardedStorage.Member(i, ShardState.ACTIVE, false, s));
        }
        return new ShardedStorage(members, 0);
    }

    private static Rows.SearchDoc doc(String id, String text, long updatedAt) {
        return new Rows.SearchDoc(id, "wf", 1, "RUNNING", null, text, updatedAt, updatedAt);
    }

    private static List<SearchIndex.Target> active(int... ids) {
        List<SearchIndex.Target> out = new ArrayList<>();
        for (int id : ids) out.add(new SearchIndex.Target(id, ShardState.ACTIVE));
        return out;
    }

    private static Rows.SearchQuery query(String text) {
        return new Rows.SearchQuery(text, null, null, null, null, 1000);
    }

    @Test @DisplayName("a document's shard is a stable rendezvous choice, and spreads documents over the shards")
    void placement() {
        SearchIndex index = new SearchIndex(storage(), active(1, 2, 3));
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < 3000; i++) counts.merge(index.winner("wfi.s0.doc" + i), 1, Integer::sum);
        assertEquals(3, counts.size());
        counts.values().forEach(n -> assertTrue(n > 800 && n < 1200, counts.toString()));
        assertEquals(index.winner("wfi.s0.x"), new SearchIndex(storage(), active(3, 1, 2)).winner("wfi.s0.x"),
                "the order shards are listed in does not matter");

        SearchIndex twoOnly = new SearchIndex(storage(), active(1, 2));
        int moved = 0;
        for (int i = 0; i < 3000; i++) {
            String id = "wfi.s0.doc" + i;
            if (index.winner(id) != 3) {
                assertEquals(index.winner(id), twoOnly.winner(id), "a document not on the removed shard stays put");
            } else {
                moved++;
            }
        }
        assertEquals(counts.get(3), moved, "only the removed shard's documents move");
    }

    @Test @DisplayName("a query reads every search shard and keeps the global best, the newest copy of each instance")
    void searchMerges() {
        ShardedStorage storage = storage();
        SearchIndex index = new SearchIndex(storage, active(1, 2, 3));
        List<Rows.SearchDoc> docs = new ArrayList<>();
        for (int i = 0; i < 30; i++) docs.add(doc("wfi.s0.d" + i, "apple" + (i % 3 == 0 ? " apple" : ""), 100 + i));
        index.index(docs);
        assertTrue(shards.values().stream().allMatch(s -> s.docs() > 0), "documents are spread");
        assertEquals(30, shards.values().stream().mapToInt(Shard::docs).sum());

        SearchIndex.Result r = index.search(new Rows.SearchQuery("apple", null, null, null, null, 5), false);
        assertEquals(5, r.hits().size());
        assertTrue(r.hits().stream().allMatch(h -> h.score() == 2), "the best five are the double matches");
        assertFalse(r.partial());

        // A stale copy on a shard that no longer owns the instance: the newer one wins.
        String id = "wfi.s0.d1";
        int stray = index.winner(id) == 1 ? 2 : 1;
        storage.inShard(stray, tx -> tx.upsertSearchDoc(doc(id, "apple", 1)));
        List<Rows.SearchHit> all = index.search(query("apple"), false).hits();
        assertEquals(30, all.size(), "one hit per instance");
        assertEquals(101, all.stream().filter(h -> h.doc().instanceId().equals(id)).findFirst().orElseThrow().doc().updatedAt());
    }

    @Test @DisplayName("a search shard that does not answer fails the search, or makes it partial when asked")
    void unreachableShard() {
        SearchIndex index = new SearchIndex(storage(), active(1, 2, 3));
        List<Rows.SearchDoc> docs = new ArrayList<>();
        for (int i = 0; i < 30; i++) docs.add(doc("wfi.s0.d" + i, "pear", 100));
        index.index(docs);
        shards.get(2).down = true;
        assertThrows(StorageException.class, () -> index.search(query("pear"), false));
        SearchIndex.Result r = index.search(query("pear"), true);
        assertTrue(r.partial());
        assertEquals(30 - shards.get(2).mem.inTx(tx -> tx.searchDocsAfter("", 1000)).size(), r.hits().size());
    }

    @Test @DisplayName("after a search shard is drained, the rebalance moves its documents to their new shard")
    void rebalance() {
        ShardedStorage storage = storage();
        new SearchIndex(storage, active(1, 2, 3)).index(java.util.stream.IntStream.range(0, 300)
                .mapToObj(i -> doc("wfi.s0.r" + i, "plum", 100)).toList());
        int onThree = shards.get(3).docs();
        assertTrue(onThree > 0);

        SearchIndex draining = new SearchIndex(storage, List.of(new SearchIndex.Target(1, ShardState.ACTIVE),
                new SearchIndex.Target(2, ShardState.ACTIVE), new SearchIndex.Target(3, ShardState.DRAINING)));
        assertEquals(300, draining.search(query("plum"), false).hits().size(), "a draining shard is still queried");
        int moved = 0;
        for (int i = 0; i < 10; i++) moved += draining.rebalance(50);
        assertEquals(onThree, moved);
        assertEquals(0, shards.get(3).docs());
        assertEquals(300, draining.search(query("plum"), false).hits().size(), "nothing is lost or doubled");
        assertEquals(0, draining.rebalance(50), "a second pass finds nothing to move");
    }

    @Test @DisplayName("retention deletes documents by when their instance last changed")
    void retention() {
        SearchIndex index = new SearchIndex(storage(), active(1, 2, 3));
        index.index(List.of(doc("wfi.s0.old", "fig", 10), doc("wfi.s0.new", "fig", 1_000)));
        assertEquals(1, index.retain(500, 100));
        assertEquals(List.of("wfi.s0.new"), index.search(query("fig"), false).hits().stream()
                .map(h -> h.doc().instanceId()).toList());
    }
}
