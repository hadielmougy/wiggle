package com.wiggle.server.search;

import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.ShardState;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Semantic search and the embedding model's life: a model's index is built before queries use it,
 * a replacement is built alongside, queries switch only once it is complete, and the old vectors go.
 * The indexer's upkeep is called directly, so each step is observed.
 */
class SemanticSearchTest {

    private final Storage storage = new InMemoryStorage();
    /** Recent enough that the upkeep's retention keeps every document. */
    private final long t = System.currentTimeMillis();
    private final WorkflowEngine engine;
    private final SearchIndex index;

    SemanticSearchTest() {
        storage.migrate();
        engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
        index = new SearchIndex(storage, List.of(new SearchIndex.Target(storage.home(), ShardState.ACTIVE)));
    }

    private SearchIndexer indexer(Embedder embedder) {
        return new SearchIndexer(engine, storage, index, null, 60_000, Set.of(), embedder, System::currentTimeMillis);
    }

    private Search search(Embedder... embedders) {
        return new Search(index, engine, storage, List.of(embedders));
    }

    private void docs(int n, String text) {
        List<Rows.SearchDoc> docs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            docs.add(new Rows.SearchDoc("wfi.s0.bulk" + i, "bulk", 1, "COMPLETED", null, text + " " + i, t, t));
        }
        index.index(docs);
    }

    private Map<String, String> states() {
        return storage.inHome(tx -> tx.searchModels()).stream()
                .collect(Collectors.toMap(Rows.SearchModel::model, Rows.SearchModel::state));
    }

    @Test @DisplayName("the closest meaning comes first, within the filters")
    void nearestFirst() {
        Embedder e = new HashingEmbedder(256);
        index.index(List.of(
                new Rows.SearchDoc("wfi.s0.a", "orders", 1, "RUNNING", null, "refund for a damaged parcel", t + 1, t + 1),
                new Rows.SearchDoc("wfi.s0.b", "orders", 1, "RUNNING", null, "invoice paid on time", t + 2, t + 2),
                new Rows.SearchDoc("wfi.s0.c", "billing", 1, "RUNNING", null, "refund for a damaged parcel", t + 3, t + 3)));
        indexer(e).maintain();
        Search.SemanticResult r = search(e).semantic("damaged parcel refund", "orders", null, null, null, 10, false, null);
        assertEquals("hashing-256", r.model());
        assertEquals(List.of("wfi.s0.a", "wfi.s0.b"), r.hits().stream().map(Search.Hit::instanceId).toList());
        assertTrue(r.hits().get(0).score() > r.hits().get(1).score());
        assertEquals(List.of(), search(e).semantic("refund", null, null, null, null, 10, false, Set.of()).hits(),
                "a caller who may read no workflow finds nothing");
    }

    @Test @DisplayName("before any model's index is complete, or without an embedder, a semantic search is a precondition failure")
    void preconditions() {
        assertEquals(409, assertThrows(EngineException.class,
                () -> search().semantic("x", null, null, null, null, 10, false, null)).statusCode());
        assertEquals(409, assertThrows(EngineException.class,
                () -> search(new HashingEmbedder(8)).semantic("x", null, null, null, null, 10, false, null)).statusCode(),
                "no model is READY yet");
    }

    @Test @DisplayName("a new model is built beside the old one; queries switch when it is complete; the old vectors go")
    void modelChange() {
        Embedder a = new HashingEmbedder(16), b = new HashingEmbedder(32);
        docs(1_500, "parcel");
        indexer(a).maintain();
        indexer(a).maintain();
        assertEquals(Map.of("hashing-16", "READY"), states());

        SearchIndexer next = indexer(b);
        next.maintain();
        assertEquals(Map.of("hashing-16", "READY", "hashing-32", "BUILDING"), states(),
                "one pass embeds 1280; 220 documents are still to do");
        assertEquals("hashing-16", search(b, a).semantic("parcel", null, null, null, null, 5, false, null).model(),
                "queries stay on the complete index");
        EngineException missing = assertThrows(EngineException.class,
                () -> search(b).semantic("parcel", null, null, null, null, 5, false, null));
        assertTrue(missing.getMessage().contains("hashing-16"), missing.getMessage());

        next.maintain();
        assertEquals(Map.of("hashing-16", "RETIRED", "hashing-32", "READY"), states());
        assertEquals("hashing-32", search(b).semantic("parcel", null, null, null, null, 5, false, null).model());

        for (int i = 0; i < 5; i++) next.maintain();
        assertEquals(0, storage.inTx(tx -> tx.searchVectorsOf(List.of("wfi.s0.bulk7"))).stream()
                .filter(v -> v.model().equals("hashing-16")).count(), "the retired model's vectors are deleted");
    }

    @Test @DisplayName("a document that changes is embedded again")
    void staleVectorsAreRedone() {
        Embedder e = new HashingEmbedder(64);
        index.index(List.of(new Rows.SearchDoc("wfi.s0.x", "wf", 1, "RUNNING", null, "apples", t, t)));
        indexer(e).maintain();
        index.index(List.of(new Rows.SearchDoc("wfi.s0.x", "wf", 1, "COMPLETED", null, "oranges", t, t + 1)));
        assertEquals(1, storage.inTx(tx -> tx.docsNeedingVector("hashing-64", 10)).size());
        indexer(e).maintain();
        assertEquals(0, storage.inTx(tx -> tx.docsNeedingVector("hashing-64", 10)).size());
        assertEquals("wfi.s0.x", search(e).semantic("oranges", null, null, null, null, 1, false, null)
                .hits().getFirst().instanceId());
    }
}
