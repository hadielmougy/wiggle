package com.wiggle.postgres;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.coord.CoordDefinition;
import com.wiggle.server.coord.CoordNamespace;
import com.wiggle.server.coord.CoordNode;
import com.wiggle.server.coord.CoordPolicy;
import com.wiggle.server.coord.NamespaceSpec;
import com.wiggle.server.coord.ProvisionState;
import com.wiggle.server.coord.StorageConfig;
import com.wiggle.server.coord.CoordPolicy.EpochRing;
import com.wiggle.server.coord.CoordPolicy.EpochStatus;
import com.wiggle.server.coord.CoordPolicy.RingSlot;
import com.wiggle.server.coord.CoordinatorStore;
import com.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 1 / T5: the {@link CoordinatorStore} contract -- CAS-guarded policy, node roster with expiry,
 * and an idempotent definition registry -- verified against both the in-memory and JDBC (H2) impls.
 */
class CoordinatorStoreTest {

    private static CoordPolicy policy(String ns, long currentEpoch, Map<Long, EpochRing> epochs) {
        return new CoordPolicy(ns, currentEpoch, 0 /* revision ignored on write */, epochs);
    }

    private static EpochRing ring(String cell, EpochStatus status) {
        return new EpochRing(List.of(new RingSlot(0, cell, "eu-west")), status);
    }

    private void scenario(CoordinatorStore store) {
        // ---- policy CAS ----
        assertTrue(store.getPolicy("acme").isEmpty());
        long r1 = store.casPolicy("acme", 0, policy("acme", 0, Map.of(0L, ring("cell-3", EpochStatus.OPEN))));
        assertEquals(1, r1, "create returns revision 1");
        assertEquals(-1, store.casPolicy("acme", 0, policy("acme", 0, Map.of(0L, ring("cell-3", EpochStatus.OPEN)))),
                "a second create loses");

        CoordPolicy got = store.getPolicy("acme").orElseThrow();
        assertEquals(1, got.revision());
        assertEquals(0, got.currentEpoch());
        assertEquals("cell-3", got.epochs().get(0L).ring().get(0).cellId());
        assertEquals(EpochStatus.OPEN, got.epochs().get(0L).status());

        // scale out: open epoch 1, drain epoch 0 -- CAS on revision 1
        Map<Long, EpochRing> two = Map.of(
                0L, ring("cell-3", EpochStatus.DRAINING),
                1L, ring("cell-5", EpochStatus.OPEN));
        assertEquals(2, store.casPolicy("acme", 1, policy("acme", 1, two)));
        assertEquals(-1, store.casPolicy("acme", 1, policy("acme", 1, two)), "a stale-revision update loses");

        CoordPolicy got2 = store.getPolicy("acme").orElseThrow();
        assertEquals(2, got2.revision());
        assertEquals(1, got2.currentEpoch());
        assertEquals(2, got2.epochs().size());
        assertEquals(EpochStatus.DRAINING, got2.epochs().get(0L).status());

        // ---- node roster ----
        store.upsertNode(new CoordNode("n1", "acme", "cellA", "grpc://h:1", "eu-west", "2.1.5", "fp-A", 2, 1_000));
        store.upsertNode(new CoordNode("n1", "acme", "cellA", "grpc://h:2", "eu-west", "2.1.5", "fp-A", 2, 2_000)); // upsert same id
        assertEquals(1, store.nodes("acme").size());
        assertEquals("grpc://h:2", store.nodes("acme").get(0).endpoint(), "upsert replaced the endpoint");
        assertEquals("cellA", store.nodes("acme").get(0).cellId(), "cell id round-trips");
        assertEquals("fp-A", store.nodes("acme").get(0).cellFingerprint(), "cell fingerprint round-trips");

        store.upsertNode(new CoordNode("n2", "acme", "cellB", "grpc://h:3", "eu-west", "2.1.5", "fp-A", 2, 500));
        assertEquals(2, store.nodes("acme").size());
        assertEquals(1, store.expireNodes(900), "n2 (hb 500) is stale");
        assertEquals(1, store.nodes("acme").size());

        // ---- definitions (idempotent) ----
        assertTrue(store.getDefinition("acme", "order").isEmpty());
        store.putDefinition(new CoordDefinition("acme", "order", 42, "hashA", 111));
        store.putDefinition(new CoordDefinition("acme", "order", 42, "hashA", 111)); // idempotent
        assertEquals(1, store.definitions("acme").size());
        assertEquals("hashA", store.getDefinition("acme", "order").orElseThrow().hash());

        store.putDefinition(new CoordDefinition("acme", "order", 43, "hashB", 222)); // update in place
        assertEquals(43, store.getDefinition("acme", "order").orElseThrow().version());
        assertEquals(1, store.definitions("acme").size());

        // ---- namespace registry (provisioning, T13) ----
        assertTrue(store.getNamespace("acme").isEmpty());
        StorageConfig sc = StorageConfig.jdbc("jdbc:postgresql://db/acme", "app", "ACME_DB_SECRET", 8);
        store.putNamespace(CoordNamespace.requested(new NamespaceSpec("acme", sc, 2, "eu-west", 8100), 1_000));
        CoordNamespace ns = store.getNamespace("acme").orElseThrow();
        assertEquals(ProvisionState.REQUESTED, ns.state());
        assertEquals("ACME_DB_SECRET", ns.storage().secretRef(), "the ref is stored, never the secret");
        assertEquals(2, ns.replicas());

        store.putNamespace(ns.active("grpc://acme:8100", 2_000));   // upsert in place
        CoordNamespace active = store.getNamespace("acme").orElseThrow();
        assertEquals(ProvisionState.ACTIVE, active.state());
        assertEquals("grpc://acme:8100", active.endpoint());
        assertEquals(1, store.namespaces().size());
    }

    /** The atomic cell-identity binding: claim / replica / conflict, and orphan pruning. */
    private void cellBindingScenario(CoordinatorStore store) {
        assertTrue(store.bindCell("orders", "cellA", "fp-A"), "first cell claims the id");
        assertTrue(store.bindCell("orders", "cellA", "fp-A"), "a replica of the same cell matches");
        assertFalse(store.bindCell("orders", "cellA", "fp-B"), "a different cell may not reuse the id");
        assertTrue(store.bindCell("orders", "cellB", "fp-B"), "a different cell id is free");
        assertTrue(store.bindCell("other", "cellA", "fp-B"), "the same id in another namespace is independent");
        assertTrue(store.bindCell("orders", "cellA", null), "a null fingerprint skips the guard");

        // No node references any binding yet -> all three are orphans and prune reclaims them.
        assertEquals(3, store.pruneOrphanCellBindings(), "orphan bindings pruned");
        assertTrue(store.bindCell("orders", "cellA", "fp-C"), "a pruned id is reusable by a new cell");

        // A live node keeps its cell's binding.
        store.upsertNode(new CoordNode("bn1", "orders", "cellA", "grpc://h:1", "eu", "v", "fp-C", 0, 1_000));
        assertEquals(0, store.pruneOrphanCellBindings(), "a binding with a live node is kept");
    }

    @Test @DisplayName("in-memory store honours the CoordinatorStore contract")
    void inMemory() {
        try (CoordinatorStore store = new InMemoryCoordinatorStore()) {
            scenario(store);
        }
        try (CoordinatorStore store = new InMemoryCoordinatorStore()) {
            cellBindingScenario(store);
        }
    }

    @Test @DisplayName("JDBC (H2) store honours the CoordinatorStore contract")
    void jdbcH2() throws Exception {
        String url = "jdbc:h2:mem:coordstore-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (JdbcStorage storage = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            scenario(storage.coordinatorStore());   // migrates the coord_* schema, then a store over the pool
            cellBindingScenario(storage.coordinatorStore());
        }
    }

    @Test @DisplayName("bindCell is atomic under concurrent claims: exactly one fingerprint's cohort wins")
    void bindCellConcurrent() throws Exception {
        try (CoordinatorStore store = new InMemoryCoordinatorStore()) {
            int threads = 32;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch go = new CountDownLatch(1);
            List<Future<Boolean>> fs = new ArrayList<>();
            for (int i = 0; i < threads; i++) {
                String fp = (i % 2 == 0) ? "fp-A" : "fp-B";   // 16 each
                fs.add(pool.submit(() -> { go.await(); return store.bindCell("ns", "cell", fp); }));
            }
            go.countDown();
            int wins = 0;
            for (Future<Boolean> f : fs) if (f.get()) wins++;
            pool.shutdownNow();
            assertEquals(16, wins, "exactly the winning fingerprint's 16 threads succeeded; the other 16 were rejected");
        }
    }
}
