package dev.wiggle.postgres;

import dev.wiggle.jdbc.JdbcCoordinatorStore;
import dev.wiggle.jdbc.JdbcStorage;
import dev.wiggle.server.ServerRole;
import dev.wiggle.server.coord.CoordDefinition;
import dev.wiggle.server.coord.CoordNode;
import dev.wiggle.server.coord.CoordPolicy;
import dev.wiggle.server.coord.CoordPolicy.EpochRing;
import dev.wiggle.server.coord.CoordPolicy.EpochStatus;
import dev.wiggle.server.coord.CoordPolicy.RingSlot;
import dev.wiggle.server.coord.CoordinatorStore;
import dev.wiggle.server.coord.InMemoryCoordinatorStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        store.upsertNode(new CoordNode("n1", "acme", "cellA", "grpc://h:1", "eu-west", "2.1.5", 2, 1_000));
        store.upsertNode(new CoordNode("n1", "acme", "cellA", "grpc://h:2", "eu-west", "2.1.5", 2, 2_000)); // upsert same id
        assertEquals(1, store.nodes("acme").size());
        assertEquals("grpc://h:2", store.nodes("acme").get(0).endpoint(), "upsert replaced the endpoint");
        assertEquals("cellA", store.nodes("acme").get(0).cellId(), "cell id round-trips");

        store.upsertNode(new CoordNode("n2", "acme", "cellB", "grpc://h:3", "eu-west", "2.1.5", 2, 500));
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
    }

    @Test @DisplayName("in-memory store honours the CoordinatorStore contract")
    void inMemory() {
        try (CoordinatorStore store = new InMemoryCoordinatorStore()) {
            scenario(store);
        }
    }

    @Test @DisplayName("JDBC (H2) store honours the CoordinatorStore contract")
    void jdbcH2() throws Exception {
        String url = "jdbc:h2:mem:coordstore-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (JdbcStorage mig = new JdbcStorage(url, "sa", "", 2, new H2Dialect())) {
            mig.migrate(ServerRole.COORDINATOR);   // create the coord_* tables
            try (CoordinatorStore store = new JdbcCoordinatorStore(url, "sa", "", 2, new H2Dialect())) {
                scenario(store);
            }
        }
    }
}
