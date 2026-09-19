package com.wiggle.postgres;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.EngineException;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The registration paths on real PostgreSQL. The rest of the suite runs them on H2, which is a
 * faithful stand-in right up until it is not: an earlier draft of this work used a composite-target
 * {@code ON CONFLICT ... DO UPDATE} that H2 rejects outright, and only a live database would have
 * said so. These are the statements H2 has been the sole witness to — the fingerprint read under
 * {@code FOR UPDATE}, the replacing {@code UPDATE}, the graph-row {@code DELETE}, and
 * {@code MAX(version)} — plus the migrations that added the columns they touch.
 *
 * <p>Opt-in exactly like {@link PostgresClaimTest}: set {@code WIGGLE_TEST_PG_URL}.
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class PostgresRegistrationTest {

    interface TwoSteps {
        Map<String, Object> first(Map<String, Object> ctx);
        Map<String, Object> second(Map<String, Object> ctx);
    }

    private static JdbcStorage storage() {
        JdbcStorage storage = new JdbcStorage(TestDb.url("PG"),
                TestDb.user("PG"), TestDb.password("PG"), 4, new PostgresDialect());
        storage.migrate();
        return storage;
    }

    /** One step, or two: two topologies that differ, at whatever version the caller declares. */
    private static WorkflowDefinition spec(String name, int version, boolean twoSteps) {
        return FlowSpec.define(name, version, Map.class, TwoSteps.class, (f, s) ->
                twoSteps ? f.thenApply(s::first).thenApply(s::second) : f.thenApply(s::first))
                .definition();
    }

    private static String unique() {
        return "pg-reg-" + com.wiggle.core.Ids.next("wf");
    }

    @Test @DisplayName("an unchanged graph re-registers as a no-op; a changed one under the same version is refused")
    void rejectsAChangedGraph() {
        try (JdbcStorage storage = storage()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            String name = unique();

            registry.register(spec(name, 1, false));
            registry.register(spec(name, 1, false));   // byte-identical: the fingerprint matches

            EngineException e = assertThrows(EngineException.class,
                    () -> registry.register(spec(name, 1, true)));
            assertEquals(409, e.statusCode());
            assertTrue(e.getMessage().contains("already registered with a different graph"), e.getMessage());

            // refused means refused: the stored graph is still the one-step topology
            storage.inTxVoid(tx -> assertTrue(tx.graphNode(name, 1, "n2").isEmpty(),
                    "the second step must not have been written"));
        }
    }

    @Test @DisplayName("force replaces the graph rows rather than merging into them")
    void forceReplacesTheGraph() {
        try (JdbcStorage storage = storage()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            String name = unique();

            registry.register(spec(name, 1, true));    // two steps
            storage.inTxVoid(tx -> assertTrue(tx.graphNode(name, 1, "n2").isPresent()));

            registry.register(spec(name, 1, false), true);   // forced down to one step

            // DELETE then re-INSERT: the dropped step's row is gone, not orphaned
            storage.inTxVoid(tx -> {
                assertTrue(tx.graphNode(name, 1, "n1").isPresent(), "the surviving step");
                assertTrue(tx.graphNode(name, 1, "n2").isEmpty(), "the removed step's row was deleted");
            });

            // and the blob was overwritten in place, not duplicated
            storage.inTxVoid(tx -> assertEquals(
                    spec(name, 1, false).fingerprint(),
                    tx.definitionFingerprint(name, 1).orElseThrow().value()));
        }
    }

    @Test @DisplayName("latestVersion is the highest declared version, whatever order they were written in")
    void latestIsTheHighest() {
        try (JdbcStorage storage = storage()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            String name = unique();

            registry.register(spec(name, 3, true));
            registry.register(spec(name, 1, false));
            registry.register(spec(name, 2, false));

            storage.inTxVoid(tx -> assertEquals(3, tx.latestVersion(name).orElseThrow()));
            storage.inTxVoid(tx -> assertTrue(tx.latestVersion(unique()).isEmpty(),
                    "an unknown name has no latest -- MAX() over no rows is not version 0"));
        }
    }

    @Test @DisplayName("a fingerprint is stored with the algorithm that produced it")
    void fingerprintIsStamped() {
        try (JdbcStorage storage = storage()) {
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            String name = unique();
            WorkflowDefinition one = spec(name, 1, false);
            registry.register(one);

            storage.inTxVoid(tx -> {
                var stored = tx.definitionFingerprint(name, 1).orElseThrow();
                assertEquals(one.fingerprint(), stored.value());
                assertEquals(WorkflowDefinition.FINGERPRINT_ALGO, stored.algo());
            });
            assertNotEquals(one.fingerprint(), spec(name, 2, true).fingerprint(),
                    "a different topology fingerprints differently");
        }
    }
}
