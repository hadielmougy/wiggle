package com.wiggle.postgres;

import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.tests.TestDb;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * A store in a schema of its own, on a PostgreSQL whose pgvector lives in another schema that is
 * not on the store's search path: the store still gets the native vector column, indexes it, and
 * searches it in the database. And a store that has no native column does not take another schema's
 * for its own. Opt-in, as {@link PostgresClaimTest}:
 *
 * <pre>
 *   WIGGLE_TEST_PG_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_PG_USER=wiggle WIGGLE_TEST_PG_PASSWORD=wiggle \
 *     ./gradlew :tests:test --tests "com.wiggle.postgres.PgvectorSchemaTest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "WIGGLE_TEST_PG_URL", matches = ".+")
class PgvectorSchemaTest {

    @Test @DisplayName("pgvector in a schema off the search path still backs a schema-scoped store")
    void extensionOffTheSearchPath() throws Exception {
        String extensionSchema = installedOrInstall();
        String schema = "pgv_" + System.nanoTime();
        exec("CREATE SCHEMA " + schema);
        try {
            assertNotEquals(schema, extensionSchema);
            JdbcStorage storage = storage(schema);
            storage.migrate();
            assertEquals(1, count("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='" + schema
                    + "' AND table_name='wf_search_vec' AND column_name='vec'"), "the native vector column was added");

            long now = System.currentTimeMillis();
            List<Rows.SearchHit> hits = storage.inTx(tx -> {
                tx.upsertSearchDoc(new Rows.SearchDoc("near", "w", 1, "RUNNING", null, "near", now, now));
                tx.upsertSearchDoc(new Rows.SearchDoc("far", "w", 1, "RUNNING", null, "far", now, now));
                tx.putSearchModel(new Rows.SearchModel("m", 3, Rows.SearchModel.BUILDING, now, null));
                tx.ensureVectorIndex("m", 3);
                tx.upsertSearchVectors(List.of(new Rows.SearchVector("near", "m", new float[]{1, 0, 0}, now),
                        new Rows.SearchVector("far", "m", new float[]{0, 1, 0}, now)));
                return tx.searchVectors(new Rows.VectorQuery("m", new float[]{1, 0.1f, 0}, null, null, null, null, 2));
            });
            assertEquals(List.of("near", "far"), hits.stream().map(h -> h.doc().instanceId()).toList());
            assertEquals(1, count("SELECT COUNT(*) FROM pg_indexes WHERE schemaname='" + schema
                    + "' AND indexdef LIKE '%USING hnsw%'"), "the model's HNSW index was built in the store's schema");
            assertEquals(2, count("SELECT COUNT(*) FROM " + schema + ".wf_search_vec WHERE vec IS NOT NULL"),
                    "vectors were kept natively, not compared in Java");
        } finally {
            exec("DROP SCHEMA " + schema + " CASCADE");
        }
    }

    @Test @DisplayName("a store without the native column does not take another schema's for its own")
    void anotherSchemasColumnIsNotOurs() throws Exception {
        installedOrInstall();
        String withColumn = "pgv_" + System.nanoTime();
        String without = "pgv_" + System.nanoTime();
        exec("CREATE SCHEMA " + withColumn);
        exec("CREATE SCHEMA " + without);
        try {
            storage(withColumn).migrate();
            storage(without).migrate();
            exec("ALTER TABLE " + without + ".wf_search_vec DROP COLUMN vec");   // as where pgvector could not be enabled

            long now = System.currentTimeMillis();
            storage(without).inTxVoid(tx -> {
                tx.upsertSearchDoc(new Rows.SearchDoc("a", "w", 1, "RUNNING", null, "a", now, now));
                tx.ensureVectorIndex("m", 3);
                tx.upsertSearchVectors(List.of(new Rows.SearchVector("a", "m", new float[]{1, 0, 0}, now)));
            });
            assertEquals(1, count("SELECT COUNT(*) FROM " + without + ".wf_search_vec WHERE embedding IS NOT NULL"),
                    "kept in the embedding column, as a store without the native column does");
        } finally {
            exec("DROP SCHEMA " + withColumn + " CASCADE");
            exec("DROP SCHEMA " + without + " CASCADE");
        }
    }

    /** The schema pgvector is installed in, installing it in {@code public} where it is not; skips
     *  where this PostgreSQL has no pgvector. */
    private static String installedOrInstall() throws SQLException {
        assumeTrue(count("SELECT COUNT(*) FROM pg_available_extensions WHERE name='vector'") > 0,
                "pgvector is not installed on this PostgreSQL");
        exec("CREATE EXTENSION IF NOT EXISTS vector SCHEMA public");
        try (Connection c = connect(TestDb.url("PG")); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT n.nspname FROM pg_extension e JOIN pg_namespace n "
                     + "ON n.oid=e.extnamespace WHERE e.extname='vector'")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static JdbcStorage storage(String schema) {
        String url = TestDb.url("PG") + (TestDb.url("PG").contains("?") ? "&" : "?") + "currentSchema=" + schema;
        return new JdbcStorage(url, TestDb.user("PG"), TestDb.password("PG"), 2, new PostgresDialect());
    }

    private static long count(String sql) throws SQLException {
        try (Connection c = connect(TestDb.url("PG")); Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Connection c = connect(TestDb.url("PG")); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    private static Connection connect(String url) throws SQLException {
        return DriverManager.getConnection(url, TestDb.user("PG"), TestDb.password("PG"));
    }
}
