package com.wiggle.tests;

/**
 * Where a test's server keeps its state. H2 in PostgreSQL mode by default -- which still exercises
 * the JDBC store, its migrations and the generic claim path, in-process and with nothing to install.
 *
 * <p>Set {@code WIGGLE_TEST_DB_URL} (with {@code _USER} / {@code _PASSWORD}) and the same tests run
 * against that database instead -- any backend the dist storage factory recognises: the real dialect,
 * its real claim path, real transactions and a real network hop. {@code docker compose up -d postgres}
 * provides a PostgreSQL on 5433. {@code WIGGLE_TEST_PG_URL} is still accepted as the older name.
 *
 * <pre>
 *   WIGGLE_TEST_DB_URL=jdbc:postgresql://localhost:5433/wiggle \
 *   WIGGLE_TEST_DB_USER=wiggle WIGGLE_TEST_DB_PASSWORD=wiggle \
 *     ./gradlew :tests:test
 * </pre>
 *
 * <p>These credentials also serve the per-backend opt-in tests, so adding {@code WIGGLE_TEST_PG_URL}
 * to the above is enough to enable the PostgreSQL-only ones -- see {@link TestDb}.
 *
 * <p>A live database is shared across tests and keeps its rows, so anything using this must isolate
 * itself by name rather than by assuming an empty store.
 */
public final class TestStorage {

    private TestStorage() {}

    /** WIGGLE_TEST_DB_URL points at any supported backend; WIGGLE_TEST_PG_URL is the older name. */
    private static String live() {
        return TestDb.env("WIGGLE_TEST_DB_URL", "WIGGLE_TEST_PG_URL");
    }

    /** True when a real database is configured, for a test that wants to say which it ran on. */
    public static boolean isLive() {
        return live() != null;
    }

    /** The JDBC URL: the configured database, or a fresh in-memory H2 named after {@code label}. */
    public static String url(String label) {
        String url = live();
        return url != null ? url
                : "jdbc:h2:mem:" + label + "-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
    }

    public static String user() {
        return live() != null ? TestDb.env("WIGGLE_TEST_DB_USER", "WIGGLE_TEST_PG_USER") : "sa";
    }

    public static String password() {
        return live() != null ? TestDb.env("WIGGLE_TEST_DB_PASSWORD", "WIGGLE_TEST_PG_PASSWORD") : "";
    }
}