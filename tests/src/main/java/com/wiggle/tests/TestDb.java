package com.wiggle.tests;

/**
 * Which database a test talks to, resolved from the environment.
 *
 * <p>There are two families of variable and the difference between them is deliberate.
 * {@code WIGGLE_TEST_DB_*} points the <em>generic</em> suite at one database -- see
 * {@link TestStorage} -- so the ordinary tests run against a real dialect instead of H2.
 * {@code WIGGLE_TEST_<BACKEND>_URL} separately opts in a backend's <em>dialect-specific</em> tests:
 * the {@code SKIP LOCKED} claim H2 cannot execute, Oracle's {@code MERGE}, and so on. Those URLs stay
 * per backend on purpose, because they name different servers and several can be enabled in one pass.
 *
 * <p>Credentials are the part that should not have been duplicated. {@code <BACKEND>_USER} /
 * {@code _PASSWORD} win where they are set, and otherwise fall back to the generic
 * {@code WIGGLE_TEST_DB_USER} / {@code _PASSWORD}. Without that fallback, the natural thing to do --
 * set {@code WIGGLE_TEST_DB_*} to run the suite on PostgreSQL, then add {@code WIGGLE_TEST_PG_URL} to
 * also get the PostgreSQL-only tests -- handed the driver a null password and died inside SCRAM
 * authentication, which says nothing about the variable that was actually missing.
 *
 * <p>A null password is left null rather than rejected: {@code trust} authentication is a legitimate
 * local setup, and guessing otherwise would break it.
 */
public final class TestDb {

    private TestDb() {}

    /** The first of {@code keys} that is set and non-blank, or null. */
    static String env(String... keys) {
        for (String key : keys) {
            String v = System.getenv(key);
            if (v != null && !v.isBlank()) return v;
        }
        return null;
    }

    /**
     * The JDBC URL for {@code backend} ({@code PG}, {@code MYSQL}, {@code ORACLE}, {@code SQLSERVER}),
     * or null when that backend is not enabled.
     *
     * <p>No generic fallback here, unlike the credentials: a backend's URL has to name that backend's
     * server, and falling back would aim the Oracle tests at whatever {@code WIGGLE_TEST_DB_URL} was.
     */
    public static String url(String backend) {
        return env("WIGGLE_TEST_" + backend + "_URL");
    }

    public static String user(String backend) {
        return env("WIGGLE_TEST_" + backend + "_USER", "WIGGLE_TEST_DB_USER");
    }

    public static String password(String backend) {
        return env("WIGGLE_TEST_" + backend + "_PASSWORD", "WIGGLE_TEST_DB_PASSWORD");
    }
}
