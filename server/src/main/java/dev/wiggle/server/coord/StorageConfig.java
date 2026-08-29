package dev.wiggle.server.coord;

/**
 * The storage a cell runs on, as captured in the namespace registry. The credential is a
 * {@code secretRef} -- a pointer resolved to the real secret at deploy time by a
 * {@link SecretResolver} -- so the coordinator never stores a password (R22). An in-memory cell
 * (for tests/dev) leaves {@code jdbcUrl} null.
 */
public record StorageConfig(String scheme, String jdbcUrl, String user, String secretRef, int poolSize) {

    /** An in-memory store (no JDBC): the default for tests and single-box dev. */
    public static StorageConfig inMemory() {
        return new StorageConfig("memory", null, null, null, 0);
    }

    /** A JDBC store; {@code secretRef} points at the password, resolved at deploy time. */
    public static StorageConfig jdbc(String jdbcUrl, String user, String secretRef, int poolSize) {
        return new StorageConfig("jdbc", jdbcUrl, user, secretRef, poolSize);
    }

    public boolean isInMemory() {
        return jdbcUrl == null || jdbcUrl.isBlank();
    }
}
