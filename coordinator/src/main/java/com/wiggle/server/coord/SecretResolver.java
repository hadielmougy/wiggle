package com.wiggle.server.coord;

/**
 * Resolves a {@code secretRef} (a pointer stored in the namespace registry) to the real secret at
 * deploy time, so the coordinator never persists a credential (R22). The default treats the ref as an
 * environment-variable name; production deployments inject one backed by a secret manager.
 */
@FunctionalInterface
public interface SecretResolver {

    /** The secret for {@code secretRef}, or {@code null} if there is none (e.g. an in-memory cell). */
    String resolve(String secretRef);

    /** Treats the ref as the name of an environment variable holding the secret. */
    SecretResolver ENV = ref -> ref == null || ref.isBlank() ? null : System.getenv(ref);
}
