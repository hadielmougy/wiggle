package com.wiggle.server.coord;

/**
 * A coordinated namespace has no placement ring yet, so it cannot be resolved or served: an epoch must
 * be opened (naming its cells) before instances can start or route. Distinct from an internal error --
 * it is a retryable precondition (a bootstrap step is missing), mapped to gRPC {@code FAILED_PRECONDITION}
 * so a client can tell "not ready, retry after bootstrap" apart from a genuine failure.
 */
public final class NamespaceNotReadyException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NamespaceNotReadyException(String namespace) {
        super("namespace '" + namespace + "' has no ring; open an epoch before resolving it");
    }
}