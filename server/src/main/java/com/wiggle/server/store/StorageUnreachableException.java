package com.wiggle.server.store;

/**
 * The database could not be reached at all: no connection could be opened, so nothing was applied.
 * Transient, so a caller sees UNAVAILABLE, but not replayed in place: a replay would wait out the
 * same connection timeout again.
 */
public final class StorageUnreachableException extends StorageException {

    private static final long serialVersionUID = 1L;

    public StorageUnreachableException(String message, Throwable cause) {
        super(message, cause, Classification.TRANSIENT);
    }
}
