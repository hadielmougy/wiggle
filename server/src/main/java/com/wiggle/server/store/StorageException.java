package com.wiggle.server.store;

/**
 * A storage operation that did not complete, carrying what a caller may assume about the attempt's
 * durable effect. The {@link Classification} is the whole point of this type: "the database said no"
 * is three different events, and only one of them may be repeated blindly.
 *
 * <p>A backend MUST classify every failure it raises. Where it cannot tell, {@link
 * Classification#PERMANENT} is the safe answer: it costs a recoverable failure its retry, while
 * guessing {@link Classification#TRANSIENT} would re-apply work that may already be durable.
 */
public class StorageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** What the failure says about the work the transaction was carrying. */
    public enum Classification {

        /**
         * The database was momentarily unable and the transaction rolled back: nothing was applied,
         * and nothing can have been. The call may be repeated as it stands, whether or not the work
         * it carries is idempotent. Connection loss, a pool timeout, a deadlock victim and a
         * serialization failure all land here.
         */
        TRANSIENT,

        /**
         * The commit's outcome is unknown -- the connection died while the commit was in flight, so
         * the work may be durable in full. It MUST NOT be repeated unless the caller knows the work
         * is idempotent, and it MUST NOT be reported to a client as "nothing happened".
         */
        AMBIGUOUS,

        /**
         * The statement was refused on its own terms: nothing was applied, and a repeat fails
         * identically. A constraint violation, a syntax error, a missing table, bad credentials.
         */
        PERMANENT
    }

    private final Classification classification;

    /** A failure whose class could not be determined: {@link Classification#PERMANENT}. */
    public StorageException(String message, Throwable cause) {
        this(message, cause, Classification.PERMANENT);
    }

    public StorageException(String message, Throwable cause, Classification classification) {
        super(message, cause);
        this.classification = classification;
    }

    public Classification classification() {
        return classification;
    }

    /** Whether the attempt provably applied nothing and may be repeated as it stands. */
    public boolean repeatable() {
        return classification == Classification.TRANSIENT;
    }
}
