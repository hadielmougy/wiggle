package dev.wiggle.client.worker;

/** Thrown from an activity to skip retries and fail the instance immediately. */
public class PermanentActivityException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PermanentActivityException(String message) { super(message); }
    public PermanentActivityException(String message, Throwable cause) { super(message, cause); }
}
