package dev.wiggle.server.engine;

/** Signals a client error (bad lease, unknown id, wrong state) rather than an engine bug. */
public class EngineException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int statusCode;

    public EngineException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int statusCode() { return statusCode; }

    public static EngineException notFound(String what) { return new EngineException(404, what + " not found"); }
    public static EngineException conflict(String msg) { return new EngineException(409, msg); }
    public static EngineException badRequest(String msg) { return new EngineException(400, msg); }
}
