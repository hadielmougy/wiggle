package dev.wiggle.core;

/**
 * How a workflow's steps are driven. Part of the compiled definition (and its content hash),
 * so an instance keeps the mode it started on.
 */
public enum ExecutionMode {
    /** The server advances one node at a time; the worker executes a single step per claim. */
    SERVER,
    /**
     * The worker chains consecutive same-queue steps locally, committing each to the server
     * before running the next. As durable as {@link #SERVER} (one-step crash blast radius),
     * but without the re-poll round-trip and context re-shipping between steps.
     */
    LOCAL_SYNC,
    /**
     * Like {@link #LOCAL_SYNC} but the worker reports a whole local run in one batch at handback.
     * Higher throughput, wider crash blast radius (the whole run re-executes) -- requires
     * idempotent steps. (Not yet implemented on the worker; treated as {@link #LOCAL_SYNC}.)
     */
    LOCAL_ASYNC,
    /** Defer to the server's configured default ({@code WIGGLE_EXECUTION_MODE}). */
    DEFAULT
}
