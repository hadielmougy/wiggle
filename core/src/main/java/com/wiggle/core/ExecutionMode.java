package com.wiggle.core;

/**
 * How a workflow's steps are driven. Part of the compiled definition (and its fingerprint),
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
     * Like {@link #LOCAL_SYNC} but the worker buffers up to {@code WorkerOptions.localBatchSize}
     * steps and reports them in one batch at handback. Higher throughput, wider crash blast radius
     * (the whole batch re-executes on recovery) -- so steps must be idempotent.
     */
    LOCAL_ASYNC,
    /**
     * The server never dispatches: the steps run inside an instrumented application, which
     * reports them after the fact with their timings. The server checks each report against the
     * topology, records an anomaly where the reported order departs from it, and keeps per-step
     * durations for bottleneck analysis. An observed graph holds only TASK, PREDICATE and END
     * nodes, and its tokens are never offered to a worker.
     */
    OBSERVED,
    /** Defer to the server's configured default ({@code WIGGLE_EXECUTION_MODE}). */
    DEFAULT
}
