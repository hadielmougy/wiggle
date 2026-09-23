package com.wiggle.server.engine;

/**
 * One call into a {@link RunningMode}, self-typed by its own result: {@link AdvanceRunContext}
 * declares itself {@code ExecutionContext<AdvanceOutcome>}, {@link CompleteExecutionContext}
 * declares itself {@code ExecutionContext<Void>}. {@code runOn} is virtual dispatch, not a switch --
 * each implementation's own {@code T} is checked once, concretely, against its own declaration,
 * so {@link RunningMode#execute} can hand the result straight back with no cast.
 */
public sealed interface ExecutionContext<T> permits
        CompleteExecutionContext, AdvanceRunContext, AdvanceBatchContext, ObserveRunContext, SettleContext {

    T runOn(BaseRunningMode mode);
}
