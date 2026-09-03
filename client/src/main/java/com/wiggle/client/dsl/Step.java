package com.wiggle.client.dsl;

/**
 * A step body, in the spirit of {@code java.util.function.Function}: it receives the current context
 * as some type {@code I} and returns the next context as a possibly different type {@code O}. The
 * workflow itself carries no context type -- each step declares its own input type (via the
 * {@code Class<I>} passed to {@link WorkflowStream#step}) so the engine can rebuild it from the
 * JSON persisted between steps, and the output is encoded reflectively.
 */
@FunctionalInterface
public interface Step<I, O> {
    O apply(I context) throws Exception;
}
