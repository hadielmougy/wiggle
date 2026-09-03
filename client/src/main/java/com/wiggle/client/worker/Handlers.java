package com.wiggle.client.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as the handler set for a workflow: its methods implement the workflow's steps. The
 * value is the workflow name the methods bind to (the same name passed to {@code Workflow.define}).
 *
 * <p>Each public instance method whose name matches a step (case/style-insensitive, so {@code
 * inStock} binds {@code in-stock}) is a handler; its <em>signature</em> defines the step:
 * <ul>
 *   <li>a single parameter is the step input, decoded from the persisted JSON into that type;</li>
 *   <li>a {@code boolean} return is a gate/guard, {@code void} is an effect, any other return type
 *       is a task whose value becomes the next context (types may change from step to step);</li>
 *   <li>several {@link Arm}-annotated parameters make a combine for the matching {@code combine}
 *       node -- each branch's result decoded into its parameter's type; add a {@link Context}
 *       parameter for the pre-fork context.</li>
 * </ul>
 * A method annotated {@link Decode} is not a step but a custom decoder for its return type (the
 * seam for schema versioning / upcasts / bespoke codecs).
 *
 * @see Worker#handlers(Object)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Handlers {
    String value();
}
