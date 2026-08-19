package dev.wiggle.client.dsl;

import java.util.function.UnaryOperator;

/** One arm of a {@link WorkflowStream#fork} -- a named sub-pipeline over the same context type. */
public record Branch<T>(String name, UnaryOperator<WorkflowStream<T>> body) {

    public static <T> Branch<T> of(String name, UnaryOperator<WorkflowStream<T>> body) {
        return new Branch<>(name, body);
    }
}
