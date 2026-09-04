package com.wiggle.client.dsl;

import java.util.function.UnaryOperator;

/** One arm of a {@link WorkflowBuilder#fork} -- a named sub-pipeline. */
public record Branch(String name, UnaryOperator<WorkflowBuilder> body) {

    public static Branch of(String name, UnaryOperator<WorkflowBuilder> body) {
        return new Branch(name, body);
    }
}
