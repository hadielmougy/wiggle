package com.wiggle.client.dsl;

import java.util.function.UnaryOperator;

/** One arm of a {@link WorkflowStream#fork} -- a named sub-pipeline. */
public record Branch(String name, UnaryOperator<WorkflowStream> body) {

    public static Branch of(String name, UnaryOperator<WorkflowStream> body) {
        return new Branch(name, body);
    }
}
