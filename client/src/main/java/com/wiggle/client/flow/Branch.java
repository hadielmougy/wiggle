package com.wiggle.client.flow;

import java.util.function.UnaryOperator;

/** One arm of a {@link GraphBuilder#fork} -- a named sub-pipeline. */
public record Branch(String name, UnaryOperator<GraphBuilder> body) {

    public static Branch of(String name, UnaryOperator<GraphBuilder> body) {
        return new Branch(name, body);
    }
}
