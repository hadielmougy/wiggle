package com.wiggle.client.flow;

import com.wiggle.core.RetryPolicy;

import java.util.function.UnaryOperator;

/**
 * The stage {@link WiggleFlow#thenForEach} returns; its {@link #combine combine} is mandatory. Each
 * item ran on its own isolated context -- the element itself -- so a combine is the only way the
 * results reach the flow. They arrive as a {@code List} ordered by item index, or a {@code Map} keyed
 * like the input when it was a map; see {@link Combines}.
 */
public final class Items extends Combines {

    private final WiggleFlow<?> from;
    private final String name;
    private final String itemsKey;
    private final UnaryOperator<GraphBuilder> body;

    Items(WiggleFlow<?> from, String name, String itemsKey, UnaryOperator<GraphBuilder> body) {
        this.from = from;
        this.name = name;
        this.itemsKey = itemsKey;
        this.body = body;
    }

    @Override
    <R> WiggleFlow<R> merge(String combineName, RetryPolicy retry, String queue) {
        return from.recordForEach(name, itemsKey, body, combineName, retry, queue);
    }
}
