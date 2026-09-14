package com.wiggle.greet;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;

/**
 * The tiny "greet" flow used by {@link GreetWorker} and {@link GreetStart}. The flowSpec carries both
 * the topology (two task steps) and the step logic, so a worker that {@code register}s it is bound by
 * name ({@code greet#hello}, {@code greet#world}) and can execute instances.
 */
public final class GreetFlow {

    private GreetFlow() { }

    public static FlowSpec flowSpec() {
        return Wiggle.graph("greet")
                .step("hello")
                .step("world")
                .build();
    }
}
