package com.wiggle.greet;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;

/**
 * The tiny "greet" flow used by {@link GreetWorker} and {@link GreetStart}. The blueprint carries both
 * the topology (two task steps) and the step logic, so a worker that {@code register}s it is bound by
 * name ({@code greet#hello}, {@code greet#world}) and can execute instances.
 */
public final class GreetFlow {

    private GreetFlow() { }

    public static Blueprint blueprint() {
        return Workflow.define("greet")
                .step("hello")
                .step("world")
                .build();
    }
}
