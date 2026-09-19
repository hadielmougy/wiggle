package com.wiggle.greet;

import com.wiggle.client.flow.FlowSpec;
import java.util.Map;

/**
 * The tiny "greet" flow used by {@link GreetWorker} and {@link GreetStart}. The flowSpec carries both
 * the topology (two task steps) and the step logic, so a worker that {@code register}s it is bound by
 * name ({@code greet#hello}, {@code greet#world}) and can execute instances.
 */
public final class GreetFlow {

    interface GreetSteps {
        Map<String, Object> hello(Map<String, Object> ctx);
        Map<String, Object> world(Map<String, Object> ctx);
    }

    private GreetFlow() { }

    public static FlowSpec flowSpec() {
        return FlowSpec.define("greet", 1, Map.class, GreetSteps.class, (f, s) -> f
                .thenApply(s::hello)
                .thenApply(s::world));
    }
}
