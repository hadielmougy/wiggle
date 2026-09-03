package com.wiggle.greet;

import com.wiggle.client.worker.Handlers;

import java.util.Map;

/**
 * Step logic for {@link GreetFlow}, bound on a worker by name. The context is a plain JSON map, so
 * each method takes and returns a {@code Map<String,Object>}; returning it unchanged leaves the
 * context as-is (the engine diffs the result against its input).
 */
@Handlers("greet")
public final class GreetHandlers {

    public Map<String, Object> hello(Map<String, Object> ctx) {
        System.out.println("[greet#hello] hello " + ctx.get("name"));
        return ctx;
    }

    public Map<String, Object> world(Map<String, Object> ctx) {
        System.out.println("[greet#world] world");
        return ctx;
    }
}
