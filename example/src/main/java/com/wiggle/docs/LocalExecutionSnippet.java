package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.ExecutionMode;

/** The execution-mode line quoted in {@code docs/local-execution.md}. */
public final class LocalExecutionSnippet {

    public record Ctx(String state) {}

    interface Steps {
        Ctx first(Ctx c);
        Ctx second(Ctx c);
    }

    static FlowSpec define() {
        // docs:begin execution-mode
        FlowSpec spec = FlowSpec.define("name", 1, Ctx.class, Steps.class, (f, s) -> f
                .executeInLocalSync()   // or executeInServer() / executeInLocalAsync(); none = server default
                .thenApply(s::first)
        // docs:elide         ...);
                // docs:skip
                .thenApply(s::second));
                // docs:resume
        // docs:end execution-mode
        return spec;
    }

    private LocalExecutionSnippet() {}
}
