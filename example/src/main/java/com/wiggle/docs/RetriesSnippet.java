package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;

/**
 * The code on <a href="https://wiggle.sh/patterns/retries/">wiggle.sh/patterns/retries</a>.
 *
 * <p>That page quotes fragments -- a single chained call, one handler line -- rather than whole
 * programs, which is the right shape for what it explains. A region is any run of lines, so a
 * fragment can still be a slice of a chain the compiler checked: the page shows the line, the build
 * proves the line is real.
 */
public final class RetriesSnippet {

    public record Order(int quantity, String status) {}

    public record Ctx(boolean ready, boolean cancelled, String status, String ref) {
        Ctx withStatus(String s) { return new Ctx(ready, cancelled, s, ref); }
    }

    interface OrderSteps {
        Order   validate(Order o);
        boolean inStock(Order o);
        Order   authorise(Order o);
        Order   charge(Order o);
    }

    interface SettlementSteps {
        boolean stillPending(Ctx c);
        boolean notCancelled(Ctx c);
        Ctx     poll(Ctx c);
        Ctx     finish(Ctx c);
    }

    static FlowSpec retried() {
        return FlowSpec.define("retried", Order.class, OrderSteps.class, (f, s) -> f
                .thenApply(s::validate)
                // docs:begin retry-line
                .thenApply(s::authorise, RetryPolicy.exponential(5, Duration.ofMillis(100)))
                // docs:end retry-line
                .thenApply(s::charge));
    }

    static FlowSpec gated() {
        return FlowSpec.define("gated", Order.class, OrderSteps.class, (f, s) -> f
                // docs:begin gate-chain
                .thenApply(s::validate)
                .thenFilter(s::inStock)    // false ⇒ the instance ENDS CLEANLY — not an error, no alarm
                .thenApply(s::charge)
                // docs:end gate-chain
        );
    }

    static FlowSpec settlement() {
        // docs:begin poll-loop
        FlowSpec settling = FlowSpec.define("await-settlement", Ctx.class, SettlementSteps.class, (f, s) -> f
                .repeatWhile(s::stillPending, b -> b
                        .thenFilter(s::notCancelled)       // false short-circuits OUT of the loop entirely
                        .thenApply(s::poll)
                        .thenSleep("backoff", Duration.ofSeconds(30)))   // parked server-side, no worker held
                .thenApply(s::finish));
        // docs:end poll-loop
        return settling;
    }

    private RetriesSnippet() {}
}
