package com.wiggle.cookbook;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Case;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A reference set of small workflows, each pairing operators that don't otherwise appear
 * together in {@code order-fulfilment} or {@code account transfer}. Read alongside
 * {@code docs/dsl-cookbook.md}, which explains what each one demonstrates and why. Run them
 * all with {@code ./gradlew :example:runCookbook} ({@link CookbookDemo}).
 *
 * <p>Every blueprint uses {@link Workflow#define}. A step's {@code fn} must return the
 * <b>whole</b> context, not just the fields it touched -- {@link #with} builds that full copy.
 * The engine then shallow-diffs the returned document against the one it was given and merges
 * only the keys that actually changed, so parallel branches that touch different fields merge
 * cleanly; returning a partial map (e.g. bare {@code Map.of("k", v)}) would tell the engine
 * every *other* key was deliberately cleared.
 */
public final class Cookbook {

    private Cookbook() {}

    /** Returns a copy of {@code ctx} with {@code key} set to {@code value}. */
    static Map<String, Object> with(Map<String, Object> ctx, String key, Object value) {
        Map<String, Object> m = new LinkedHashMap<>(ctx);
        m.put(key, value);
        return m;
    }

    /** Returns a copy of {@code ctx} with two keys set. */
    static Map<String, Object> with(Map<String, Object> ctx, String k1, Object v1, String k2, Object v2) {
        Map<String, Object> m = with(ctx, k1, v1);
        m.put(k2, v2);
        return m;
    }

    // ---------------------------------------------------------------------------------------
    // 1. step + then + effect + gate -- the smallest linear pipeline with a filter.
    // ---------------------------------------------------------------------------------------
    public static Blueprint linearWithGate() {
        return Workflow.define("cb-linear-gate")

                .step("normalise")

                .then("classify")

                // A false gate ends the instance successfully as "gated:eligible" -- not an error.
                .gate("eligible")

                .effect("welcome")

                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 2. choose + fork + retry -- an exclusive branch whose body itself fans out in parallel.
    // ---------------------------------------------------------------------------------------
    public static Blueprint chooseThenFork() {
        return Workflow.define("cb-choose-fork")

                .choose(
                        Case.when("is-large",
                                b -> b.fork(
                                        Branch.of("fraud-check", s -> s.step("fraud-check",
                                                RetryPolicy.exponential(3, Duration.ofMillis(50)))),
                                        Branch.of("manager-notice", s -> s.effect("manager-notice"))).combine("large-merge")),

                        Case.otherwise("standard", b -> b.step("fast-path")))

                .step("settle")
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 3. forkEach + per-step queue -- dynamic fan-out with mixed worker pools.
    // ---------------------------------------------------------------------------------------
    public static Blueprint forkEachAcrossQueues() {
        return Workflow.define("cb-foreach-queues").defaultQueue("cpu")

                .forkEach("charge-items", "items", "item", b -> b
                        // forkEach branches share one context, so a plain "priced" key would race
                        // across items (last write wins) -- namespace by itemIndex instead.
                        .step("price")
                        // Only this step moves to the "gpu" queue; the workflow default stays "cpu".
                        .step("render-thumbnail", "gpu"))

                .step("summarise")
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 4. doWhile + gate -- retry-until-ready loop, with an inner gate short-circuiting a
    //    cancelled draw straight out of the loop.
    // ---------------------------------------------------------------------------------------
    public static Blueprint pollUntilReady() {
        return Workflow.define("cb-poll-until-ready")

                .doWhile("still-pending", b -> b
                        // gate() short-circuits to the loop's exit (the enclosing join/end),
                        // not just the body -- a cancellation ends the whole instance here.
                        .gate("not-cancelled")
                        .step("poll"))

                .step("finish")
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 5. awaitSignal (timeout + escalation) + choose -- branch on how the wait resolved.
    // ---------------------------------------------------------------------------------------
    public static Blueprint approvalWithEscalation() {
        return Workflow.define("cb-approval-escalation")

                .step("submit")

                .awaitSignal("manager-approval", Duration.ofMillis(200),
                        esc -> esc.step("auto-escalate"))

                .choose(
                        Case.when("was-escalated",
                                b -> b.effect("notify-director")),
                        Case.otherwise("was-approved",
                                b -> b.effect("notify-submitter")))

                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 6. subWorkflow + gate + fork -- compose a registered child workflow into a bigger one.
    // ---------------------------------------------------------------------------------------
    public static Blueprint childCheckThenFork() {
        return Workflow.define("cb-parent")

                // Runs cb-linear-gate as a child; its final context (incl. "vip") merges back here.
                .subWorkflow("run-eligibility", "cb-linear-gate")

                .gate("child-passed")

                .fork(
                        Branch.of("provision", s -> s.step("provision")),
                        Branch.of("audit", s -> s.effect("audit")))
                .combine("merge")
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 7. execution(LOCAL_ASYNC) + checkpoint + doWhile -- batched local execution with a
    //    deliberate commit point so a crash mid-loop only replays the current iteration.
    // ---------------------------------------------------------------------------------------
    public static Blueprint batchedLoopWithCheckpoint() {
        return Workflow.define("cb-batched-loop").execution(ExecutionMode.LOCAL_ASYNC)

                .doWhile("more-batches", b -> b
                        .step("process-batch")
                        .checkpoint()) // flush the buffer before the next iteration under LOCAL_ASYNC

                .step("finalise")
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 8. Everything at once -- step, gate, choose, fork (retry + per-step queue branches), forkEach,
    //    sleep, awaitSignal + escalation, subWorkflow, doWhile, defaultQueue, and checkpoint,
    //    in a single graph. Not idiomatic; a deliberate stress test of the combination space.
    // ---------------------------------------------------------------------------------------
    public static Blueprint kitchenSink() {
        return Workflow.define("cb-kitchen-sink").defaultQueue("default").execution(ExecutionMode.LOCAL_SYNC)

                .step("intake")

                .gate("has-items")

                .subWorkflow("run-eligibility", "cb-linear-gate")

                .choose(
                        Case.when("is-vip", b -> b
                                .fork(
                                        Branch.of("priority-pack", s -> s
                                                .step("pack",
                                                        RetryPolicy.fixed(2, Duration.ofMillis(20)), "packing")),
                                        Branch.of("priority-notice", s -> s
                                                .sleep("brief-hold", Duration.ofMillis(50))
                                                .effect("notice"))).combine("large-merge")),

                        Case.otherwise("standard", b -> b
                                // Namespaced by itemIndex for the same reason as example 3.
                                .forkEach("pack-items", "items", "item", body -> body
                                        .step("pack-item"))))

                .awaitSignal("dock-clear", Duration.ofMillis(150),
                        esc -> esc.effect("auto-clear"))

                .doWhile("more-checks", b -> b
                        .step("run-check")
                        .checkpoint())

                .step("ship")
                .build();
    }
}
