package com.wiggle.cookbook;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Aggregator;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Case;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
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
    private static Map<String, Object> with(Map<String, Object> ctx, String key, Object value) {
        Map<String, Object> m = new LinkedHashMap<>(ctx);
        m.put(key, value);
        return m;
    }

    /** Returns a copy of {@code ctx} with two keys set. */
    private static Map<String, Object> with(Map<String, Object> ctx, String k1, Object v1, String k2, Object v2) {
        Map<String, Object> m = with(ctx, k1, v1);
        m.put(k2, v2);
        return m;
    }

    // ---------------------------------------------------------------------------------------
    // 1. step + then + effect + gate -- the smallest linear pipeline with a filter.
    // ---------------------------------------------------------------------------------------
    public static Blueprint linearWithGate() {
        return Workflow.define("cb-linear-gate")

                .step("normalise", ctx -> with(ctx, "email", String.valueOf(ctx.get("email")).toLowerCase()))

                .then("classify", ctx -> with(ctx, "vip", "hadi@wiggle.dev".equals(ctx.get("email"))))

                // A false gate ends the instance successfully as "gated:eligible" -- not an error.
                .gate("eligible", ctx -> Boolean.TRUE.equals(ctx.get("vip")))

                .effect("welcome", ctx -> System.out.println("   [cookbook] welcome email -> " + ctx.get("email")))

                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 2. choose + fork + retry -- an exclusive branch whose body itself fans out in parallel.
    // ---------------------------------------------------------------------------------------
    public static Blueprint chooseThenFork() {
        return Workflow.define("cb-choose-fork")

                .choose(
                        Case.when("is-large", ctx -> ((Number) ctx.get("amount")).doubleValue() >= 1000,
                                b -> b.fork(
                                        Branch.of("fraud-check", s -> s.step("fraud-check",
                                                ctx -> with(ctx, "fraudChecked", true),
                                                RetryPolicy.exponential(3, Duration.ofMillis(50)))),
                                        Branch.of("manager-notice", s -> s.effect("manager-notice",
                                                ctx -> System.out.println("   [cookbook] large txn: " + ctx.get("amount"))))).combine("large-merge", Aggregator.union())),

                        Case.otherwise("standard", b -> b.step("fast-path", ctx -> with(ctx, "fraudChecked", false))))

                .step("settle", ctx -> with(ctx, "settled", true))
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
                        .step("price", item -> with(item, "priced-" + item.get("itemIndex"), true))
                        // Only this step moves to the "gpu" queue; the workflow default stays "cpu".
                        .step("render-thumbnail", item ->
                                with(item, "thumbnail-" + item.get("itemIndex"), "thumb-" + item.get("itemIndex")),
                                "gpu"))

                .step("summarise", ctx -> with(ctx, "done", true))
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 4. doWhile + gate -- retry-until-ready loop, with an inner gate short-circuiting a
    //    cancelled draw straight out of the loop.
    // ---------------------------------------------------------------------------------------
    public static Blueprint pollUntilReady() {
        return Workflow.define("cb-poll-until-ready")

                .doWhile("still-pending", ctx -> !Boolean.TRUE.equals(ctx.get("ready")), b -> b
                        // gate() short-circuits to the loop's exit (the enclosing join/end),
                        // not just the body -- a cancellation ends the whole instance here.
                        .gate("not-cancelled", ctx -> !Boolean.TRUE.equals(ctx.get("cancelled")))
                        .step("poll", ctx -> {
                            int n = ((Number) ctx.getOrDefault("polls", 0)).intValue() + 1;
                            return with(with(ctx, "polls", n), "ready", n >= 3);
                        }))

                .step("finish", ctx -> with(ctx, "finishedAfter", ctx.get("polls")))
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 5. awaitSignal (timeout + escalation) + choose -- branch on how the wait resolved.
    // ---------------------------------------------------------------------------------------
    public static Blueprint approvalWithEscalation() {
        return Workflow.define("cb-approval-escalation")

                .step("submit", ctx -> with(ctx, "submitted", true))

                .awaitSignal("manager-approval", Duration.ofMillis(200),
                        esc -> esc.step("auto-escalate", ctx -> with(with(ctx, "escalated", true), "approved", false)))

                .choose(
                        Case.when("was-escalated", ctx -> Boolean.TRUE.equals(ctx.get("escalated")),
                                b -> b.effect("notify-director", ctx -> System.out.println("   [cookbook] escalated to director"))),
                        Case.otherwise("was-approved",
                                b -> b.effect("notify-submitter", ctx -> System.out.println("   [cookbook] approved directly"))))

                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 6. subWorkflow + gate + fork -- compose a registered child workflow into a bigger one.
    // ---------------------------------------------------------------------------------------
    public static Blueprint childCheckThenFork() {
        return Workflow.define("cb-parent")

                // Runs cb-linear-gate as a child; its final context (incl. "vip") merges back here.
                .subWorkflow("run-eligibility", "cb-linear-gate")

                .gate("child-passed", ctx -> Boolean.TRUE.equals(ctx.get("vip")))

                .fork(
                        Branch.of("provision", s -> s.step("provision", ctx -> with(ctx, "provisioned", true))),
                        Branch.of("audit", s -> s.effect("audit", ctx -> System.out.println("   [cookbook] provisioning audited"))))
                .combine("merge", Aggregator.union())
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 7. execution(LOCAL_ASYNC) + checkpoint + doWhile -- batched local execution with a
    //    deliberate commit point so a crash mid-loop only replays the current iteration.
    // ---------------------------------------------------------------------------------------
    public static Blueprint batchedLoopWithCheckpoint() {
        return Workflow.define("cb-batched-loop").execution(ExecutionMode.LOCAL_ASYNC)

                .doWhile("more-batches", ctx -> ((Number) ctx.getOrDefault("batch", 0)).intValue() < 3, b -> b
                        .step("process-batch", ctx -> {
                            int n = ((Number) ctx.getOrDefault("batch", 0)).intValue() + 1;
                            return with(ctx, "batch", n);
                        })
                        .checkpoint()) // flush the buffer before the next iteration under LOCAL_ASYNC

                .step("finalise", ctx -> with(ctx, "batchesDone", ctx.get("batch")))
                .build();
    }

    // ---------------------------------------------------------------------------------------
    // 8. Everything at once -- step, gate, choose, fork (retry + per-step queue branches), forkEach,
    //    sleep, awaitSignal + escalation, subWorkflow, doWhile, defaultQueue, and checkpoint,
    //    in a single graph. Not idiomatic; a deliberate stress test of the combination space.
    // ---------------------------------------------------------------------------------------
    public static Blueprint kitchenSink() {
        return Workflow.define("cb-kitchen-sink").defaultQueue("default").execution(ExecutionMode.LOCAL_SYNC)

                .step("intake", ctx -> with(ctx, "stage", "intake"))

                .gate("has-items", ctx -> ctx.get("items") != null && !((List<?>) ctx.get("items")).isEmpty())

                .subWorkflow("run-eligibility", "cb-linear-gate")

                .choose(
                        Case.when("is-vip", ctx -> Boolean.TRUE.equals(ctx.get("vip")), b -> b
                                .fork(
                                        Branch.of("priority-pack", s -> s
                                                .step("pack", ctx -> with(ctx, "packed", true),
                                                        RetryPolicy.fixed(2, Duration.ofMillis(20)), "packing")),
                                        Branch.of("priority-notice", s -> s
                                                .sleep("brief-hold", Duration.ofMillis(50))
                                                .effect("notice", ctx -> System.out.println("   [cookbook] VIP order held briefly")))).combine("large-merge", Aggregator.union())),

                        Case.otherwise("standard", b -> b
                                // Namespaced by itemIndex for the same reason as example 3.
                                .forkEach("pack-items", "items", "item", body -> body
                                        .step("pack-item", item -> with(item, "packed-" + item.get("itemIndex"), true)))))

                .awaitSignal("dock-clear", Duration.ofMillis(150),
                        esc -> esc.effect("auto-clear", ctx -> System.out.println("   [cookbook] dock auto-cleared")))

                .doWhile("more-checks", ctx -> ((Number) ctx.getOrDefault("checks", 0)).intValue() < 2, b -> b
                        .step("run-check", ctx -> {
                            int n = ((Number) ctx.getOrDefault("checks", 0)).intValue() + 1;
                            return with(ctx, "checks", n);
                        })
                        .checkpoint())

                .step("ship", ctx -> with(ctx, "stage", "shipped"))
                .build();
    }
}
