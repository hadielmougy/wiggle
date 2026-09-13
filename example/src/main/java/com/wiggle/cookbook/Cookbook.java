package com.wiggle.cookbook;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
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
 * <p>Every flowSpec uses {@link Workflow#define}. A step's return REPLACES the context: it must
 * be the <b>whole</b> next document, not just the fields it touched -- {@link #with} builds that
 * full copy, and a partial map (e.g. bare {@code Map.of("k", v)}) deliberately clears every other
 * key. Nothing merges implicitly anywhere: fork branches rejoin at an explicit combine handler,
 * and a forEach body works on the ITEM's value (the base rides on {@code Step.base()}).
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
    public static FlowSpec linearWithGate() {
        return Wiggle.define("cb-linear-gate", Map.class, f -> f

                .thenApply("normalise")

                .thenApply("classify")

                // A false gate ends the instance successfully as "gated:eligible" -- not an error.
                .thenFilter("eligible")

                .thenAccept("welcome"));
    }

    // ---------------------------------------------------------------------------------------
    // 2. choose + fork + retry -- an exclusive branch whose body itself fans out in parallel.
    // ---------------------------------------------------------------------------------------
    public static FlowSpec chooseThenFork() {
        return Wiggle.define("cb-choose-fork", Map.class, f -> {
            // the large arm itself fans out: a fan-out inside a choice arm is just a fan-out whose
            // junction is the guard
            var large = f.when("is-large");
            var fraud = large.thenApply("fraud-check")
                    .withRetry(RetryPolicy.exponential(3, Duration.ofMillis(50)));
            var notice = large.thenAccept("manager-notice");
            var largeArm = Wiggle.allOf(fraud, notice).combine("large-merge", Map.class);

            var standard = f.otherwise().thenApply("fast-path");

            return Wiggle.oneOf(largeArm, standard).thenApply("settle");
        });
    }

    // ---------------------------------------------------------------------------------------
    // 3. forEach + per-step queue -- dynamic fan-out with mixed worker pools. The element IS each
    //    item's context (handlers take it directly; the base rides on Step.base()); the mandatory
    //    combine receives the collected final values.
    // ---------------------------------------------------------------------------------------
    public static FlowSpec forEachAcrossQueues() {
        return Wiggle.define("cb-foreach-queues", Map.class, f -> f.defaultQueue("cpu")

                .thenForEach("charge-items", "items", Map.class, b -> b
                        .thenApply("price")
                        // Only this step moves to the "gpu" queue; the workflow default stays "cpu".
                        .thenApply("render-thumbnail").onQueue("gpu"))
                .combine("collect-items", Map.class)

                .thenApply("summarise"));
    }

    // ---------------------------------------------------------------------------------------
    // 4. doWhile + gate -- retry-until-ready loop, with an inner gate short-circuiting a
    //    cancelled draw straight out of the loop.
    // ---------------------------------------------------------------------------------------
    public static FlowSpec pollUntilReady() {
        return Wiggle.define("cb-poll-until-ready", Map.class, f -> f

                .repeatWhile("still-pending", b -> b
                        // a gate short-circuits to the loop's exit (the enclosing join/end),
                        // not just the body -- a cancellation ends the whole instance here.
                        .thenFilter("not-cancelled")
                        .thenApply("poll"))

                .thenApply("finish"));
    }

    // ---------------------------------------------------------------------------------------
    // 5. awaitSignal (timeout + escalation) + choose -- branch on how the wait resolved.
    // ---------------------------------------------------------------------------------------
    public static FlowSpec approvalWithEscalation() {
        return Wiggle.define("cb-approval-escalation", Map.class, f -> {
            var waited = f
                    .thenApply("submit")
                    .thenAwait("manager-approval", Duration.ofMillis(200),
                            esc -> esc.thenApply("auto-escalate"));

            var escalated = waited.when("was-escalated").thenAccept("notify-director");
            var approved = waited.otherwise().thenAccept("notify-submitter");

            return Wiggle.oneOf(escalated, approved);
        });
    }

    // ---------------------------------------------------------------------------------------
    // 6. subWorkflow + gate + fork -- compose a registered child workflow into a bigger one.
    // ---------------------------------------------------------------------------------------
    public static FlowSpec childCheckThenFork() {
        return Wiggle.define("cb-parent", Map.class, f -> {
            var checked = f
                    // Runs cb-linear-gate as a child; its final context (incl. "vip") merges back here.
                    .thenSubFlow("run-eligibility", "cb-linear-gate", Map.class)
                    .thenFilter("child-passed");

            var provision = checked.thenApply("provision");
            var audit = checked.thenAccept("audit");

            return Wiggle.allOf(provision, audit).combine("merge", Map.class);
        });
    }

    // ---------------------------------------------------------------------------------------
    // 7. execution(LOCAL_ASYNC) + checkpoint + doWhile -- batched local execution with a
    //    deliberate commit point so a crash mid-loop only replays the current iteration.
    // ---------------------------------------------------------------------------------------
    public static FlowSpec batchedLoopWithCheckpoint() {
        return Wiggle.define("cb-batched-loop", Map.class, f -> f
                .execution(ExecutionMode.LOCAL_ASYNC)

                .repeatWhile("more-batches", b -> b
                        .thenApply("process-batch")
                        .checkpoint()) // flush the buffer before the next iteration under LOCAL_ASYNC

                .thenApply("finalise"));
    }

    // ---------------------------------------------------------------------------------------
    // 8. Everything at once -- step, gate, choose, fork (retry + per-step queue branches), forEach,
    //    sleep, awaitSignal + escalation, subWorkflow, doWhile, defaultQueue, and checkpoint,
    //    in a single graph. Not idiomatic; a deliberate stress test of the combination space.
    // ---------------------------------------------------------------------------------------
    public static FlowSpec kitchenSink() {
        return Wiggle.define("cb-kitchen-sink", Map.class, f -> {
            var ready = f.defaultQueue("default").execution(ExecutionMode.LOCAL_SYNC)
                    .thenApply("intake")
                    .thenFilter("has-items")
                    .thenSubFlow("run-eligibility", "cb-linear-gate", Map.class);

            var vip = ready.when("is-vip");
            var pack = vip.thenApply("pack")
                    .withRetry(RetryPolicy.fixed(2, Duration.ofMillis(20))).onQueue("packing");
            var notice = vip.thenSleep("brief-hold", Duration.ofMillis(50)).thenAccept("notice");
            var vipArm = Wiggle.allOf(pack, notice).combine("large-merge", Map.class);

            var standard = ready.otherwise()
                    .thenForEach("pack-items", "items", Map.class, body -> body.thenApply("pack-item"))
                    .combine("collect-packed", Map.class);

            return Wiggle.oneOf(vipArm, standard)
                    .thenAwait("dock-clear", Duration.ofMillis(150),
                            esc -> esc.thenAccept("auto-clear"))
                    .repeatWhile("more-checks", b -> b
                            .thenApply("run-check")
                            .checkpoint())
                    .thenApply("ship");
        });
    }
}
