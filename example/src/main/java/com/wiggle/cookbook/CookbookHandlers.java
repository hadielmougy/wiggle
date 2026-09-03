package com.wiggle.cookbook;

import com.wiggle.client.worker.Handlers;

import java.util.List;
import java.util.Map;

import static com.wiggle.cookbook.Cookbook.with;

/**
 * Step logic for the {@link Cookbook} blueprints, one {@code @Handlers} class per workflow. Each
 * method's name matches a step (case/style-insensitive, so {@code inStock} would serve {@code
 * in-stock}) and its signature defines the step: a {@code Map<String,Object>} in and out is a task, a
 * {@code boolean} return is a gate, {@code void} is an effect. Combine steps ({@code large-merge},
 * {@code merge}) have no method, so their branches fold with the default union.
 */
public final class CookbookHandlers {

    private CookbookHandlers() {}

    /** 1. cb-linear-gate. */
    @Handlers("cb-linear-gate")
    public static final class LinearGate {
        public Map<String, Object> normalise(Map<String, Object> ctx) {
            return with(ctx, "email", String.valueOf(ctx.get("email")).toLowerCase());
        }
        public Map<String, Object> classify(Map<String, Object> ctx) {
            return with(ctx, "vip", "hadi@wiggle.dev".equals(ctx.get("email")));
        }
        public boolean eligible(Map<String, Object> ctx) {
            return Boolean.TRUE.equals(ctx.get("vip"));
        }
        public void welcome(Map<String, Object> ctx) {
            System.out.println("   [cookbook] welcome email -> " + ctx.get("email"));
        }
    }

    /** 2. cb-choose-fork. */
    @Handlers("cb-choose-fork")
    public static final class ChooseFork {
        public boolean isLarge(Map<String, Object> ctx) {
            return ((Number) ctx.get("amount")).doubleValue() >= 1000;
        }
        public Map<String, Object> fraudCheck(Map<String, Object> ctx) {
            return with(ctx, "fraudChecked", true);
        }
        public void managerNotice(Map<String, Object> ctx) {
            System.out.println("   [cookbook] large txn: " + ctx.get("amount"));
        }
        public Map<String, Object> fastPath(Map<String, Object> ctx) {
            return with(ctx, "fraudChecked", false);
        }
        public Map<String, Object> settle(Map<String, Object> ctx) {
            return with(ctx, "settled", true);
        }
    }

    /** 3. cb-foreach-queues. */
    @Handlers("cb-foreach-queues")
    public static final class ForeachQueues {
        public Map<String, Object> price(Map<String, Object> item) {
            return with(item, "priced-" + item.get("itemIndex"), true);
        }
        public Map<String, Object> renderThumbnail(Map<String, Object> item) {
            return with(item, "thumbnail-" + item.get("itemIndex"), "thumb-" + item.get("itemIndex"));
        }
        public Map<String, Object> summarise(Map<String, Object> ctx) {
            return with(ctx, "done", true);
        }
    }

    /** 4. cb-poll-until-ready. */
    @Handlers("cb-poll-until-ready")
    public static final class PollUntilReady {
        public boolean stillPending(Map<String, Object> ctx) {
            return !Boolean.TRUE.equals(ctx.get("ready"));
        }
        public boolean notCancelled(Map<String, Object> ctx) {
            return !Boolean.TRUE.equals(ctx.get("cancelled"));
        }
        public Map<String, Object> poll(Map<String, Object> ctx) {
            int n = ((Number) ctx.getOrDefault("polls", 0)).intValue() + 1;
            return with(with(ctx, "polls", n), "ready", n >= 3);
        }
        public Map<String, Object> finish(Map<String, Object> ctx) {
            return with(ctx, "finishedAfter", ctx.get("polls"));
        }
    }

    /** 5. cb-approval-escalation. */
    @Handlers("cb-approval-escalation")
    public static final class ApprovalEscalation {
        public Map<String, Object> submit(Map<String, Object> ctx) {
            return with(ctx, "submitted", true);
        }
        public Map<String, Object> autoEscalate(Map<String, Object> ctx) {
            return with(with(ctx, "escalated", true), "approved", false);
        }
        public boolean wasEscalated(Map<String, Object> ctx) {
            return Boolean.TRUE.equals(ctx.get("escalated"));
        }
        public void notifyDirector(Map<String, Object> ctx) {
            System.out.println("   [cookbook] escalated to director");
        }
        public void notifySubmitter(Map<String, Object> ctx) {
            System.out.println("   [cookbook] approved directly");
        }
    }

    /** 6. cb-parent. */
    @Handlers("cb-parent")
    public static final class Parent {
        public boolean childPassed(Map<String, Object> ctx) {
            return Boolean.TRUE.equals(ctx.get("vip"));
        }
        public Map<String, Object> provision(Map<String, Object> ctx) {
            return with(ctx, "provisioned", true);
        }
        public void audit(Map<String, Object> ctx) {
            System.out.println("   [cookbook] provisioning audited");
        }
    }

    /** 7. cb-batched-loop. */
    @Handlers("cb-batched-loop")
    public static final class BatchedLoop {
        public boolean moreBatches(Map<String, Object> ctx) {
            return ((Number) ctx.getOrDefault("batch", 0)).intValue() < 3;
        }
        public Map<String, Object> processBatch(Map<String, Object> ctx) {
            int n = ((Number) ctx.getOrDefault("batch", 0)).intValue() + 1;
            return with(ctx, "batch", n);
        }
        public Map<String, Object> finalise(Map<String, Object> ctx) {
            return with(ctx, "batchesDone", ctx.get("batch"));
        }
    }

    /** 8. cb-kitchen-sink. */
    @Handlers("cb-kitchen-sink")
    public static final class KitchenSink {
        public Map<String, Object> intake(Map<String, Object> ctx) {
            return with(ctx, "stage", "intake");
        }
        public boolean hasItems(Map<String, Object> ctx) {
            return ctx.get("items") != null && !((List<?>) ctx.get("items")).isEmpty();
        }
        public boolean isVip(Map<String, Object> ctx) {
            return Boolean.TRUE.equals(ctx.get("vip"));
        }
        public Map<String, Object> pack(Map<String, Object> ctx) {
            return with(ctx, "packed", true);
        }
        public void notice(Map<String, Object> ctx) {
            System.out.println("   [cookbook] VIP order held briefly");
        }
        public Map<String, Object> packItem(Map<String, Object> item) {
            return with(item, "packed-" + item.get("itemIndex"), true);
        }
        public void autoClear(Map<String, Object> ctx) {
            System.out.println("   [cookbook] dock auto-cleared");
        }
        public boolean moreChecks(Map<String, Object> ctx) {
            return ((Number) ctx.getOrDefault("checks", 0)).intValue() < 2;
        }
        public Map<String, Object> runCheck(Map<String, Object> ctx) {
            int n = ((Number) ctx.getOrDefault("checks", 0)).intValue() + 1;
            return with(ctx, "checks", n);
        }
        public Map<String, Object> ship(Map<String, Object> ctx) {
            return with(ctx, "stage", "shipped");
        }
    }
}
