package com.wiggle.cookbook;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Eight recipes covering every operator the engine has, each one runnable:
 *
 * <pre>./gradlew :example:runCookbook</pre>
 *
 * <p><b>Notice what names what.</b> Each recipe is an <em>interface</em> declaring its steps, and a
 * class implementing them. The spec names the steps through the interface — {@code s::normalise},
 * never an implementation — because a spec does not run a step: it records the step's name, and a
 * worker supplies the code by matching that name. The {@code s} handed to the body is inert; calling
 * a method on it throws.
 *
 * <p>The implementation then declares {@code implements} that interface, so the compiler checks both
 * halves against one contract: every step the spec declares exists on the worker, with the right
 * types. Implementing it is a convenience, not a requirement — binding is by name, which is what lets
 * a step be served by another service in another language — but taking the convenience means the two
 * halves cannot drift. Recipe 1 changes context type mid-flow ({@code Signup → Classified}) and
 * neither the spec nor the handler would compile if the other disagreed.
 *
 * <p><b>And the vocabulary.</b> A fan-out is {@code Wiggle.allOf} over handles that branched from a
 * common point; an exclusive choice is {@code Wiggle.oneOf} over arms opened with {@code when} /
 * {@code otherwise}. Nothing executes while a workflow is defined — the chain is walked once and
 * recorded — which is why there is no {@code get()} on a handle, and why a {@code for} loop in a
 * definition body would unroll into nodes rather than loop at run time.
 */
public final class Cookbook {

    private Cookbook() {}

    // ---------------------------------------------------------------------------------------
    // The contexts. Records, so a step's signature says what it consumes and produces.
    // ---------------------------------------------------------------------------------------

    public record Signup(String email) {}

    /** What recipe 1 turns a {@link Signup} into -- a different type, mid-flow. */
    public record Classified(String email, boolean vip, String provisioned, String audited) {
        public Classified(String email, boolean vip) { this(email, vip, null, null); }
    }

    public record Purchase(int amount, String outcome) {}

    public record Item(String sku, long price) {}

    public record Basket(List<Item> items, long total) {}

    public record Job(int polls, boolean cancelled) {}

    public record Expense(String state) {}

    public record Batch(int done) {}

    // ---------------------------------------------------------------------------------------
    // 1. step + effect + gate -- the smallest linear pipeline with a filter, and a type change.
    // ---------------------------------------------------------------------------------------
    /** What tcb-linear-gate names; LinearWithGate implements it. */
    public interface LinearGateSteps {
        Signup normalise(Signup s);
        Classified classify(Signup s);
        boolean eligible(Classified c);
        void welcome(Classified c);
    }

    @ForFlow("tcb-linear-gate")
    public static final class LinearWithGate implements LinearGateSteps {

        public FlowSpec spec() {
            return FlowSpec.define("tcb-linear-gate", Signup.class, LinearGateSteps.class, (f, s) -> f
                    .thenApply(s::normalise)
                    // classify returns a different record, so the context type changes here; every
                    // step after it must consume Classified, and the compiler holds that
                    .thenApply(s::classify)
                    // a false gate ends the instance successfully as "gated:eligible" -- not an error
                    .thenFilter(s::eligible)
                    .thenAccept(s::welcome));
        }

        public Signup normalise(Signup s) { return new Signup(s.email().trim().toLowerCase()); }

        public Classified classify(Signup s) { return new Classified(s.email(), s.email().endsWith("wiggle.dev")); }

        public boolean eligible(Classified c) { return c.email().contains("@"); }

        public void welcome(Classified c) {
            System.out.println("   [typed] welcome email -> " + c.email() + (c.vip() ? " (vip)" : ""));
        }
    }

    // ---------------------------------------------------------------------------------------
    // 2. oneOf + allOf + retry -- an exclusive branch whose arm itself fans out.
    // ---------------------------------------------------------------------------------------
    /** What tcb-choose-fork names; ChooseThenFork implements it. */
    public interface ChooseForkSteps {
        boolean isLarge(Purchase p);
        Purchase fraudCheck(Purchase p);
        void managerNotice(Purchase p);
        Purchase largeMerge(@Context Purchase base, Purchase fraud, Purchase notice);
        Purchase fastPath(Purchase p);
        Purchase settle(Purchase p);
    }

    @ForFlow("tcb-choose-fork")
    public static final class ChooseThenFork implements ChooseForkSteps {

        public FlowSpec spec() {
            return FlowSpec.define("tcb-choose-fork", Purchase.class, ChooseForkSteps.class, (f, s) -> {
                // the large arm fans out: a fan-out inside a choice arm is just a fan-out whose
                // common point is the guard
                var large = f.when(s::isLarge);
                var fraud = large.thenApply(s::fraudCheck,
                        RetryPolicy.exponential(3, Duration.ofMillis(50)));
                var notice = large.thenAccept(s::managerNotice);
                var largeArm = Wiggle.allOf(fraud, notice).combineWithContext(s::largeMerge);

                var standard = f.otherwise().thenApply(s::fastPath);

                // both arms end at Purchase, which is what lets oneOf give back a Purchase
                return Wiggle.oneOf(largeArm, standard).thenApply(s::settle);
            });
        }

        public boolean isLarge(Purchase p) { return p.amount() >= 1000; }

        public Purchase fraudCheck(Purchase p) { return new Purchase(p.amount(), "fraud-checked"); }

        public void managerNotice(Purchase p) {
            System.out.println("   [typed] manager notified of " + p.amount());
        }

        /** One parameter per arm, in fork order; the notice arm is an effect, so it folds nothing. */
        public Purchase largeMerge(@Context Purchase base, Purchase fraud, Purchase notice) {
            return new Purchase(base.amount(), fraud.outcome());
        }

        public Purchase fastPath(Purchase p) { return new Purchase(p.amount(), "fast-path"); }

        public Purchase settle(Purchase p) { return new Purchase(p.amount(), p.outcome() + "+settled"); }
    }

    // ---------------------------------------------------------------------------------------
    // 3. forEach + per-step queue -- dynamic fan-out with mixed worker pools. The element IS each
    //    item's context, so the body's steps take an Item, not the Basket.
    // ---------------------------------------------------------------------------------------
    /** What tcb-foreach-queues names; ForEachAcrossQueues implements it. */
    public interface ForEachSteps {
        Item price(Item i);
        Item renderThumbnail(Item i);
        Basket collectItems(@Context Basket base, List<Item> priced);
        Basket summarise(Basket b);
    }

    @ForFlow("tcb-foreach-queues")
    public static final class ForEachAcrossQueues implements ForEachSteps {

        public FlowSpec spec() {
            return FlowSpec.define("tcb-foreach-queues", Basket.class, ForEachSteps.class, (f, s) -> f
                    .defaultQueue("cpu")
                    .thenForEach("items", Item.class, item -> item
                            .thenApply(s::price)
                            // only this step moves to the "gpu" queue; the default stays "cpu"
                            .thenApply(s::renderThumbnail).onQueue("gpu"))
                    .combine(s::collectItems)
                    .thenApply(s::summarise));
        }

        public Item price(Item i) { return new Item(i.sku(), i.sku().length() * 100L); }

        public Item renderThumbnail(Item i) { return new Item(i.sku() + "@2x", i.price()); }

        /** The collected item results, plus the pre-forEach context. Its return is the whole context. */
        public Basket collectItems(@Context Basket base, List<Item> priced) {
            return new Basket(List.copyOf(priced), 0L);
        }

        public Basket summarise(Basket b) {
            long total = 0;
            for (Item i : b.items()) total += i.price();
            return new Basket(b.items(), total);
        }
    }

    // ---------------------------------------------------------------------------------------
    // 4. repeatWhile + gate -- poll-until-ready, with an inner gate short-circuiting a cancelled
    //    job straight out of the loop.
    // ---------------------------------------------------------------------------------------
    /** What tcb-poll-until-ready names; PollUntilReady implements it. */
    public interface PollSteps {
        boolean notCancelled(Job j);
        Job poll(Job j);
        boolean stillPending(Job j);
        Job finish(Job j);
    }

    @ForFlow("tcb-poll-until-ready")
    public static final class PollUntilReady implements PollSteps {

        public FlowSpec spec() {
            return FlowSpec.define("tcb-poll-until-ready", Job.class, PollSteps.class, (f, s) -> f
                    // the body runs once, then the condition is evaluated -- do-while, not while-do
                    .repeatWhile(s::stillPending, b -> b
                            // a gate short-circuits to the loop's exit, not just the body: a
                            // cancellation ends the whole instance here
                            .thenFilter(s::notCancelled)
                            .thenApply(s::poll))
                    .thenApply(s::finish));
        }

        public boolean notCancelled(Job j) { return !j.cancelled(); }

        public Job poll(Job j) { return new Job(j.polls() + 1, j.cancelled()); }

        public boolean stillPending(Job j) { return j.polls() < 3; }

        public Job finish(Job j) { return j; }
    }

    // ---------------------------------------------------------------------------------------
    // 5. thenAwait (timeout + escalation) + oneOf -- branch on how the wait resolved.
    // ---------------------------------------------------------------------------------------
    /** What tcb-approval-escalation names; ApprovalWithEscalation implements it. */
    public interface ApprovalSteps {
        Expense submit(Expense e);
        Expense autoEscalate(Expense e);
        boolean wasEscalated(Expense e);
        void notifyDirector(Expense e);
        void notifySubmitter(Expense e);
    }

    @ForFlow("tcb-approval-escalation")
    public static final class ApprovalWithEscalation implements ApprovalSteps {

        public FlowSpec spec() {
            return FlowSpec.define("tcb-approval-escalation", Expense.class, ApprovalSteps.class, (f, s) -> {
                var waited = f
                        .thenApply(s::submit)
                        // no worker is held while it waits; if nobody signals in time the
                        // escalation branch runs instead, then rejoins here
                        .thenAwait("manager-approval", Duration.ofMillis(200),
                                esc -> esc.thenApply(s::autoEscalate));

                var escalated = waited.when(s::wasEscalated).thenAccept(s::notifyDirector);
                var approved = waited.otherwise().thenAccept(s::notifySubmitter);

                return Wiggle.oneOf(escalated, approved);
            });
        }

        public Expense submit(Expense e) { return new Expense("submitted"); }

        public Expense autoEscalate(Expense e) { return new Expense("escalated"); }

        public boolean wasEscalated(Expense e) { return "escalated".equals(e.state()); }

        public void notifyDirector(Expense e) { System.out.println("   [typed] escalated to director"); }

        public void notifySubmitter(Expense e) { System.out.println("   [typed] approval relayed"); }
    }

    // ---------------------------------------------------------------------------------------
    // 6. thenSubFlow + gate + allOf -- compose a registered child workflow into a bigger one.
    // ---------------------------------------------------------------------------------------
    /** What tcb-parent names; ChildCheckThenFork implements it. */
    public interface ParentSteps {
        boolean childPassed(Classified c);
        Classified provision(Classified c);
        void audit(Classified c);
        Classified merge(@Context Classified base, Classified provisioned, Classified audited);
    }

    @ForFlow("tcb-parent")
    public static final class ChildCheckThenFork implements ParentSteps {

        public FlowSpec spec() {
            return FlowSpec.define("tcb-parent", Signup.class, ParentSteps.class, (f, s) -> {
                var checked = f
                        // runs tcb-linear-gate as a child; its final context merges back here, which
                        // is why this continues as Classified
                        .thenSubFlow("run-eligibility", "tcb-linear-gate", Classified.class)
                        .thenFilter(s::childPassed);

                var provision = checked.thenApply(s::provision);
                var audit = checked.thenAccept(s::audit);

                return Wiggle.allOf(provision, audit).combineWithContext(s::merge);
            });
        }

        public boolean childPassed(Classified c) { return c.email() != null; }

        public Classified provision(Classified c) {
            return new Classified(c.email(), c.vip(), "hw-" + c.email().hashCode(), c.audited());
        }

        public void audit(Classified c) { System.out.println("   [typed] provisioning audited"); }

        public Classified merge(@Context Classified base, Classified provisioned, Classified audited) {
            return new Classified(base.email(), base.vip(), provisioned.provisioned(), "audited");
        }
    }

    // ---------------------------------------------------------------------------------------
    // 7. execution(LOCAL_ASYNC) + checkpoint + repeatWhile -- batched local execution with a
    //    deliberate commit point, so a crash mid-loop only replays the current iteration.
    // ---------------------------------------------------------------------------------------
    /** What tcb-batched-loop names; BatchedLoopWithCheckpoint implements it. */
    public interface BatchedSteps {
        Batch processBatch(Batch b);
        boolean moreBatches(Batch b);
        Batch finalise(Batch b);
    }

    @ForFlow("tcb-batched-loop")
    public static final class BatchedLoopWithCheckpoint implements BatchedSteps {

        public FlowSpec spec() {
            return FlowSpec.define("tcb-batched-loop", Batch.class, BatchedSteps.class, (f, s) -> f
                    .execution(ExecutionMode.LOCAL_ASYNC)
                    .repeatWhile(s::moreBatches, b -> b
                            .thenApply(s::processBatch)
                            .checkpoint())   // flush the buffer before the next iteration
                    .thenApply(s::finalise));
        }

        public Batch processBatch(Batch b) { return new Batch(b.done() + 1); }

        public boolean moreBatches(Batch b) { return b.done() < 3; }

        public Batch finalise(Batch b) { return b; }
    }

    // ---------------------------------------------------------------------------------------
    // 8. Everything at once -- gate, sub-workflow, oneOf whose arms fan out and fan over a
    //    collection, sleep, signal + escalation, loop, checkpoint, queues. Not idiomatic; a
    //    deliberate stress test of the combination space.
    // ---------------------------------------------------------------------------------------
    /** What tcb-kitchen-sink names; KitchenSink implements it. */
    public interface KitchenSinkSteps {
        Basket intake(Basket b);
        boolean hasItems(Basket b);
        boolean isVip(Basket b);
        Basket pack(Basket b);
        void notice(Basket b);
        Basket priorityMerge(@Context Basket base, Basket packed, Basket held);
        Item packItem(Item i);
        Basket collectPacked(@Context Basket base, List<Item> items);
        Basket autoClear(Basket b);
        Basket runCheck(Basket b);
        boolean moreChecks(Basket b);
        Basket ship(Basket b);
    }

    @ForFlow("tcb-kitchen-sink")
    public static final class KitchenSink implements KitchenSinkSteps {

        public FlowSpec spec() {
            return FlowSpec.define("tcb-kitchen-sink", Basket.class, KitchenSinkSteps.class, (f, s) -> {
                var ready = f
                        .defaultQueue("default")
                        .execution(ExecutionMode.LOCAL_SYNC)
                        .thenApply(s::intake)
                        .thenFilter(s::hasItems);

                var vip = ready.when(s::isVip);
                var packed = vip.thenApply(s::pack,
                        RetryPolicy.fixed(2, Duration.ofMillis(20)), "packing");
                var held = vip.thenSleep("brief-hold", Duration.ofMillis(50)).thenAccept(s::notice);
                var vipArm = Wiggle.allOf(packed, held).combineWithContext(s::priorityMerge);

                var standard = ready.otherwise()
                        .thenForEach("pack-items", "items", Item.class, item -> item.thenApply(s::packItem))
                        .combine(s::collectPacked);

                return Wiggle.oneOf(vipArm, standard)
                        .thenAwait("dock-clear", Duration.ofMillis(150), esc -> esc.thenApply(s::autoClear))
                        .repeatWhile(s::moreChecks, b -> b.thenApply(s::runCheck).checkpoint())
                        .thenApply(s::ship);
            });
        }

        public Basket intake(Basket b) { return b; }

        public boolean hasItems(Basket b) { return b.items() != null && !b.items().isEmpty(); }

        public boolean isVip(Basket b) { return b.items().size() > 2; }

        public Basket pack(Basket b) { return new Basket(b.items(), b.total() + 1); }

        public void notice(Basket b) { System.out.println("   [typed] VIP basket held briefly"); }

        public Basket priorityMerge(@Context Basket base, Basket packed, Basket held) {
            return new Basket(base.items(), packed.total());
        }

        public Item packItem(Item i) { return new Item(i.sku() + "-packed", i.price()); }

        public Basket collectPacked(@Context Basket base, List<Item> items) {
            return new Basket(List.copyOf(items), base.total());
        }

        public Basket autoClear(Basket b) { return b; }

        public Basket runCheck(Basket b) { return new Basket(b.items(), b.total() + 10); }

        public boolean moreChecks(Basket b) { return b.total() % 100 < 20; }

        public Basket ship(Basket b) {
            List<Item> shipped = new ArrayList<>(b.items());
            return new Basket(shipped, b.total() + 1000);
        }
    }
}