package com.wiggle.cookbook;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.Handlers;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@link Cookbook} recipes again, written with {@link Wiggle#define} instead of
 * {@link Wiggle#graph} — the same eight operator combinations, the same engine behaviour, a
 * different way of saying it. Run them side by side to compare:
 *
 * <pre>
 *   ./gradlew :example:runCookbook        # Wiggle.graph — topology by name
 *   ./gradlew :example:runTypedCookbook   # Wiggle.define — topology by method reference
 * </pre>
 *
 * <p><b>Notice the file layout.</b> The graph cookbook is two files — {@code Cookbook} holds the
 * topologies and {@code CookbookHandlers} the logic, because naming steps as strings is what lets
 * them be written apart. Here each recipe is <em>one class</em> that is both: the steps are method
 * references to its own methods, so the compiler checks that every step consumes what the one before
 * it produced. Recipe 1 changes context type mid-flow ({@code Signup → Classified}) and the chain
 * simply would not compile if a later step disagreed.
 *
 * <p><b>And the vocabulary.</b> A fan-out is {@code Wiggle.allOf} over handles that branched from a
 * common point; an exclusive choice is {@code Wiggle.oneOf} over arms opened with {@code when} /
 * {@code otherwise}. Nothing executes while a workflow is defined — the chain is walked once and
 * recorded — which is why there is no {@code get()} on a handle, and why a {@code for} loop in a
 * definition body would unroll into nodes rather than loop at run time.
 */
public final class TypedCookbook {

    private TypedCookbook() {}

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
    @Handlers("tcb-linear-gate")
    public static final class LinearWithGate {

        public FlowSpec spec() {
            return Wiggle.define("tcb-linear-gate", Signup.class, f -> f
                    .thenApply(this::normalise)
                    // classify returns a different record, so the context type changes here; every
                    // step after it must consume Classified, and the compiler holds that
                    .thenApply(this::classify)
                    // a false gate ends the instance successfully as "gated:eligible" -- not an error
                    .thenFilter(this::eligible)
                    .thenAccept(this::welcome));
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
    @Handlers("tcb-choose-fork")
    public static final class ChooseThenFork {

        public FlowSpec spec() {
            return Wiggle.define("tcb-choose-fork", Purchase.class, f -> {
                // the large arm fans out: a fan-out inside a choice arm is just a fan-out whose
                // common point is the guard
                var large = f.when(this::isLarge);
                var fraud = large.thenApply(this::fraudCheck,
                        RetryPolicy.exponential(3, Duration.ofMillis(50)));
                var notice = large.thenAccept(this::managerNotice);
                var largeArm = Wiggle.allOf(fraud, notice).combineWithContext(this::largeMerge);

                var standard = f.otherwise().thenApply(this::fastPath);

                // both arms end at Purchase, which is what lets oneOf give back a Purchase
                return Wiggle.oneOf(largeArm, standard).thenApply(this::settle);
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
    @Handlers("tcb-foreach-queues")
    public static final class ForEachAcrossQueues {

        public FlowSpec spec() {
            return Wiggle.define("tcb-foreach-queues", Basket.class, f -> f
                    .defaultQueue("cpu")
                    .thenForEach("items", Item.class, item -> item
                            .thenApply(this::price)
                            // only this step moves to the "gpu" queue; the default stays "cpu"
                            .thenApply(this::renderThumbnail).onQueue("gpu"))
                    .combine(this::collectItems)
                    .thenApply(this::summarise));
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
    @Handlers("tcb-poll-until-ready")
    public static final class PollUntilReady {

        public FlowSpec spec() {
            return Wiggle.define("tcb-poll-until-ready", Job.class, f -> f
                    // the body runs once, then the condition is evaluated -- do-while, not while-do
                    .repeatWhile(this::stillPending, b -> b
                            // a gate short-circuits to the loop's exit, not just the body: a
                            // cancellation ends the whole instance here
                            .thenFilter(this::notCancelled)
                            .thenApply(this::poll))
                    .thenApply(this::finish));
        }

        public boolean notCancelled(Job j) { return !j.cancelled(); }

        public Job poll(Job j) { return new Job(j.polls() + 1, j.cancelled()); }

        public boolean stillPending(Job j) { return j.polls() < 3; }

        public Job finish(Job j) { return j; }
    }

    // ---------------------------------------------------------------------------------------
    // 5. thenAwait (timeout + escalation) + oneOf -- branch on how the wait resolved.
    // ---------------------------------------------------------------------------------------
    @Handlers("tcb-approval-escalation")
    public static final class ApprovalWithEscalation {

        public FlowSpec spec() {
            return Wiggle.define("tcb-approval-escalation", Expense.class, f -> {
                var waited = f
                        .thenApply(this::submit)
                        // no worker is held while it waits; if nobody signals in time the
                        // escalation branch runs instead, then rejoins here
                        .thenAwait("manager-approval", Duration.ofMillis(200),
                                esc -> esc.thenApply(this::autoEscalate));

                var escalated = waited.when(this::wasEscalated).thenAccept(this::notifyDirector);
                var approved = waited.otherwise().thenAccept(this::notifySubmitter);

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
    @Handlers("tcb-parent")
    public static final class ChildCheckThenFork {

        public FlowSpec spec() {
            return Wiggle.define("tcb-parent", Signup.class, f -> {
                var checked = f
                        // runs tcb-linear-gate as a child; its final context merges back here, which
                        // is why this continues as Classified
                        .thenSubFlow("run-eligibility", "tcb-linear-gate", Classified.class)
                        .thenFilter(this::childPassed);

                var provision = checked.thenApply(this::provision);
                var audit = checked.thenAccept(this::audit);

                return Wiggle.allOf(provision, audit).combineWithContext(this::merge);
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
    @Handlers("tcb-batched-loop")
    public static final class BatchedLoopWithCheckpoint {

        public FlowSpec spec() {
            return Wiggle.define("tcb-batched-loop", Batch.class, f -> f
                    .execution(ExecutionMode.LOCAL_ASYNC)
                    .repeatWhile(this::moreBatches, b -> b
                            .thenApply(this::processBatch)
                            .checkpoint())   // flush the buffer before the next iteration
                    .thenApply(this::finalise));
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
    @Handlers("tcb-kitchen-sink")
    public static final class KitchenSink {

        public FlowSpec spec() {
            return Wiggle.define("tcb-kitchen-sink", Basket.class, f -> {
                var ready = f
                        .defaultQueue("default")
                        .execution(ExecutionMode.LOCAL_SYNC)
                        .thenApply(this::intake)
                        .thenFilter(this::hasItems);

                var vip = ready.when(this::isVip);
                var packed = vip.thenApply(this::pack,
                        RetryPolicy.fixed(2, Duration.ofMillis(20)), "packing");
                var held = vip.thenSleep("brief-hold", Duration.ofMillis(50)).thenAccept(this::notice);
                var vipArm = Wiggle.allOf(packed, held).combineWithContext(this::priorityMerge);

                var standard = ready.otherwise()
                        .thenForEach("pack-items", "items", Item.class, item -> item.thenApply(this::packItem))
                        .combine(this::collectPacked);

                return Wiggle.oneOf(vipArm, standard)
                        .thenAwait("dock-clear", Duration.ofMillis(150), esc -> esc.thenApply(this::autoClear))
                        .repeatWhile(this::moreChecks, b -> b.thenApply(this::runCheck).checkpoint())
                        .thenApply(this::ship);
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