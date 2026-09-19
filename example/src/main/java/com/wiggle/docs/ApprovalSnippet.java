package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;

import java.time.Duration;
import java.util.Map;

/** The code on <a href="https://wiggle.sh/patterns/approval/">wiggle.sh/patterns/approval</a>. */
public final class ApprovalSnippet {

    public record Expense(String state, boolean escalated) {
        Expense withState(String s) { return new Expense(s, escalated); }
        Expense escalated(boolean e) { return new Expense(state, e); }
        boolean isEscalated() { return escalated; }
    }

    // docs:begin contract
    interface ExpenseSteps {
        Expense submit(Expense e);
        Expense autoEscalate(Expense e);
        boolean wasEscalated(Expense e);
        void    notifyDirector(Expense e);
        Expense payOut(Expense e);
    }
    // docs:end contract

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec approval = FlowSpec.define("expense-approval", 1, Expense.class, ExpenseSteps.class, (f, s) -> {
            var waited = f.thenApply(s::submit)
                    .thenAwait("manager-approval", Duration.ofHours(48),
                            esc -> esc.thenApply(s::autoEscalate));   // runs only if the deadline passes

            // exactly one of these runs; both arms end at Expense, which is what lets oneOf return one
            var escalated = waited.when(s::wasEscalated).thenAccept(s::notifyDirector);
            var approved  = waited.otherwise().thenApply(s::payOut);

            return Wiggle.oneOf(escalated, approved);
        });
        // docs:end topology
        return approval;
    }

    static void approve(WiggleClient client, String instanceId) {
        // docs:begin signal
        client.signal(instanceId, "manager-approval", Map.of("decision", "approved", "by", "sam"));
        // docs:end signal
    }

    private ApprovalSnippet() {}
}
