package com.wiggle.docs;

import com.wiggle.client.worker.ForFlow;
import com.wiggle.docs.ApprovalSnippet.Expense;

/** The handler half of <a href="https://wiggle.sh/patterns/approval/">wiggle.sh/patterns/approval</a>. */
// docs:begin handlers
@ForFlow("expense-approval")
class ExpenseHandlers {
    // docs:skip
    private Mail mail;

    interface Mail { void director(Expense e); }
    // docs:resume

    public Expense submit(Expense e)         { return e.withState("PENDING_APPROVAL"); }

    // deadline branch: nobody acted within 48h
    public Expense autoEscalate(Expense e)   { return e.escalated(true); }

    public boolean wasEscalated(Expense e)   { return e.isEscalated(); }   // choose guard
    public void    notifyDirector(Expense e) { mail.director(e); }         // effect: no state change
    public Expense payOut(Expense e)         { return e.withState("PAID"); }
}
// docs:end handlers
