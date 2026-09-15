package com.wiggle.docs;

import com.wiggle.client.worker.ForFlow;
import com.wiggle.docs.RetriesSnippet.Ctx;
import com.wiggle.docs.RetriesSnippet.Order;

/** The handler lines quoted on <a href="https://wiggle.sh/patterns/retries/">wiggle.sh/patterns/retries</a>. */
@ForFlow("retried")
class RetriesHandlers {
    // No docs:skip here, unlike the other handler fixtures: this page quotes individual methods
    // rather than the whole class, so the scaffolding is outside every region already.
    private Api api;

    interface Api { String check(String ref); }

    // docs:begin gate-handler
    public boolean inStock(Order o) { return o.quantity() > 0; }
    // docs:end gate-handler

    // docs:begin poll-handlers
    public boolean stillPending(Ctx c)  { return !c.ready(); }      // loop condition, after each pass
    public boolean notCancelled(Ctx c)  { return !c.cancelled(); }
    public Ctx     poll(Ctx c)          { return c.withStatus(api.check(c.ref())); }
    // docs:end poll-handlers
}
