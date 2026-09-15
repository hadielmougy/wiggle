package com.wiggle.docs;

import com.wiggle.docs.FanOutSnippet.PricingSteps;
import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Step;
import com.wiggle.docs.FanOutSnippet.LineItem;
import com.wiggle.docs.FanOutSnippet.Order;
import com.wiggle.docs.FanOutSnippet.Priced;

import java.math.BigDecimal;
import java.util.List;

/** The handler half of <a href="https://wiggle.sh/patterns/fan-out/">wiggle.sh/patterns/fan-out</a>. */
// docs:begin handlers
@ForFlow("price-order")
class PricingHandlers implements PricingSteps {
    // docs:skip
    private Repo repo;

    interface Repo { Order load(Order o); }
    // docs:resume

    public Order loadOrder(Order o) { return repo.load(o); }

    // The parameter IS the element — the element is the item's context. Scalars work too.
    public Priced price(LineItem line) {
        Order base = Step.base(Order.class);     // frozen pre-forEach context, read-only
        return new Priced(line.sku(), base.rate().multiply(line.amount()));
    }

    // The engine collects each item's FINAL value: List in order for a list input,
    // Map keyed like the input for a map input. You fold explicitly.
    public Order collect(@Context Order base, List<Priced> priced) {
        return base.withItems(priced)
                   .withTotal(priced.stream().map(Priced::amount)
                           .reduce(BigDecimal.ZERO, BigDecimal::add));
    }

    public Order summarise(Order o) { return o.withStatus("PRICED"); }
}
// docs:end handlers
