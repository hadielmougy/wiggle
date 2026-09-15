package com.wiggle.docs;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Context;

import java.math.BigDecimal;
import java.util.List;

/**
 * The code on <a href="https://wiggle.sh/patterns/fan-out/">wiggle.sh/patterns/fan-out</a>. See
 * {@link SagaSnippet} for why these pages draw their Java from compiled source.
 *
 * <p>Compiling it settled a disagreement the page had with itself: the contract said
 * {@code Item price(Item)} while the handlers said {@code Priced price(LineItem)}, and the combine
 * took two different list types on the two halves of the same example. Only one of those can be
 * right, and nothing had to decide until now.
 */
public final class FanOutSnippet {

    public record LineItem(String sku, BigDecimal amount) {}
    public record Priced(String sku, BigDecimal amount) {}

    public record Order(List<LineItem> items, List<Priced> priced, BigDecimal rate,
                        BigDecimal total, String status) {
        Order withItems(List<Priced> p) { return new Order(items, p, rate, total, status); }
        Order withTotal(BigDecimal t) { return new Order(items, priced, rate, t, status); }
        Order withStatus(String s) { return new Order(items, priced, rate, total, s); }
    }

    // docs:begin contract
    interface PricingSteps {
        Order  loadOrder(Order o);
        Priced price(LineItem line);                 // the element IS each branch's context
        Order  collect(@Context Order base, List<Priced> priced);
        Order  summarise(Order o);
    }
    // docs:end contract

    static FlowSpec define() {
        // docs:begin topology
        FlowSpec pricing = FlowSpec.define("price-order", Order.class, PricingSteps.class, (f, s) -> f
                .thenApply(s::loadOrder)
                .thenForEach(Order::items,                   // one branch per element of Order.items
                        item -> item.thenApply(s::price))
                .combine(s::collect)                         // receives the collected results
                .thenApply(s::summarise));
        // docs:end topology
        return pricing;
    }

    private FanOutSnippet() {}
}
