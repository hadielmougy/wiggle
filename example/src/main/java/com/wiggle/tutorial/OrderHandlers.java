package com.wiggle.tutorial;

import com.wiggle.client.worker.Context;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.tutorial.Orders.Item;
import com.wiggle.tutorial.Orders.Order;
import com.wiggle.tutorial.Orders.OrderSteps;

import java.math.BigDecimal;
import java.util.List;

/** The handlers the tutorial writes; see {@link Orders} for why this lives as source. */
// docs:begin handlers
@ForFlow("orders")                                  // which flow these steps belong to
public class OrderHandlers implements OrderSteps {  // implementing the contract is optional,
                                                    // but then the compiler checks every signature

    @Override public Order validate(Order o) {
        if (o.items().isEmpty()) throw new IllegalArgumentException("empty order " + o.id());
        return o.withStatus("VALIDATED");
    }

    @Override public boolean inStock(Order o) {
        return o.items().size() <= 50;              // false ends the instance cleanly, not as a failure
    }

    @Override public Item price(Item item) {
        return new Item(item.sku(), item.price().multiply(new BigDecimal("1.20")));   // + VAT
    }

    @Override public Order total(@Context Order base, List<Item> priced) {
        return base.withTotal(priced.stream().map(Item::price)
                .reduce(BigDecimal.ZERO, BigDecimal::add));
    }

    @Override public Order confirm(Order o) {
        return o.withStatus("CONFIRMED");
    }
}
// docs:end handlers
