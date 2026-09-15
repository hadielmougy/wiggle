package com.wiggle.docs;

import com.wiggle.client.worker.CompensableActivity;
import com.wiggle.client.worker.Compensation;
import com.wiggle.docs.SagaDocSnippet.Order;
import com.wiggle.docs.SagaDocSnippet.Wms;

/** Scaffolding for {@link SagaDocHandlers}; not published. */
final class ReserveStock implements CompensableActivity<Order, Order> {

    private final Wms wms;

    ReserveStock(Wms wms) { this.wms = wms; }

    public Order execute(Order o) { return new Order(wms.reserve(o), o.status()); }

    public void compensate(Compensation<Order, Order> c) { }
}
