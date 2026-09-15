package com.wiggle.docs.decode;

import com.wiggle.client.worker.Decode;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.core.RecordMapper;
import com.wiggle.docs.onboarding.OnboardingSnippet.Order;

import java.util.Map;

/** The {@code @Decode} upcast quoted in {@code docs/onboarding.md}. */
// docs:begin decode
@ForFlow("order-fulfilment")
class OrderHandlers {
    @Decode
    public Order load(Map<String, Object> raw) {     // upcast an older shape to the current Order
        raw.putIfAbsent("currency", "USD");           // e.g. default a field added in a later version
        return (Order) RecordMapper.fromJson(raw, Order.class);
    }
    // docs:elide     // ... step methods, which now receive the upcast Order ...
}
// docs:end decode
