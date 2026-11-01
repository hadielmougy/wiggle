package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.AttributeFilter;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.criteria.AttrType;

import java.util.List;

/** The code quoted in {@code docs/onboarding.md}. See {@link SagaSnippet} for why these live as source. */
public final class SearchAttributesSnippet {

    static FlowSpec declare() {
        // docs:begin declare
        FlowSpec orders = FlowSpec.define("regional-orders", 1, QueuesSnippet.RegionalOrder.class,
                QueuesSnippet.RegionalSteps.class, (f, s) -> f
                .searchAttribute("region", AttrType.STRING, "customer.region")   // read from the instance's context
                .searchAttribute("total", AttrType.DECIMAL, "total")
                .thenApply(s::validate)
                .thenApply(s::charge, "payments"));
        // docs:end declare
        return orders;
    }

    static void query(WiggleClient client) {
        // docs:begin query
        // EU orders of 1000 or more: the attributes need the workflow, the words are optional
        WiggleClient.SearchResult r = client.search("", "regional-orders", null, null, null, 50, false, List.of(
                AttributeFilter.eq("region", "eu"),
                AttributeFilter.ge("total", 1000)));
        // docs:end query
    }

    private SearchAttributesSnippet() {}
}
