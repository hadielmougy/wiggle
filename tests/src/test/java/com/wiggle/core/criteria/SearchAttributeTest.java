package com.wiggle.core.criteria;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Json;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SearchAttributeTest {

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
    }

    private static final Map<String, Object> CTX = Map.of(
            "customer", Map.of("region", "eu", "vip", true),
            "total", 250.5,
            "count", new BigDecimal("3.000"));

    @Test
    @DisplayName("a path reads its value as the declared type, or leaves it absent")
    void extraction() {
        assertEquals("eu", new SearchAttribute("region", AttrType.STRING, "customer.region").extract(CTX));
        assertEquals(true, new SearchAttribute("vip", AttrType.BOOLEAN, "customer.vip").extract(CTX));
        assertEquals(3L, new SearchAttribute("count", AttrType.LONG, "count").extract(CTX));
        assertNull(new SearchAttribute("total", AttrType.LONG, "total").extract(CTX), "250.5 is not a whole number");
        assertNull(new SearchAttribute("region", AttrType.LONG, "customer.region").extract(CTX), "text is not a number");
        assertNull(new SearchAttribute("gone", AttrType.STRING, "customer.gone").extract(CTX));
        assertNull(new SearchAttribute("deep", AttrType.STRING, "total.deeper").extract(CTX), "a path through a non-object");
        assertEquals(Map.of("region", "eu"), SearchAttribute.extractAll(
                List.of(new SearchAttribute("region", AttrType.STRING, "customer.region"),
                        new SearchAttribute("gone", AttrType.STRING, "gone")), CTX));
    }

    @Test
    @DisplayName("names, paths and the number a flow declares are checked")
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> new SearchAttribute("Region", AttrType.STRING, "a"));
        assertThrows(IllegalArgumentException.class, () -> new SearchAttribute("region", AttrType.STRING, "a..b"));
        assertThrows(IllegalArgumentException.class, () -> new SearchAttribute("region", null, "a"));
        SearchAttribute a = new SearchAttribute("a", AttrType.STRING, "a");
        assertThrows(IllegalArgumentException.class, () -> SearchAttribute.validate(List.of(a, a)), "declared twice");
        List<SearchAttribute> many = new ArrayList<>();
        for (int i = 0; i <= SearchAttribute.MAX_PER_FLOW; i++) many.add(new SearchAttribute("a" + i, AttrType.STRING, "a"));
        assertThrows(IllegalArgumentException.class, () -> SearchAttribute.validate(many));
    }

    private static FlowSpec flow(boolean declare) {
        return FlowSpec.define("orders", 1, Map.class, Steps.class, (f, s) -> declare
                ? f.searchAttribute("region", AttrType.STRING, "customer.region")
                        .searchAttribute("total", AttrType.DECIMAL, "total").thenApply(s::a)
                : f.thenApply(s::a));
    }

    @Test
    @DisplayName("a flow declares them; they are part of the version and survive the wire")
    void inTheDefinition() {
        WorkflowDefinition declared = flow(true).definition();
        assertEquals(List.of(new SearchAttribute("region", AttrType.STRING, "customer.region"),
                new SearchAttribute("total", AttrType.DECIMAL, "total")), declared.searchAttributes());
        assertEquals(declared.searchAttributes(),
                WorkflowDefinition.fromJson(Json.parse(Json.canonical(declared.toJson()))).searchAttributes());
        assertNotEquals(flow(false).definition().fingerprint(), declared.fingerprint(),
                "changing the declaration is a new version, not a re-registration");
        assertEquals(List.of(), flow(false).definition().searchAttributes());
        assertEquals(flow(false).definition().fingerprint(),
                WorkflowDefinition.fingerprint("orders", flow(false).definition().startNode(),
                        flow(false).definition().nodes().values(), flow(false).definition().executionMode(),
                        flow(false).definition().checkpoints()),
                "a flow with none digests exactly as it did before they existed");
    }
}
