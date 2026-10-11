package com.wiggle.core.criteria;

import com.wiggle.client.flow.FlowSpec;
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

class DispatchAttributeTest {

    interface Steps {
        Map<String, Object> a(Map<String, Object> ctx);
    }

    private static final Map<String, Object> CTX = Map.of(
            "customer", Map.of("region", "eu", "vip", true, "since", 2019L),
            "total", 250.5,
            "count", new BigDecimal("3.000"),
            "huge", new BigDecimal("1E+40"));

    @Test
    @DisplayName("WGL-QC-042/043: a path reads its value as the declared type, or leaves it absent")
    void extraction() {
        assertEquals("eu", new DispatchAttribute("region", AttrType.STRING, "customer.region").extract(CTX));
        assertEquals(true, new DispatchAttribute("vip", AttrType.BOOLEAN, "customer.vip").extract(CTX));
        assertEquals(2019L, new DispatchAttribute("since", AttrType.LONG, "customer.since").extract(CTX));
        assertEquals(3L, new DispatchAttribute("count", AttrType.LONG, "count").extract(CTX), "3.000 is a whole number");
        assertEquals(0, new BigDecimal("250.5").compareTo(
                (BigDecimal) new DispatchAttribute("total", AttrType.DECIMAL, "total").extract(CTX)));

        assertNull(new DispatchAttribute("x", AttrType.STRING, "customer.missing").extract(CTX), "unresolved");
        assertNull(new DispatchAttribute("x", AttrType.STRING, "customer.region.deeper").extract(CTX), "through a leaf");
        assertNull(new DispatchAttribute("x", AttrType.LONG, "total").extract(CTX), "a fraction is not a LONG");
        assertNull(new DispatchAttribute("x", AttrType.STRING, "customer.since").extract(CTX), "a number is not text");
        assertNull(new DispatchAttribute("x", AttrType.STRING, "customer").extract(CTX), "an object is not text");
        assertNull(new DispatchAttribute("x", AttrType.DECIMAL, "huge").extract(CTX), "too many integer digits");
        assertNull(new DispatchAttribute("x", AttrType.STRING, "s").extract(Map.of("s", "x".repeat(257))), "too long");
        assertNull(new DispatchAttribute("x", AttrType.DECIMAL, "d").extract(Map.of("d", new BigDecimal("0.00000000001"))),
                "more than 10 fraction digits");
    }

    @Test
    @DisplayName("WGL-QC-041: names, paths, duplicates and the per-flow limit are checked")
    void declarationRules() {
        assertThrows(IllegalArgumentException.class, () -> new DispatchAttribute("Region", AttrType.STRING, "r"));
        assertThrows(IllegalArgumentException.class, () -> new DispatchAttribute("region", AttrType.STRING, "a..b"));
        assertThrows(IllegalArgumentException.class, () -> DispatchAttribute.validate(List.of(
                new DispatchAttribute("r", AttrType.STRING, "a"), new DispatchAttribute("r", AttrType.LONG, "b"))));
        List<DispatchAttribute> many = new ArrayList<>();
        for (int i = 0; i <= DispatchAttribute.MAX_PER_FLOW; i++) many.add(new DispatchAttribute("a" + i, AttrType.STRING, "p"));
        assertThrows(IllegalArgumentException.class, () -> DispatchAttribute.validate(many));
    }

    @Test
    @DisplayName("WGL-QC-040: attributes are published with the graph, and a graph without them keeps its fingerprint")
    void publishedWithTheGraph() {
        FlowSpec plain = FlowSpec.define("attr-fp", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::a));
        FlowSpec declared = FlowSpec.define("attr-fp", 1, Map.class, Steps.class, (f, s) -> f
                .attribute("region", AttrType.STRING, "customer.region")
                .thenApply(s::a));
        WorkflowDefinition def = plain.definition();
        assertEquals(WorkflowDefinition.fingerprint(def.name(), def.startNode(), def.nodes().values(),
                def.executionMode(), def.checkpoints()), def.fingerprint(), "unchanged by the attributes field");
        assertNotEquals(def.fingerprint(), declared.definition().fingerprint());

        WorkflowDefinition back = WorkflowDefinition.fromJson(declared.definition().toJson());
        assertEquals(declared.definition().attributes(), back.attributes());
        assertEquals(declared.definition().fingerprint(), back.fingerprint());
    }

    @Test
    @DisplayName("WGL-QC-051: attribute literals are typed and bounded")
    void literalRules() {
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(cb -> cb.lessThan(Task_.attr("vip", Boolean.class), true)));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(new Predicate.Comparison(Field.ATTRIBUTE, "n", Op.LIKE, List.of(1L))));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(new Predicate.Comparison(Field.ATTRIBUTE, "n", Op.IN, List.of(1L, "1"))));
        assertThrows(IllegalArgumentException.class, () -> QueueCriteria.from("q")
                .where(cb -> cb.equal(Task_.attr("d", BigDecimal.class), new BigDecimal("0.00000000001"))));
        assertThrows(IllegalArgumentException.class, () -> Task_.attr("d", Double.class));
        assertThrows(IllegalArgumentException.class, () -> new Predicate.Comparison(Field.WORKFLOW, "w", Op.EQUAL, List.of("x")));
    }
}
