package com.wiggle.client.flow;

import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The forEach collection named by the context's own accessor: the key comes off the reference and the
 * element type off its return type, so no {@code Class<E>} witness is needed. Each test compiling at
 * all is half the assertion -- the body handler takes {@code Item}, which only type-checks if the
 * element type was inferred.
 */
class ForEachAccessorTest {

    record Item(String sku, int qty) {}

    record Basket(List<Item> items, Map<String, Item> itemsBySku, Item[] spares,
                  List<Item> inStockItems) {}

    interface S {
        Item price(Item i);
        Item reprice(Item i);
        Basket collect(List<Item> priced);
        Basket recollect(List<Item> priced);
    }

    /** A fan-out node stores its key raw; the paired combine stores it JSON-encoded. */
    private static String keyOf(WorkflowDefinition d, String nodeName) {
        Node n = d.nodes().values().stream()
                .filter(x -> nodeName.equals(x.name()) && x.kind() == NodeKind.DYN_FORK)
                .findFirst().orElseThrow(() -> new AssertionError("no forEach node named " + nodeName));
        return n.itemsKey();
    }

    @Test @DisplayName("a list accessor gives both the key and the element type")
    void listAccessor() {
        WorkflowDefinition d = FlowSpec.define("fa-list", 1, Basket.class, S.class, (f, s) ->
                f.thenForEach(Basket::items, item -> item.thenApply(s::price))
                        .combine(s::collect)).definition();
        assertEquals("items", keyOf(d, "items"));
        assertTrue(d.nodes().values().stream().anyMatch(n -> "price".equals(n.name())));
    }

    @Test @DisplayName("a map accessor fans out over the values")
    void mapAccessor() {
        WorkflowDefinition d = FlowSpec.define("fa-map", 1, Basket.class, S.class, (f, s) ->
                f.thenForEach(Basket::itemsBySku, item -> item.thenApply(s::price))
                        .combine(s::collect)).definition();
        assertEquals("itemsBySku", keyOf(d, "itemsBySku"));
    }

    @Test @DisplayName("an array accessor works the same -- a JSON array either way")
    void arrayAccessor() {
        WorkflowDefinition d = FlowSpec.define("fa-array", 1, Basket.class, S.class, (f, s) ->
                f.thenForEach(Basket::spares, item -> item.thenApply(s::price))
                        .combine(s::collect)).definition();
        assertEquals("spares", keyOf(d, "spares"));
    }

    @Test @DisplayName("the explicit-name form still fans the same collection twice")
    void sameCollectionTwice() {
        WorkflowDefinition d = FlowSpec.define("fa-twice", 1, Basket.class, S.class, (f, s) -> {
            var once = f.thenForEach("first-pass", Basket::items, i -> i.thenApply(s::price))
                    .combine(s::collect);
            return once.thenForEach("second-pass", Basket::items, i -> i.thenApply(s::reprice))
                    .combine(s::recollect);
        }).definition();
        assertEquals("items", keyOf(d, "first-pass"));
        assertEquals("items", keyOf(d, "second-pass"));
    }

    @Test @DisplayName("the key is the component name exactly -- never folded the way a step name is")
    void keyIsNotFolded() {
        // as a step name inStockItems would bind to a node named in-stock-items; a key must stay exact,
        // because the engine does context.get(itemsKey) with no folding
        assertEquals("inStockItems", StepNames.ofKey((FlowItems<Basket, Item>) Basket::inStockItems));
    }

    @Test @DisplayName("a lambda cannot name a key -- there is no component behind it")
    void lambdaRejected() {
        FlowItems<Basket, Item> lambda = b -> b.items();
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> StepNames.ofKey(lambda));
        assertTrue(e.getMessage().contains("direct accessor reference"), e.getMessage());
    }

    /** Stands in for any object that could supply the collection from outside the context. */
    static final class Elsewhere {
        List<Item> itemsOf(Basket b) { return b.items(); }
    }

    @Test @DisplayName("an accessor bound to a captured instance is refused -- the key comes from the type")
    void boundAccessorRejected() {
        Elsewhere other = new Elsewhere();
        FlowItems<Basket, Item> bound = other::itemsOf;   // captures `other`
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> StepNames.ofKey(bound));
        assertTrue(e.getMessage().contains("captured value"), e.getMessage());
    }
}
