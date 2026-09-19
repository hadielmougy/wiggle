package com.wiggle.tests;

import com.wiggle.core.Json;
import com.wiggle.core.Node;
import com.wiggle.core.NodeKind;
import com.wiggle.core.WorkflowDefinition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A graph registered before combines had typed fields carried the shape as JSON inside itemsKey.
 * Those blobs are still in databases, so the decode has to keep understanding them.
 */
class LegacyCombineTest {

    private static Map<String, Object> legacyTask(String id, String itemsKeyJson) {
        return Map.of("id", id, "kind", "TASK", "name", id, "activity", "wf#" + id,
                "itemsKey", itemsKeyJson);
    }

    @Test @DisplayName("a legacy fork combine's JSON-array itemsKey decodes into arm names")
    void legacyForkCombine() {
        Node n = Node.fromJson(legacyTask("merge", Json.write(List.of("air", "hotel"))));
        assertEquals(List.of("air", "hotel"), n.armNames());
        assertNull(n.collectKey());
        assertNull(n.itemsKey(), "the legacy encoding is consumed, not left behind");
        assertTrue(n.isCombine());
    }

    @Test @DisplayName("a legacy forEach combine's JSON-string itemsKey decodes into a collect key")
    void legacyForEachCombine() {
        Node n = Node.fromJson(legacyTask("collect", Json.write("__forEach__items")));
        assertEquals("__forEach__items", n.collectKey());
        assertEquals(List.of(), n.armNames());
        assertNull(n.itemsKey());
        assertTrue(n.isCombine());
    }

    @Test @DisplayName("a DYN_FORK's itemsKey is a plain context key and is left alone")
    void dynForkKeyIsNotACombine() {
        Node n = Node.fromJson(Map.of("id", "fan", "kind", "DYN_FORK", "name", "fan",
                "itemsKey", "items", "itemKey", "item"));
        assertEquals("items", n.itemsKey());
        assertEquals(List.of(), n.armNames());
        assertNull(n.collectKey());
        assertTrue(!n.isCombine());
    }

    @Test @DisplayName("a current combine round-trips through JSON on the typed fields")
    void currentCombineRoundTrips() {
        Node fork = Node.task("merge", "merge", "wf#merge", "q", null).withArmNames(List.of("a", "b"));
        Node back = Node.fromJson(fork.toJson());
        assertEquals(List.of("a", "b"), back.armNames());
        assertEquals(NodeKind.TASK, back.kind());

        Node each = Node.task("collect", "collect", "wf#collect", "q", null).withCollectKey("__forEach__xs");
        assertEquals("__forEach__xs", Node.fromJson(each.toJson()).collectKey());
    }

    @Test @DisplayName("the legacy and typed encodings describe the same graph to the engine")
    void legacyAndTypedAgree() {
        Node typed = Node.task("merge", "merge", "wf#merge", "q", null).withArmNames(List.of("air", "hotel"));
        Node legacy = Node.fromJson(legacyTask("merge", Json.write(List.of("air", "hotel"))));
        assertEquals(typed.armNames(), legacy.armNames());
        assertEquals(typed.isCombine(), legacy.isCombine());
    }

    @Test @DisplayName("a definition blob written in the legacy encoding still loads")
    void legacyDefinitionBlob() {
        Map<String, Object> blob = Map.of(
                "name", "trip", "version", 7L, "startNode", "merge",
                "nodes", List.of(legacyTask("merge", Json.write(List.of("air", "hotel")))),
                "queues", List.of("q"), "executionMode", "SERVER");
        WorkflowDefinition def = WorkflowDefinition.fromJson(blob);
        assertEquals(7, def.version());
        assertEquals(List.of("air", "hotel"), def.node("merge").armNames());
    }
}
