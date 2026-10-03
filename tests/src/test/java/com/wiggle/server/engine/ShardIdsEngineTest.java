package com.wiggle.server.engine;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.Ids;
import com.wiggle.core.ShardIds;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every id the engine mints for an instance carries that instance's shard: its tokens and the
 * sub-workflows its tokens start. An instance whose id carries no shard gets ids that carry none
 * either, so everything it owns stays with it on the home shard.
 */
class ShardIdsEngineTest {

    interface Steps {
        Map<String, Object> work(Map<String, Object> ctx);
        Map<String, Object> more(Map<String, Object> ctx);
    }


    private static FlowSpec child() {
        return FlowSpec.define("child", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    }

    private static FlowSpec parent() {
        return FlowSpec.define("parent", 1, Map.class, Steps.class, (f, s) -> f
                .thenSubFlow("delegate", "child", Map.class));
    }

    private static WorkflowEngine engine(Storage storage, InstanceIds ids) {
        storage.migrate();
        DefinitionRegistry registry = new DefinitionRegistry(storage);
        registry.register(child().definition());
        registry.register(parent().definition());
        return new WorkflowEngine(storage, registry, 30_000, ids);
    }

    /** Mints root instances bare, the way every id was minted before shards. */
    private static final InstanceIds BARE = new InstanceIds() {
        @Override public String next() { return Ids.next("wfi"); }
    };

    private static String onlyChild(Storage storage, String parentId) {
        List<String> children = storage.inTx(tx -> tx.childInstanceIds(parentId));
        assertEquals(1, children.size(), "the sub-flow node started one child");
        return children.get(0);
    }

    @Test @DisplayName("an instance, its tokens and its sub-workflow all carry the shard it was minted on")
    void everythingAnInstanceOwnsIsOnItsShard() {
        Storage storage = new InMemoryStorage();
        WorkflowEngine engine = engine(storage, InstanceIds.onShard(5));

        String parent = engine.start("parent", null, Map.of(), null);
        assertEquals(OptionalInt.of(5), ShardIds.shardOf(parent));
        engine.tokens(parent).forEach(t -> assertEquals(OptionalInt.of(5), ShardIds.shardOf(t.id), t.id));

        String child = onlyChild(storage, parent);
        assertEquals(OptionalInt.of(5), ShardIds.shardOf(child), "a sub-workflow lives with its parent");
        List<Rows.Token> childTokens = engine.tokens(child);
        assertFalse(childTokens.isEmpty());
        childTokens.forEach(t -> assertEquals(OptionalInt.of(5), ShardIds.shardOf(t.id), t.id));
    }

    @Test @DisplayName("an instance with a bare id gets bare tokens and a bare sub-workflow")
    void aBareInstanceKeepsEverythingBare() {
        Storage storage = new InMemoryStorage();
        WorkflowEngine engine = engine(storage, BARE);

        String parent = engine.start("parent", null, Map.of(), null);
        assertTrue(parent.startsWith("wfi_"), parent);
        engine.tokens(parent).forEach(t -> assertTrue(t.id.startsWith("tok_"), t.id));
        String child = onlyChild(storage, parent);
        assertTrue(child.startsWith("wfi_"), "carries no shard, so it stays on the home shard: " + child);
        engine.tokens(child).forEach(t -> assertTrue(t.id.startsWith("tok_"), t.id));
    }
}
