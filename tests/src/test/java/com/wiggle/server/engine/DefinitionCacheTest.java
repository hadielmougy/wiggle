package com.wiggle.server.engine;

import com.wiggle.core.Node;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.server.store.GraphStore;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The registry holds small definitions whole, so the drive loop reads their nodes without a query.
 * Every property below is invisible to the rest of the suite: a cache that silently stops
 * populating, or silently serves the wrong graph, changes latency and correctness without failing
 * a single behavioural test. Each case here is a way it has already gone wrong.
 *
 * <p>Membership is observed rather than inspected. A handle is asked for a node while its
 * {@link GraphStore} is one that throws on contact, so reaching the store at all is visible:
 * {@link StoreTouched} means the definition was not served from memory.
 */
class DefinitionCacheTest {

    /** Raised by {@link #REFUSES} when a handle falls through to the store. */
    private static final class StoreTouched extends RuntimeException {
        StoreTouched(String method) {
            super("the store was consulted for " + method + "; this definition should have been "
                    + "served from memory");
        }
    }

    /** A store that cannot be used, only caught using. */
    private static final GraphStore REFUSES = new GraphStore() {
        @Override public void putDefinition(String n, int v, String j, String f, String a) {
            throw new StoreTouched("putDefinition");
        }
        @Override public void replaceDefinition(String n, int v, String j, String f, String a) {
            throw new StoreTouched("replaceDefinition");
        }
        @Override public Optional<String> definition(String n, int v) {
            throw new StoreTouched("definition");
        }
        @Override public Optional<StoredFingerprint> definitionFingerprint(String n, int v) {
            throw new StoreTouched("definitionFingerprint");
        }
        @Override public Optional<Integer> latestVersion(String n) {
            throw new StoreTouched("latestVersion");
        }
        @Override public List<String> definitionNames() {
            throw new StoreTouched("definitionNames");
        }
        @Override public void putGraph(WorkflowDefinition d) {
            throw new StoreTouched("putGraph");
        }
        @Override public void deleteGraph(String w, int v) {
            throw new StoreTouched("deleteGraph");
        }
        @Override public Optional<Node> graphNode(String w, int v, String id) {
            throw new StoreTouched("graphNode");
        }
        @Override public Optional<String> graphStartNode(String w, int v) {
            throw new StoreTouched("graphStartNode");
        }
        @Override public int graphNodeCount(String w, int v) {
            throw new StoreTouched("graphNodeCount");
        }
    };

    @Test
    @DisplayName("a process restarting against an existing database still caches the definition")
    void aRestartedRegistryStillCaches() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            new DefinitionRegistry(storage).register(def("orders", 1, "a"));

            // A second registry over the same database is what a restarted process looks like: the
            // graph is already stored, so registration is a no-op in the store. The cache must fill
            // anyway, or it would only ever populate on a database's very first write -- which is
            // to say, never again after the first deploy.
            DefinitionRegistry restarted = new DefinitionRegistry(storage);
            restarted.register(def("orders", 1, "a"));

            assertEquals("a", restarted.graph(REFUSES, "orders", 1).node("step").activity());
        }
    }

    @Test
    @DisplayName("a forced replacement refreshes the cached definition rather than keeping the old one")
    void forcedReplacementRefreshesTheCache() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            registry.register(def("orders", 1, "before"));
            registry.register(def("orders", 1, "after"), true);

            assertEquals("after", registry.graph(REFUSES, "orders", 1).node("step").activity(),
                    "the replaced graph must win; a stale entry would serve the old one forever");
        }
    }

    @Test
    @DisplayName("two registries never share a cache, even for the same name and version")
    void registriesDoNotShareACache() {
        try (Storage one = new InMemoryStorage(); Storage two = new InMemoryStorage()) {
            one.migrate();
            two.migrate();
            DefinitionRegistry first = new DefinitionRegistry(one);
            DefinitionRegistry second = new DefinitionRegistry(two);
            first.register(def("orders", 1, "from-first"));
            second.register(def("orders", 1, "from-second"));

            assertEquals("from-first", first.graph(REFUSES, "orders", 1).node("step").activity());
            assertEquals("from-second", second.graph(REFUSES, "orders", 1).node("step").activity(),
                    "a shared cache would serve the other database's graph under the same key");
        }
    }

    @Test
    @DisplayName("a rejected registration leaves the cache holding the graph that is actually stored")
    void aRejectedRegistrationDoesNotCache() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            registry.register(def("orders", 1, "stored"));

            assertThrows(EngineException.class, () -> registry.register(def("orders", 1, "rejected")));

            assertEquals("stored", registry.graph(REFUSES, "orders", 1).node("step").activity(),
                    "a registration that never committed must not reach the cache");
        }
    }

    @Test
    @DisplayName("a node that did not register a small definition holds it after first reading it")
    void anotherNodeFillsTheCacheOnFirstUse() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            new DefinitionRegistry(storage).register(def("orders", 1, "a"));
            DefinitionRegistry other = new DefinitionRegistry(storage);
            assertEquals("a", storage.inTx(tx -> other.graph(tx, "orders", 1).node("step").activity()));
            assertEquals("a", other.graph(REFUSES, "orders", 1).node("step").activity(),
                    "from then on it reads nothing from the store for this graph");
            assertThrows(StoreTouched.class, () -> other.graph(REFUSES, "unknown", 1).node("step"),
                    "a version it has never read is not invented");
        }
    }

    @Test
    @DisplayName("a definition past the node limit is left to the one-node-at-a-time path")
    void anOversizedDefinitionIsNotCached() {
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            registry.register(chain("big", 1, 200));
            DefinitionRegistry other = new DefinitionRegistry(storage);
            assertEquals("act-1", storage.inTx(tx -> other.graph(tx, "big", 1).node("n1").activity()),
                    "a node that did not register it counts its nodes, and stays on the lazy path");
            assertThrows(StoreTouched.class, () -> other.graph(REFUSES, "big", 1).node("n0"),
                    "having counted once, it does not count again, and never holds the graph whole");

            LazyGraph uncached = registry.graph(REFUSES, "big", 1);
            assertThrows(StoreTouched.class, () -> uncached.node("n0"),
                    "a large graph must not be materialised whole just to advance one token");

            assertEquals("act-0", storage.inTx(tx -> registry.graph(tx, "big", 1).node("n0").activity()),
                    "and it must still resolve correctly against a real store");
        }
    }

    /** One TASK node named {@code step}, carrying {@code activity} so a graph is identifiable. */
    private static WorkflowDefinition def(String name, int version, String activity) {
        Map<String, Node> nodes = new LinkedHashMap<>();
        nodes.put("step", Node.task("step", "step", activity, "q", null).withNext("end"));
        nodes.put("end", Node.end("end", true, null));
        return new WorkflowDefinition(name, version, "step", nodes, Set.of("q"));
    }

    /** A chain of {@code length} task nodes -- past the registry's cacheable size. */
    private static WorkflowDefinition chain(String name, int version, int length) {
        Map<String, Node> nodes = new LinkedHashMap<>();
        for (int i = 0; i < length; i++) {
            String next = i == length - 1 ? "end" : "n" + (i + 1);
            nodes.put("n" + i, Node.task("n" + i, "n" + i, "act-" + i, "q", null).withNext(next));
        }
        nodes.put("end", Node.end("end", true, null));
        return new WorkflowDefinition(name, version, "n0", nodes, Set.of("q"));
    }
}
