package com.wiggle.client.flow;

import com.wiggle.tests.Reports;
import com.wiggle.core.Json;
import com.wiggle.core.ScratchKeys;
import com.wiggle.core.TaskActivation;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * One hundred levels of fork, forEach and doWhile nested INSIDE each other, cycling through the
 * three kinds all the way down (fork at depth d≡1 (mod 3), forEach at d≡0, doWhile at d≡2), with
 * a single counting leaf at the bottom. The topology is generated recursively and driven at the
 * engine level, one claimed task at a time.
 *
 * <p>What it pins down: scope frames nest to arbitrary depth — every fork combine receives its
 * arms' views and replaces the enclosing scope, every forEach fans out over the enclosing view
 * and its combine rebuilds it, and the two designated doWhile guards (the innermost, and one
 * near the top whose second iteration re-runs ~97 levels beneath it) loop on evolved views. The
 * leaf's counter threads through every push, pop, loop and combine: 2 loops × 2 loops = exactly
 * 4 increments, sequentially accumulated.
 */
class DeepNestingTest {

    private static final int DEPTH = 100;
    /** The guards allowed to pass once (loop twice): the innermost doWhile and the outermost. */
    private static final Set<String> LOOPED = Set.of("g2", "g98");

    private int leafRuns;
    private final Map<String, Integer> guardEvals = new HashMap<>();

    @Test @DisplayName("100 levels of interleaved fork / forEach / doWhile complete, threading one counter through all of them")
    void hundredLevelsInterleaved() {
        FlowSpec bp = buildLevel(Workflow.define("deep-100", 1), DEPTH).build();
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            registry.register(bp.definition());
            Set<String> queues = bp.definition().queues();

            String id = engine.start(bp.name(), bp.version(), inputFor(DEPTH), null);

            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(60).toNanos();
            while (engine.instance(id).orElseThrow().status().equals("RUNNING")) {
                if (System.nanoTime() > deadline) throw new AssertionError("not terminal after 60s");
                for (TaskActivation t : engine.poll("w", queues, 32, null)) {
                    Reports.one(engine, t, "w", resultFor(t));
                }
            }

            assertEquals("COMPLETED", engine.instance(id).orElseThrow().status());
            assertEquals(4, leafRuns, "2 iterations of g98 x 2 of g2");
            assertEquals(4L, innermostHit(engine.instance(id).orElseThrow().context()),
                    "the counter accumulated through every level, down and back up");
            assertEquals(4, guardEvals.get("g2"), "the inner loop guard ran per outer iteration");
            assertEquals(2, guardEvals.get("g98"));
        }
    }

    @Test @DisplayName("deep nesting survives the JDBC store: join_stack grows with depth, unbounded")
    void deepNestingOnJdbc() throws Exception {
        // join_stack carries one group id per enclosing fan-out, so its LENGTH tracks nesting depth
        // -- it was VARCHAR(1000), which capped nesting near 38 levels with a driver-level "value
        // too long". Only a real store shows it; the in-memory one holds a plain String.
        String url = "jdbc:h2:mem:deep-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (com.wiggle.jdbc.JdbcStorage storage = new com.wiggle.jdbc.JdbcStorage(
                url, "sa", "", 4, new com.wiggle.postgres.H2Dialect())) {
            storage.migrate();
            DefinitionRegistry registry = new DefinitionRegistry(storage);
            WorkflowEngine engine = new WorkflowEngine(storage, registry, 30_000);
            FlowSpec bp = buildLevel(Workflow.define("deep-jdbc", 1), DEPTH).build();
            registry.register(bp.definition());
            Set<String> queues = bp.definition().queues();

            String id = engine.start(bp.name(), bp.version(), inputFor(DEPTH), null);
            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(120).toNanos();
            while (engine.instance(id).orElseThrow().status().equals("RUNNING")) {
                if (System.nanoTime() > deadline) throw new AssertionError("not terminal after 120s");
                for (TaskActivation t : engine.poll("w", queues, 32, null)) {
                    Reports.one(engine, t, "w", resultFor(t));
                }
            }
            assertEquals("COMPLETED", engine.instance(id).orElseThrow().status());
            assertEquals(4L, innermostHit(engine.instance(id).orElseThrow().context()));
        }
    }

    /** Level {@code d} wraps level {@code d-1} in the construct its depth selects; 0 is the leaf. */
    private static GraphBuilder buildLevel(GraphBuilder b, int d) {
        if (d == 0) return b.then("leaf");
        return switch (d % 3) {
            case 1 -> b.fork(
                            Branch.of("deep" + d, inner -> buildLevel(inner, d - 1)),
                            Branch.of("side" + d, inner -> inner.then("sd" + d)))
                    .combine("cfk" + d);
            case 0 -> b.forEach("fe" + d, "items", inner -> buildLevel(inner, d - 1)).combine("cfe" + d);
            default -> b.doWhile("g" + d, 5, inner -> buildLevel(inner, d - 1));
        };
    }

    /** The context each level expects: forEach levels wrap the next level in {"items": [...]};
     *  fork and doWhile levels pass the view through unchanged. */
    private static Map<String, Object> inputFor(int d) {
        if (d == 0) return new LinkedHashMap<>(Map.of("hit", 0L));
        if (d % 3 == 0) return new LinkedHashMap<>(Map.of("items", List.of(inputFor(d - 1))));
        return inputFor(d - 1);
    }

    /** What a worker would return for each activity, shape-preserving so loops can re-descend. */
    private Object resultFor(TaskActivation t) {
        String a = t.activity().substring(t.activity().indexOf('#') + 1);   // strip the flow prefix
        if (a.equals("leaf")) {
            leafRuns++;
            long hit = ((Number) Json.asObject(t.context()).get("hit")).longValue();
            return Map.of("hit", hit + 1);
        }
        if (a.startsWith("sd")) return null;                              // side arm: view untouched
        if (a.startsWith("cfk")) {                                        // fork combine: the deep arm wins
            int d = Integer.parseInt(a.substring(3));
            return Json.asObject(t.context()).get(ScratchKeys.arm("deep" + d));
        }
        if (a.startsWith("cfe")) {                                        // forEach combine: rebuild the wrapper
            int d = Integer.parseInt(a.substring(3));
            Object collected = Json.asObject(t.context()).get(ScratchKeys.forEach("fe" + d));
            return Map.of("items", List.of(Json.asArray(collected).getFirst()));
        }
        if (a.startsWith("g")) {                                          // loop guard: designated ones pass once
            int n = guardEvals.merge(a, 1, Integer::sum);
            return LOOPED.contains(a) && n % 2 == 1;
        }
        throw new AssertionError("unexpected activity " + a);
    }

    /** Digs through the rebuilt {"items": [...]} wrappers to the innermost counter. */
    private static long innermostHit(Object context) {
        Map<String, Object> m = Json.asObject(context);
        while (m.containsKey("items")) m = Json.asObject(Json.asArray(m.get("items")).getFirst());
        return ((Number) m.get("hit")).longValue();
    }
}
