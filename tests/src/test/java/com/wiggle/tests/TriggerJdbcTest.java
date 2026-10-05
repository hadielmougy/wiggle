package com.wiggle.tests;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.PostgresStorageFactory;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Triggers on the SQL store: H2 by default, the configured database under {@code WIGGLE_TEST_DB_URL}.
 * Two engines over one database stand in for two leaders overlapping during a failover.
 */
class TriggerJdbcTest {

    interface OneStep {
        Map<String, Object> work(Map<String, Object> ctx);
    }

    private static Storage open() {
        String url = TestStorage.url("triggers");
        Storage storage = new JdbcStorage(url, TestStorage.user(), TestStorage.password(), 8,
                PostgresStorageFactory.dialect(url));
        storage.migrate();
        return storage;
    }

    private static WorkflowEngine engine(Storage storage) {
        return new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
    }

    private static FlowSpec flow(String name) {
        return FlowSpec.define(name, 1, Map.class, OneStep.class, (f, s) -> f.thenApply(s::work));
    }

    @Test @DisplayName("two overlapping leaders dispatching at once start each triggered instance exactly once")
    void overlappingLeadersFireOnce() throws Exception {
        String run = Long.toHexString(System.nanoTime());
        String source = "trgj-src-" + run, target = "trgj-dst-" + run;
        try (Storage storage = open()) {
            WorkflowEngine a = engine(storage), b = engine(storage);
            a.register(flow(source).definition());
            a.register(flow(target).definition());
            String trigger = a.createTrigger(target, source, List.of("wf.cancelled"), true);
            try {
                List<String> sources = new ArrayList<>();
                for (int i = 0; i < 40; i++) {
                    String id = a.start(source, null, Map.of("n", i), null);
                    a.cancel(id, "test");
                    sources.add(id);
                }
                Thread.sleep(80);

                CountDownLatch go = new CountDownLatch(1);
                try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
                    List<Future<?>> leaders = new ArrayList<>();
                    for (WorkflowEngine leader : List.of(a, b)) {
                        leaders.add(pool.submit(() -> {
                            go.await();
                            for (int round = 0; round < 20; round++) leader.dispatchTriggers(7);
                            return null;
                        }));
                    }
                    go.countDown();
                    for (Future<?> f : leaders) f.get();
                }
                Thread.sleep(80);
                a.dispatchTriggers(1_000);

                List<InstanceView> started = a.list(target, null, 1_000);
                Set<String> fired = new HashSet<>();
                for (InstanceView v : started) {
                    Map<?, ?> t = (Map<?, ?>) Json.asObject(v.context()).get("trigger");
                    fired.add((String) t.get("instanceId"));
                }
                assertEquals(sources.size(), started.size(), "one start per cancel, none twice");
                assertEquals(new HashSet<>(sources), fired, "and every cancel fired");
            } finally {
                a.deleteTrigger(trigger);
            }
        }
    }
}