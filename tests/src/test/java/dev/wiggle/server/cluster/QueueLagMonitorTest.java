package dev.wiggle.server.cluster;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.core.WorkflowDefinition;
import dev.wiggle.server.engine.DefinitionRegistry;
import dev.wiggle.server.engine.WorkflowEngine;
import dev.wiggle.server.store.InMemoryStorage;
import dev.wiggle.server.store.Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * White-box test (same package as {@link QueueLagMonitor}) driving its package-private
 * {@code check()} directly, so the measurement can be exercised deterministically without
 * waiting on its scheduler.
 */
class QueueLagMonitorTest {

    private final List<LogRecord> captured = new CopyOnWriteArrayList<>();
    private Handler handler;

    private void captureLogsOf(Class<?> type) {
        Logger logger = Logger.getLogger(type.getName());
        logger.setLevel(Level.ALL);
        handler = new Handler() {
            @Override public void publish(LogRecord record) { captured.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        handler.setLevel(Level.ALL);
        logger.addHandler(handler);
    }

    @AfterEach
    void removeHandler() {
        if (handler != null) Logger.getLogger(QueueLagMonitor.class.getName()).removeHandler(handler);
    }

    private boolean anyWarningContains(String needle) {
        return captured.stream().anyMatch(r -> r.getLevel() == Level.WARNING
                && r.getMessage() != null && r.getMessage().contains(needle));
    }

    private WorkflowEngine newEngine(Storage storage) {
        return new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);
    }

    private WorkflowDefinition registerLagWorkflow(WorkflowEngine engine) {
        Blueprint<Map<String, Object>> bp = Workflow.defineJson("lag-probe")
                .step("work", ctx -> ctx)   // never claimed: no worker ever polls in this test
                .build();
        return engine.definitions().register(bp.definition());
    }

    @Test @DisplayName("a stuck backlog with no throughput is logged as a WARNING")
    void warnsWhenBacklogIsNotDraining() throws Exception {
        warnsWhenBacklogIsNotDraining(new InMemoryStorage());
    }

    @Test @DisplayName("the same lag detection works against a JDBC store")
    void warnsWhenBacklogIsNotDrainingOnJdbc() throws Exception {
        warnsWhenBacklogIsNotDraining(new dev.wiggle.postgres.JdbcStorage(
                "jdbc:h2:mem:lag-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 2));
    }

    private void warnsWhenBacklogIsNotDraining(Storage newStorage) throws Exception {
        captureLogsOf(QueueLagMonitor.class);
        try (Storage storage = newStorage) {
            storage.migrate();
            WorkflowEngine engine = newEngine(storage);
            WorkflowDefinition def = registerLagWorkflow(engine);
            try (ClusterManager cluster = new ClusterManager(storage, "node-1", 4, 5000, 3)) {
                cluster.start();
                assertTrue(cluster.isLeader(), "a lone node is its own leader");

                // Pile up a backlog: nothing ever polls, so every one of these stays READY.
                for (int i = 0; i < 5; i++) engine.start(def.name(), def.version(), Map.of(), null);

                QueueLagMonitor monitor = new QueueLagMonitor(engine, cluster,
                        Duration.ofSeconds(5) /* unused: check() is driven directly */, Duration.ofMillis(50));

                monitor.check();                 // first call only establishes the baseline
                assertFalse(anyWarningContains("queue lag:"), "no warning before a measurement window has elapsed");

                Thread.sleep(120);                // longer than the 50ms warn threshold
                monitor.check();                  // now there's an elapsed window with zero throughput

                assertTrue(anyWarningContains("queue lag:"), "a non-draining backlog should warn");
            }
        }
    }

    @Test @DisplayName("an empty backlog never warns")
    void noWarningWhenBacklogIsEmpty() throws Exception {
        captureLogsOf(QueueLagMonitor.class);
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = newEngine(storage);
            try (ClusterManager cluster = new ClusterManager(storage, "node-1", 4, 5000, 3)) {
                cluster.start();

                QueueLagMonitor monitor = new QueueLagMonitor(engine, cluster,
                        Duration.ofSeconds(5), Duration.ofMillis(50));
                monitor.check();
                Thread.sleep(120);
                monitor.check();

                assertFalse(anyWarningContains("queue lag:"), "nothing queued -> nothing to warn about");
            }
        }
    }

    @Test @DisplayName("a non-leader node never checks the backlog")
    void nonLeaderDoesNotWarn() throws Exception {
        captureLogsOf(QueueLagMonitor.class);
        try (Storage storage = new InMemoryStorage()) {
            storage.migrate();
            WorkflowEngine engine = newEngine(storage);
            WorkflowDefinition def = registerLagWorkflow(engine);
            for (int i = 0; i < 5; i++) engine.start(def.name(), def.version(), Map.of(), null);

            // Two nodes: the second one, constructed (and so first-heartbeated) later, is never
            // the leader (longest-running wins). Construction order matters here, not just start().
            try (ClusterManager first = new ClusterManager(storage, "node-a", 4, 5000, 3)) {
                first.start();
                Thread.sleep(20);
                try (ClusterManager second = new ClusterManager(storage, "node-b", 4, 5000, 3)) {
                    second.start();
                    assertFalse(second.isLeader(), "the newer node follows");

                    QueueLagMonitor monitor = new QueueLagMonitor(engine, second, Duration.ofSeconds(5), Duration.ofMillis(50));
                    monitor.check();
                    Thread.sleep(120);
                    monitor.check();

                    assertFalse(anyWarningContains("queue lag:"), "a non-leader must not run the check");
                }
            }
        }
    }
}
