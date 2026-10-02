package com.wiggle.tests;

import com.wiggle.core.Ids;
import com.wiggle.core.NodeKind;
import com.wiggle.core.TokenStatus;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.server.engine.DefinitionRegistry;
import com.wiggle.server.engine.EngineException;
import com.wiggle.server.engine.WorkflowEngine;
import com.wiggle.server.store.InMemoryStorage;
import com.wiggle.server.store.Rows.Token;
import com.wiggle.server.store.Storage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/** A worker's report on a task it cannot lock names what is missing: the task, or its instance. */
class TaskLockTest {

    private static Storage open(String backend) {
        if (backend.equals("memory")) return new InMemoryStorage();
        JdbcStorage s = new JdbcStorage("jdbc:h2:mem:task-lock-" + System.nanoTime()
                + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "", 2, new H2Dialect());
        s.migrate();
        return s;
    }

    @ParameterizedTest @ValueSource(strings = {"memory", "h2"})
    @DisplayName("an unknown task is a 404 for the task; a task whose instance is gone, for the instance")
    void missingTaskOrInstance(String backend) {
        try (Storage storage = open(backend)) {
            WorkflowEngine engine = new WorkflowEngine(storage, new DefinitionRegistry(storage), 30_000);

            EngineException noTask = assertThrows(EngineException.class,
                    () -> engine.fail("tok-missing", "w1", "boom", false));
            assertEquals(404, noTask.statusCode());
            assertEquals("task not found", noTask.getMessage());

            Token orphan = new Token();
            orphan.id = Ids.next("tok");
            orphan.instanceId = Ids.next("wfi");
            orphan.workflow = "task-lock";
            orphan.version = 1;
            orphan.nodeId = "n";
            orphan.kind = NodeKind.TASK;
            orphan.status = TokenStatus.RUNNING;
            orphan.leaseOwner = "w1";
            storage.inTxVoid(tx -> tx.insertToken(orphan));

            EngineException noInstance = assertThrows(EngineException.class,
                    () -> engine.fail(orphan.id, "w1", "boom", false));
            assertEquals(404, noInstance.statusCode());
            assertEquals("instance not found", noInstance.getMessage());
        }
    }
}
