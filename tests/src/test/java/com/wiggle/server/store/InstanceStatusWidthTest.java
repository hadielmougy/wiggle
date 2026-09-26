package com.wiggle.server.store;

import com.wiggle.core.InstanceStatus;
import com.wiggle.server.store.Rows.Instance;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Every instance status has to survive a round trip through a real database. wf_instance.status was
 * VARCHAR(16) and COMPENSATION_FAILED is nineteen characters, so the one status a saga reaches when
 * a compensator runs out of retries could not be written at all: the update failed, its transaction
 * rolled back, and the instance stayed COMPENSATING with the undo retrying forever. Only JDBC
 * deployments hit it, because the in-memory store holds the enum -- which is why the whole suite
 * passed. These cases fail on the old column width.
 */
class InstanceStatusWidthTest {

    @Test
    @DisplayName("every status survives an insert, an update and a read back")
    void everyStatusRoundTrips() throws Exception {
        withJdbc(storage -> {
            for (InstanceStatus s : InstanceStatus.values()) {
                String id = "wfi_rt_" + s.name();
                storage.inTxVoid(tx -> tx.insertInstance(instance(id, InstanceStatus.RUNNING)));
                storage.inTxVoid(tx -> {
                    Instance i = tx.lockInstance(id).orElseThrow();
                    i.status = s;
                    tx.updateInstance(i);
                });
                InstanceStatus back = storage.inTx(tx -> tx.findInstance(id).orElseThrow().status);
                assertEquals(s, back, s + " did not survive the round trip");
            }
        });
    }

    @Test
    @DisplayName("the JDBC purge drops exactly the statuses the enum calls terminal")
    void jdbcPurgeAgreesWithTheEnum() throws Exception {
        withJdbc(storage -> {
            for (InstanceStatus s : InstanceStatus.values()) {
                storage.inTxVoid(tx -> tx.insertInstance(instance("wfi_" + s.name(), s)));
            }
            storage.inTxVoid(tx -> tx.deleteTerminalInstancesBefore(1_000, 100));
            Set<String> survived = Arrays.stream(InstanceStatus.values())
                    .filter(s -> storage.inTx(tx -> tx.findInstance("wfi_" + s.name()).isPresent()))
                    .map(Enum::name).collect(Collectors.toSet());
            Set<String> expected = Arrays.stream(InstanceStatus.values())
                    .filter(InstanceStatus::live).map(Enum::name).collect(Collectors.toSet());
            assertEquals(expected, survived, "the purge query and InstanceStatus.live() disagree");
        });
    }

    private interface Body { void run(Storage storage) throws Exception; }

    private static void withJdbc(Body body) throws Exception {
        String url = "jdbc:h2:mem:width-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (com.wiggle.jdbc.JdbcStorage storage = new com.wiggle.jdbc.JdbcStorage(
                url, "sa", "", 2, new com.wiggle.postgres.H2Dialect())) {
            storage.migrate();
            body.run(storage);
        }
    }

    private static Instance instance(String id, InstanceStatus status) {
        Instance i = new Instance();
        i.id = id;
        i.workflow = "w";
        i.version = 1;
        i.status = status;
        i.createdAt = 0;
        i.updatedAt = 0;
        return i;
    }
}
