package com.wiggle.tests;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.core.ExecutionMode;
import com.wiggle.core.Json;
import com.wiggle.core.WorkflowDefinition;
import com.wiggle.jdbc.JdbcStorage;
import com.wiggle.postgres.H2Dialect;
import com.wiggle.proto.ProtoJson;
import com.wiggle.proto.WiggleControlPlaneGrpc;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What remains of OBSERVED execution after its removal: a definition stored under it still loads, a
 * new one is refused, and the migration closes the runs it left open and drops its schema.
 */
class ObservedRemovalTest {

    interface Steps { Map<String, Object> work(Map<String, Object> ctx); }

    private static FlowSpec spec() {
        return FlowSpec.define("was-observed", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    }

    @Test @DisplayName("a definition stored as OBSERVED loads as DEFAULT")
    void aStoredObservedDefinitionLoads() {
        Map<String, Object> json = spec().definition().toJson();
        json.put("executionMode", "OBSERVED");
        WorkflowDefinition loaded = WorkflowDefinition.fromJson(Json.parse(Json.write(json)));
        assertEquals(ExecutionMode.DEFAULT, loaded.executionMode());
        assertEquals(spec().definition().nodes().keySet(), loaded.nodes().keySet());
    }

    @Test @DisplayName("registering an OBSERVED definition is refused, naming the removal")
    void registeringObservedIsRefused() throws Exception {
        ServerConfig config = new ServerConfig(TestPorts.free(), "obs-gone", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
        try (WiggleServer server = new WiggleServer(config).start()) {
            // an older client's register call, with the mode as it would send it
            Map<String, Object> json = spec().definition().toJson();
            json.put("executionMode", "OBSERVED");
            ManagedChannel channel = Grpc.newChannelBuilder(server.baseUrl(), InsecureChannelCredentials.create()).build();
            try {
                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () ->
                        WiggleControlPlaneGrpc.newBlockingStub(channel).registerWorkflow(
                                com.wiggle.proto.WorkflowDefinition.newBuilder()
                                        .setDefinition(ProtoJson.toStruct(json)).build()));
                assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
                assertTrue(e.getStatus().getDescription().contains("OBSERVED"), e.getStatus().getDescription());
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test @DisplayName("migration 26 cancels an observed run left open and drops what OBSERVED stored")
    void migrationClosesOpenRuns() throws Exception {
        String url = "jdbc:h2:mem:obs-removal-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (Connection c = DriverManager.getConnection(url, "sa", "")) {
            c.setAutoCommit(false);
            JdbcStorage.runMigrations(c, JdbcStorage.MIGRATIONS.subList(0, 25), new H2Dialect());
            c.commit();
            try (Statement s = c.createStatement()) {
                s.execute("INSERT INTO wf_instance (id,workflow,version,status,context,created_at,updated_at,revision,"
                        + "settle_at) VALUES ('wfo_open','w',1,'RUNNING','{}',1,1,0,99)");
                s.execute("INSERT INTO wf_instance (id,workflow,version,status,context,created_at,updated_at,revision) "
                        + "VALUES ('wfi_live','w',1,'RUNNING','{}',1,1,0)");
            }
            c.commit();
            JdbcStorage.runMigrations(c, JdbcStorage.MIGRATIONS, new H2Dialect());
            c.commit();
            try (Statement s = c.createStatement()) {
                assertEquals("CANCELLED", status(s, "wfo_open"), "the open observed run is closed");
                assertEquals("RUNNING", status(s, "wfi_live"), "an ordinary instance is untouched");
                assertFalse(column(c, "WF_INSTANCE", "SETTLE_AT"));
                assertFalse(column(c, "WF_TOKEN", "SEQ"));
                try (ResultSet rs = c.getMetaData().getTables(null, null, "WF_ANOMALY", null)) {
                    assertFalse(rs.next(), "the anomaly table is gone");
                }
            }
        }
    }

    private static String status(Statement s, String id) throws Exception {
        try (ResultSet rs = s.executeQuery("SELECT status FROM wf_instance WHERE id='" + id + "'")) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static boolean column(Connection c, String table, String column) throws Exception {
        try (ResultSet rs = c.getMetaData().getColumns(null, null, table, column)) {
            return rs.next();
        }
    }
}
