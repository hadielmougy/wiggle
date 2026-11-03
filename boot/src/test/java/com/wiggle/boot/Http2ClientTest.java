package com.wiggle.boot;

import com.wiggle.boot.test.WiggleBootTest;
import com.wiggle.boot.test.WiggleTestNode;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A client on {@code WIGGLE_HTTP_VERSION=HTTP_2} talks h2c to a node, however large the call that opens a connection. */
@WiggleBootTest
class Http2ClientTest {

    @Autowired WiggleTestNode node;

    public interface Steps {
        Map<String, Object> touch(Map<String, Object> ctx);
    }

    public static final class Handlers implements Steps {
        @Override public Map<String, Object> touch(Map<String, Object> ctx) {
            Map<String, Object> next = new HashMap<>(ctx);
            next.put("touched", true);
            return next;
        }
    }

    @BeforeEach void http2() { System.setProperty("wiggle.http.version", "HTTP_2"); }

    @AfterEach void restore() { System.clearProperty("wiggle.http.version"); }

    @Test @DisplayName("a call larger than Tomcat's default upgrade buffer opens the connection, and a workflow runs")
    void largeFirstCallThenAWorkflow() throws Exception {
        @SuppressWarnings("unchecked")
        FlowSpec flow = FlowSpec.define("h2-flow", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::touch));
        Map<String, Object> large = Map.of("pad", "x".repeat(64 * 1024));
        try (WiggleClient client = node.client();
             Worker worker = new Worker(client, "h2-worker").registerHandler("h2-flow", new Handlers())) {
            WiggleClient.WiggleApiException unknown = org.junit.jupiter.api.Assertions.assertThrows(
                    WiggleClient.WiggleApiException.class, () -> client.start("no-such-flow", large));
            assertEquals(404, unknown.status(), "the large call that opened the connection was served, not refused");
            client.register(flow);
            worker.start();
            String id = client.start(flow, large);
            assertEquals("COMPLETED", client.awaitCompletion(id, Duration.ofSeconds(15)).status());
        }
    }
}
