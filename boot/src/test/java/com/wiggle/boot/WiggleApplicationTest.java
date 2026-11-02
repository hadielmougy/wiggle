package com.wiggle.boot;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.testfixtures.TestPorts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The Boot application serves the API on {@code WIGGLE_PORT}, over a store built from the same settings. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class WiggleApplicationTest {

    private static final int PORT = TestPorts.free();

    @DynamicPropertySource
    static void settings(DynamicPropertyRegistry registry) {
        registry.add("WIGGLE_PORT", () -> PORT);
        registry.add("WIGGLE_NODE_NAME", () -> "boot-test");
    }

    @Autowired ServerConfig config;

    public interface GreetSteps {
        Map<String, Object> sayHello(Map<String, Object> ctx);
    }

    public static class GreetHandlers implements GreetSteps {
        @Override public Map<String, Object> sayHello(Map<String, Object> ctx) {
            Map<String, Object> next = new HashMap<>(ctx);
            next.put("greeting", "hello, " + ctx.get("name"));
            return next;
        }
    }

    @Test @DisplayName("settings come from Spring's environment by their WIGGLE_* names")
    void settingsByEnvironmentName() {
        assertEquals(PORT, config.port());
        assertEquals("boot-test", config.nodeName());
    }

    @Test @DisplayName("a workflow runs end to end through the API on Boot's Tomcat")
    void runsAWorkflow() throws Exception {
        @SuppressWarnings("unchecked")
        FlowSpec greet = FlowSpec.define("boot-greet", 1, Map.class, GreetSteps.class, (f, s) -> f.thenApply(s::sayHello));
        try (WiggleClient client = new WiggleClient("127.0.0.1:" + PORT);
             Worker worker = new Worker(client, "boot-worker").registerHandler("boot-greet", new GreetHandlers())) {
            client.register(greet);
            worker.start();
            String id = client.start(greet, Map.of("name", "ada"));
            InstanceView done = client.awaitCompletion(id, Duration.ofSeconds(15));
            assertEquals("COMPLETED", done.status());
            assertEquals("hello, ada", Json.asObject(done.context()).get("greeting"));
        }
    }

    @Test @DisplayName("an unknown method is 404 and any verb but POST 405, as on the node's own server")
    void routing() throws Exception {
        try (HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            String base = "http://127.0.0.1:" + PORT;
            assertEquals(404, http.send(HttpRequest.newBuilder(URI.create(base + "/v1/Nope"))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString()).statusCode());
            HttpResponse<String> get = http.send(HttpRequest.newBuilder(URI.create(base + "/v1/HealthCheck")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(405, get.statusCode());
            assertEquals("POST", get.headers().firstValue("Allow").orElse(null));
        }
    }
}
