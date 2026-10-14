package com.wiggle.tests;

import com.wiggle.agents.AgentsDemo;
import com.wiggle.agents.Model;
import com.wiggle.agents.ResearchTeam;
import com.wiggle.agents.ScriptedModel;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.dist.WiggleStorageFactory;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.testfixtures.TestPorts;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The multi-agent example ({@code com.wiggle.agents}) run end to end against a real server: the
 * supervisor's branches, the critic loop, the editor's signal and its deadline.
 */
class ResearchTeamTest {

    private static ServerConfig config() {
        String url = "jdbc:h2:mem:agents-" + System.nanoTime() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        return new ServerConfig(TestPorts.free(), "node-0", url, "sa", "", 8,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0);
    }

    /** Starts a brief on a fresh server with {@code model} behind every agent and hands both to {@code body}. */
    private static <T> T withTeam(Model model, Duration approvalTimeout,
                                  BiFunction<WiggleClient, String, T> body) throws Exception {
        FlowSpec spec = ResearchTeam.spec(approvalTimeout);
        try (WiggleServer server = new WiggleServer(config(), new WiggleStorageFactory()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec);
            try (Worker worker = new Worker(client, "agents-" + Ids.next("x"))
                    .registerHandler(new ResearchTeam.Agents(model)).start()) {
                return body.apply(client, client.start(spec, new ResearchTeam.Brief("vector databases")));
            }
        }
    }

    private static InstanceView approveAndFinish(WiggleClient client, String id) {
        try {
            AgentsDemo.awaitEditor(client, id, Duration.ofSeconds(30));
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
        client.signal(id, ResearchTeam.APPROVAL, Map.of("decision", "approved"));
        return client.awaitCompletion(id, Duration.ofSeconds(30));
    }

    @Test @DisplayName("the supervisor's subtasks each run their specialist chain, and the combine sees every finding")
    void approvedBriefIsPublished() throws Exception {
        ScriptedModel model = new ScriptedModel();
        InstanceView v = withTeam(model, Duration.ofMinutes(5), ResearchTeamTest::approveAndFinish);

        assertEquals("COMPLETED", v.status(), String.valueOf(v.error()));
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("PUBLISHED", ctx.get("status"));
        assertEquals("approved", ctx.get("decision"));

        List<?> findings = (List<?>) ctx.get("findings");
        assertEquals(3, findings.size(), "one finding per subtask: " + findings);
        long checked = findings.stream()
                .map(f -> Json.asObject(f).get("agent"))
                .filter(ResearchTeam.FACT_CHECKER::equals).count();
        assertEquals(1, checked, "the numbers subtask ran analyst then fact checker: " + findings);

        assertEquals("revised draft", ctx.get("draft"));
        assertEquals(ResearchTeam.APPROVED, ctx.get("critique"));
        assertEquals(1, ((Number) ctx.get("revisions")).intValue());
        // supervisor 1, researcher 2, analyst 1, fact checker 1, writer 2, critic 2
        assertEquals(9, model.calls());
    }

    @Test @DisplayName("an editor who never answers shelves the brief at the deadline")
    void unansweredBriefIsShelved() throws Exception {
        InstanceView v = withTeam(new ScriptedModel(), Duration.ofMillis(300),
                (client, id) -> client.awaitCompletion(id, Duration.ofSeconds(30)));

        assertEquals("COMPLETED", v.status(), String.valueOf(v.error()));
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals("timed-out", ctx.get("decision"));
        assertEquals("SHELVED", ctx.get("status"));
    }

    @Test @DisplayName("a critic that never approves stops the loop at MAX_REVISIONS")
    void revisionLoopIsBounded() throws Exception {
        ScriptedModel scripted = new ScriptedModel();
        Model neverSatisfied = p -> p.agent().equals(ResearchTeam.CRITIC)
                ? "still not right" : scripted.complete(p);
        InstanceView v = withTeam(neverSatisfied, Duration.ofMinutes(5), ResearchTeamTest::approveAndFinish);

        assertEquals("COMPLETED", v.status(), String.valueOf(v.error()));
        Map<String, Object> ctx = Json.asObject(v.context());
        assertEquals(ResearchTeam.MAX_REVISIONS, ((Number) ctx.get("revisions")).intValue());
        assertEquals("PUBLISHED", ctx.get("status"));
    }

    @Test @DisplayName("a model call that fails is retried by the step, not lost")
    void failedModelCallIsRetried() throws Exception {
        ScriptedModel scripted = new ScriptedModel();
        AtomicBoolean failed = new AtomicBoolean();
        Model flaky = p -> {
            if (p.agent().equals(ResearchTeam.WRITER) && failed.compareAndSet(false, true)) {
                throw new IllegalStateException("429 rate limited");
            }
            return scripted.complete(p);
        };
        InstanceView v = withTeam(flaky, Duration.ofMinutes(5), ResearchTeamTest::approveAndFinish);

        assertTrue(failed.get(), "the writer's first call failed");
        assertEquals("COMPLETED", v.status(), String.valueOf(v.error()));
        assertEquals("PUBLISHED", Json.asObject(v.context()).get("status"));
    }

    @Test @DisplayName("the supervisor's reply parses leniently: list markers, unknown kinds, junk lines, a cap")
    void parseTasks() {
        List<ResearchTeam.Task> tasks = ResearchTeam.parseTasks("""
                Here is the plan
                1. search: who sells it
                - Numbers: how big is it
                opinion: is it good
                search:
                """);
        assertEquals(List.of(
                new ResearchTeam.Task("search", "who sells it"),
                new ResearchTeam.Task("numbers", "how big is it"),
                new ResearchTeam.Task("search", "is it good")), tasks);

        assertEquals(ResearchTeam.MAX_SUBTASKS,
                ResearchTeam.parseTasks("search: q\n".repeat(20)).size());
    }
}
