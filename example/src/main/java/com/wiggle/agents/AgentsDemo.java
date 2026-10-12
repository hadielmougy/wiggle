package com.wiggle.agents;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Runs the {@link ResearchTeam} in one embedded JVM against the offline {@link ScriptedModel}, then
 * approves the brief as the editor would.
 *
 * <pre>./gradlew :example:runAgents</pre>
 *
 * <p>{@code :example-claude:runAgents} and {@code :example-openai:runAgents} run the same team against
 * Claude and OpenAI.
 */
public final class AgentsDemo {

    public static void main(String[] args) throws Exception {
        run(new ScriptedModel(), args.length > 0 ? String.join(" ", args) : "vector databases");
    }

    /** Runs one brief on {@code topic} with {@code model} behind every agent, and prints it. */
    public static void run(Model model, String topic) throws Exception {
        FlowSpec spec = ResearchTeam.spec(Duration.ofMinutes(10));
        try (WiggleServer server = new WiggleServer(ServerConfig.fromEnvironment()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {
            client.register(spec);
            try (Worker worker = new Worker(client, "agents-worker")
                    .registerHandler(new ResearchTeam.Agents(model))) {
                worker.start();

                // docs:begin editor
                String id = client.start(spec, new ResearchTeam.Brief(topic));
                awaitEditor(client, id, Duration.ofMinutes(10));
                System.out.println("brief is with the editor; approving");
                client.signal(id, ResearchTeam.APPROVAL, Map.of("decision", "approved"));
                // docs:end editor

                InstanceView v = client.awaitCompletion(id, Duration.ofMinutes(1));
                System.out.println("status:  " + v.status());
                if (v.error() != null) System.out.println("error:   " + v.error());
                System.out.println("context: " + v.context());
            }
        }
    }

    /** Blocks until {@code id} is parked on the editor's approval, or fails after {@code timeout}. */
    public static void awaitEditor(WiggleClient client, String id, Duration timeout)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            WiggleClient.InstanceWithTokens detail = client.instanceDetail(id);
            if (!"RUNNING".equals(detail.instance().status())) {
                throw new IllegalStateException("instance " + id + " ended before review: "
                        + detail.instance().status() + " " + detail.instance().error());
            }
            boolean parked = detail.tokens().stream().anyMatch(t ->
                    "AWAITING".equals(t.status()) && ResearchTeam.APPROVAL.equals(t.activity()));
            if (parked) return;
            Thread.sleep(50);
        }
        throw new IllegalStateException("instance " + id + " never reached " + ResearchTeam.APPROVAL);
    }
}
