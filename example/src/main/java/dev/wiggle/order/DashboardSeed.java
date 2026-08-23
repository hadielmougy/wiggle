package dev.wiggle.order;

import dev.wiggle.client.dsl.Blueprint;
import dev.wiggle.client.dsl.Branch;
import dev.wiggle.client.dsl.Workflow;
import dev.wiggle.client.worker.WiggleClient;
import dev.wiggle.client.worker.Worker;
import dev.wiggle.server.ServerConfig;
import dev.wiggle.server.WiggleServer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A single-JVM playground for the web dashboard: it starts a server with the dashboard on
 * :8090, registers workflows that exercise every node kind, seeds a completed run, two runs
 * parked on a signal, and a couple of schedules, then idles so you can explore the UI.
 *
 * <pre>WIGGLE_DASHBOARD_PORT=8090 ./gradlew :example:seedDashboard   →   http://localhost:8090</pre>
 *
 * Every tab has something to see: Instances (with a live trace), Workflows (diagrams),
 * Signals (two pending approvals), Schedules (one cron, one interval).
 *
 * <p>Config comes from the environment ({@link ServerConfig#fromEnvironment()}), so the same
 * playground can run in-memory (the default) or against a database ({@code WIGGLE_JDBC_URL}),
 * and can be secured with {@code WIGGLE_DASHBOARD_PASSWORD} / TLS just like a real deployment.
 */
public final class DashboardSeed {

    private static Map<String, Object> put(Map<String, Object> c, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(c);
        n.put(k, v);
        return n;
    }

    public static void main(String[] args) throws Exception {
        // Default the dashboard on (the whole point of this tool) unless the caller set a port.
        if (System.getProperty("wiggle.dashboard.port") == null && System.getenv("WIGGLE_DASHBOARD_PORT") == null) {
            System.setProperty("wiggle.dashboard.port", "8090");
        }
        ServerConfig config = ServerConfig.fromEnvironment();

        Blueprint<Map<String, Object>> kyc = Workflow.defineJson("kyc-checks")
                .step("verify-id", ctx -> put(ctx, "idOk", true))
                .step("risk-score", ctx -> put(ctx, "risk", 12))
                .build();

        Blueprint<Map<String, Object>> onboarding = Workflow.defineJson("onboarding")
                .step("create-account", ctx -> put(ctx, "accountId", "acc-42"))
                .fork(
                        Branch.of("send-welcome", b -> b.step("welcome", ctx -> put(ctx, "welcomed", true))),
                        Branch.of("provision", b -> b.step("provision-hw", ctx -> put(ctx, "provisioned", true))))
                .subWorkflow("run-kyc", "kyc-checks")
                .awaitSignal("manager-approval", Duration.ofHours(48),
                        b -> b.step("auto-escalate", ctx -> put(ctx, "escalated", true)))
                .step("activate", ctx -> put(ctx, "active", true))
                .build();

        Blueprint<Map<String, Object>> report = Workflow.defineJson("nightly-report")
                .step("gather", ctx -> put(ctx, "rows", 128))
                .step("render", ctx -> put(ctx, "done", true))
                .build();

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "seed-worker")
                     .register(onboarding).register(kyc).register(report)) {
            client.register(kyc);
            client.register(report);
            worker.start();

            client.start(report, Map.of("source", "seed"));           // completes
            client.start(onboarding, Map.of("email", "a@example.com")); // parks on the signal
            client.start(onboarding, Map.of("email", "b@example.com")); // parks on the signal

            // Two different workflows so both cadences show (a workflow has at most one schedule).
            client.createCronSchedule("nightly-report", "0 3 * * *", Map.of("source", "cron"));
            client.createSchedule("kyc-checks", Duration.ofHours(6), Map.of("source", "timer"));

            System.out.println("\nDashboard seeded — open http://localhost:" + server.dashboardPort());
            System.out.println("Two 'onboarding' instances are parked on the 'manager-approval' signal.");
            System.out.println("Press Ctrl-C to stop.\n");
            Thread.currentThread().join();
        }
    }
}
