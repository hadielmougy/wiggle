package com.wiggle.order;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Branch;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Worker;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;

import java.time.Duration;
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

    public static void main(String[] args) throws Exception {
        // Default the dashboard on (the whole point of this tool) unless the caller set a port.
        if (System.getProperty("wiggle.dashboard.port") == null && System.getenv("WIGGLE_DASHBOARD_PORT") == null) {
            System.setProperty("wiggle.dashboard.port", "8090");
        }
        ServerConfig config = ServerConfig.fromEnvironment();

        Blueprint kyc = Workflow.define("kyc-checks")
                .step("verify-id")
                .step("risk-score")
                .build();

        Blueprint onboarding = Workflow.define("onboarding")
                .step("create-account")
                .fork(
                        Branch.of("send-welcome", b -> b.step("welcome")),
                        Branch.of("provision", b -> b.step("provision-hw")))
                .combine("merge")
                .subWorkflow("run-kyc", "kyc-checks")
                .awaitSignal("manager-approval", Duration.ofHours(48),
                        b -> b.step("auto-escalate"))
                .step("activate")
                .build();

        Blueprint report = Workflow.define("nightly-report")
                .step("gather")
                .step("render")
                .build();

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "seed-worker")
                     .register(onboarding).register(kyc).register(report)
                     .handlers(new OnboardingHandlers())
                     .handlers(new KycHandlers())
                     .handlers(new NightlyReportHandlers())) {
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
