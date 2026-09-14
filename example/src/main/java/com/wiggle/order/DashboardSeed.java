package com.wiggle.order;

import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.flow.Wiggle;
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

    interface KycSteps {
        Map<String, Object> verifyId(Map<String, Object> ctx);
        Map<String, Object> riskScore(Map<String, Object> ctx);
    }

    interface OnboardingSteps {
        Map<String, Object> createAccount(Map<String, Object> ctx);
        Map<String, Object> welcome(Map<String, Object> ctx);
        Map<String, Object> provisionHw(Map<String, Object> ctx);
        // the handler wants the pre-fork context too, so this is a combineWithContext shape
        Map<String, Object> merge(Map<String, Object> base,
                                  Map<String, Object> welcome, Map<String, Object> provisioned);
        Map<String, Object> autoEscalate(Map<String, Object> ctx);
        Map<String, Object> activate(Map<String, Object> ctx);
    }

    interface ReportSteps {
        Map<String, Object> gather(Map<String, Object> ctx);
        Map<String, Object> render(Map<String, Object> ctx);
    }

    public static void main(String[] args) throws Exception {
        // Default the dashboard on (the whole point of this tool) unless the caller set a port.
        if (System.getProperty("wiggle.dashboard.port") == null && System.getenv("WIGGLE_DASHBOARD_PORT") == null) {
            System.setProperty("wiggle.dashboard.port", "8090");
        }
        ServerConfig config = ServerConfig.fromEnvironment();

        FlowSpec kyc = FlowSpec.define("kyc-checks", Map.class, KycSteps.class, (f, s) -> f
                .thenApply(s::verifyId)
                .thenApply(s::riskScore));

        FlowSpec onboarding = FlowSpec.define("onboarding", Map.class, OnboardingSteps.class, (f, s) -> {
            var created = f.thenApply(s::createAccount);
            return Wiggle.allOf(created.thenApply(s::welcome), created.thenApply(s::provisionHw))
                    .combineWithContext(s::merge)
                    .thenSubFlow("run-kyc", "kyc-checks", Map.class)
                    .thenAwait("manager-approval", Duration.ofHours(48),
                            b -> b.thenApply(s::autoEscalate))
                    .thenApply(s::activate);
        });

        FlowSpec report = FlowSpec.define("nightly-report", Map.class, ReportSteps.class, (f, s) -> f
                .thenApply(s::gather)
                .thenApply(s::render));

        try (WiggleServer server = new WiggleServer(config).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker worker = new Worker(client, "seed-worker")
                     .registerHandler(new OnboardingHandlers())
                     .registerHandler(new KycHandlers())
                     .registerHandler(new NightlyReportHandlers())) {
            // the author publishes all three; the worker only implements their steps
            client.register(kyc);
            client.register(onboarding);
            client.register(report);
            worker.start();

            client.start(report, Map.of("source", "seed"));           // completes
            client.start(onboarding, Map.of("email", "a@example.com")); // parks on the signal
            client.start(onboarding, Map.of("email", "b@example.com")); // parks on the signal

            // Two different workflows so both cadences show (a workflow has at most one schedule).
            client.createCronSchedule("nightly-report", "0 3 * * *", Map.of("source", "cron"));
            client.createSchedule("kyc-checks", Duration.ofHours(6), Map.of("source", "timer"));

            System.out.println("\nData seeded on the server at " + server.baseUrl() + ".");
            System.out.println("Explore it in the ops console (a separate process):");
            System.out.println("    WIGGLE_URL=" + server.baseUrl() + " ./gradlew :console:run   ->   http://localhost:8090");
            System.out.println("Two 'onboarding' instances are parked on the 'manager-approval' signal.");
            System.out.println("Press Ctrl-C to stop.\n");
            Thread.currentThread().join();
        }
    }
}
