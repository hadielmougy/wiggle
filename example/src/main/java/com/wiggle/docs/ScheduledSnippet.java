package com.wiggle.docs;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;

import java.time.Duration;
import java.util.Map;

/** The code on <a href="https://wiggle.sh/patterns/scheduled/">wiggle.sh/patterns/scheduled</a>. */
public final class ScheduledSnippet {

    public record Report(String scope, String rendered) {}

    // docs:begin contract
    interface ReportSteps {
        Report gather(Report r);
        Report render(Report r);
        void   distribute(Report r);
    }
    // docs:end contract

    static void schedule(WiggleClient client) {
        // docs:begin topology
        FlowSpec report = FlowSpec.define("nightly-report", Report.class, ReportSteps.class, (f, s) -> f
                .thenApply(s::gather)
                .thenApply(s::render)
                .thenAccept(s::distribute));
        client.register(report);

        // cron: 03:00 every day (server clock), optional payload for the started instances
        client.createCronSchedule("nightly-report", "0 3 * * *", Map.of("scope", "all-tenants"));

        // or a fixed interval
        client.createSchedule("cache-refresh", Duration.ofMinutes(15), null);
        // docs:end topology
    }

    private ScheduledSnippet() {}
}
