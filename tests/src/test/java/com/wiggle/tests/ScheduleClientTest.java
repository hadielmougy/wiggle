package com.wiggle.tests;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.dsl.Workflow;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.ScheduleInfo;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.client.worker.Worker;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Schedule management over the gRPC control plane: create (interval and cron), list, delete. */
class ScheduleClientTest {

    private static ServerConfig config() {
        return new ServerConfig(0, "schedc-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    @Test @DisplayName("a client creates, lists and deletes interval and cron schedules")
    void manageSchedules() throws Exception {
        Blueprint bpA = Workflow.define("schedc-probe-a")
                .step("work", ctx -> ctx).build();
        Blueprint bpB = Workflow.define("schedc-probe-b")
                .step("work", ctx -> ctx).build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "schedc-w").register(bpA).register(bpB)) {
            w.start();

            String hourly = client.createSchedule("schedc-probe-a", Duration.ofHours(1), Map.of("k", "v"));
            String nightly = client.createCronSchedule("schedc-probe-b", "0 3 * * *", null);

            var byId = new java.util.HashMap<String, ScheduleInfo>();
            for (ScheduleInfo s : client.schedules()) byId.put(s.id(), s);
            assertEquals(2, byId.size());

            ScheduleInfo si = byId.get(hourly);
            assertEquals("schedc-probe-a", si.workflow());
            assertEquals(Duration.ofHours(1).toMillis(), si.everyMillis());
            assertNull(si.cron());
            assertTrue(si.nextFireAt() > System.currentTimeMillis(), "armed one interval ahead");

            ScheduleInfo sc = byId.get(nightly);
            assertEquals("0 3 * * *", sc.cron());
            assertEquals(0, sc.everyMillis());
            assertTrue(sc.nextFireAt() > System.currentTimeMillis(), "armed at the next 3am UTC");

            client.deleteSchedule(hourly);
            client.deleteSchedule(nightly);
            assertTrue(client.schedules().isEmpty());

            WiggleApiException bad = assertThrows(WiggleApiException.class,
                    () -> client.createCronSchedule("schedc-probe-a", "61 * * * *", null));
            assertEquals(400, bad.status());
            assertThrows(WiggleApiException.class,
                    () -> client.createSchedule("no-such-workflow", Duration.ofHours(1), null),
                    "the workflow must be registered");
        }
    }

    @Test @DisplayName("re-creating a schedule for the same workflow updates it in place, no duplicate")
    void createIsIdempotentPerWorkflow() throws Exception {
        Blueprint bp = Workflow.define("schedc-probe-dup")
                .step("work", ctx -> ctx).build();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl());
             Worker w = new Worker(client, "schedc-w2").register(bp)) {
            w.start();

            // Simulates several app instances each trying to "ensure this schedule exists" on startup.
            String first = client.createSchedule("schedc-probe-dup", Duration.ofHours(1), Map.of("v", 1));
            String second = client.createSchedule("schedc-probe-dup", Duration.ofMinutes(30), Map.of("v", 2));
            String third = client.createCronSchedule("schedc-probe-dup", "0 3 * * *", Map.of("v", 3));

            assertEquals(first, second, "same workflow keeps the same schedule id");
            assertEquals(first, third, "switching cadence still keeps the same schedule id");
            assertEquals(1, client.schedules().size(), "no duplicate schedules were created");

            ScheduleInfo s = client.schedules().get(0);
            assertEquals("0 3 * * *", s.cron(), "the latest cadence wins");
            assertEquals(0, s.everyMillis());
        }
    }
}
