package com.wiggle.server.api;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.Tls;
import com.wiggle.core.wire.ControlPlane;
import com.wiggle.core.wire.Msg;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.auth.Accounts;
import com.wiggle.testfixtures.TestPorts;
import com.wiggle.tests.RawApi;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-RPC authorization: who a call is (an API key), and what its role lets it do, with scopes,
 * revocation and expiry, and every RPC covered.
 */
class ApiAuthTest {

    interface Steps { Map<String, Object> work(Map<String, Object> ctx); }

    interface SpawnSteps {
        Map<String, Object> work(Map<String, Object> ctx);
        Map<String, Object> merge(java.util.List<Map<String, Object>> results);
    }

    @ForFlow("orders")
    public static final class OrderSteps {
        public Map<String, Object> work(Map<String, Object> ctx) { return ctx; }
    }

    private static final FlowSpec ORDERS = FlowSpec.define("orders", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    private static final FlowSpec BILLING = FlowSpec.define("billing", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));

    private static ServerConfig config(ServerConfig.ApiAuth mode) {
        return new ServerConfig(TestPorts.free(), "authz-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0)
                .withAuth(new ServerConfig.Auth(mode, Duration.ofSeconds(30)));
    }

    private static WiggleClient client(WiggleServer server, String key) {
        return new WiggleClient(server.baseUrl(), Tls.Options.DISABLED, false, key);
    }

    private static int status(Runnable call) {
        return assertThrows(WiggleApiException.class, call::run).status();
    }

    private static void awaitRefused(Runnable call, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (true) {
            try {
                call.run();
            } catch (WiggleApiException e) {
                if (e.status() == expected) return;
                throw e;
            }
            if (System.currentTimeMillis() > deadline) throw new AssertionError("still allowed after 5 s");
            Thread.sleep(100);
        }
    }

    @Test @DisplayName("enforce: no key or an unknown key is UNAUTHENTICATED; the health check needs none")
    void unauthenticated() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.ApiAuth.ENFORCE)).start();
             WiggleClient anonymous = client(server, null);
             WiggleClient forged = client(server, Accounts.KEY_PREFIX + "not-a-key")) {
            assertEquals(401, status(anonymous::workflowNames));
            assertEquals(401, status(forged::workflowNames));
            RawApi.Answer health = RawApi.call(server.baseUrl(), ControlPlane.HEALTH_CHECK,
                    Msg.create(), null);
            assertEquals("UP", health.msg().str("status"),
                    "the health check stays open for probes");
        }
    }

    @Test @DisplayName("a key's role decides what it may do, scoped to the workflows and queues it names")
    void scopedRoles() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.ApiAuth.ENFORCE)).start()) {
            Accounts accounts = server.accounts();
            accounts.putRole("ops", "orders-app", List.of("workflow.register:orders", "instance.start:orders",
                    "read:orders", "instance.cancel:orders"), true);
            accounts.putRole("ops", "orders-worker", List.of("task.poll:orders-q", "read:orders"), true);
            String appKey = accounts.createApiKey("ops", "orders-app", "orders-app", null);
            String workerKey = accounts.createApiKey("ops", "orders-worker", "orders-worker", null);
            String adminKey = accounts.createApiKey("ops", "root", "admin", null);

            try (WiggleClient app = client(server, appKey); WiggleClient worker = client(server, workerKey);
                 WiggleClient admin = client(server, adminKey)) {
                app.register(ORDERS);
                assertEquals(403, status(() -> app.register(BILLING)));
                admin.register(BILLING);

                String order = app.start(ORDERS, Map.of());
                assertEquals(403, status(() -> app.start(BILLING, Map.of())));
                String bill = admin.start(BILLING, Map.of());

                assertEquals("orders", app.instance(order).workflow());
                assertEquals(403, status(() -> app.instance(bill)), "another workflow's instance");
                assertEquals(1, app.listInstances("orders", null, 10).size());
                assertEquals(403, status(() -> app.listInstances(null, null, 10)), "a list across workflows");
                assertEquals(403, status(app::cluster));
                assertEquals(403, status(() -> app.cancel(bill, "no")));
                app.cancel(order, "yes");
                assertEquals(403, status(() -> worker.cancel(order, "no")), "no cancel permission at all");

                assertTrue(worker.poll("w", List.of("orders-q"), 1, 1_000, 0).tasks().isEmpty());
                assertEquals(403, status(() -> worker.poll("w", List.of("billing-q"), 1, 1_000, 0)));
                assertEquals(403, status(() -> worker.poll("w", List.of(), 1, 1_000, 0)), "every queue is not one queue");
                assertEquals(403, status(() -> worker.pollEvents("feed", 10, 0, -1)));
            }
        }
    }

    private static final FlowSpec ACME = FlowSpec.define("acme.orders", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    private static final FlowSpec GLOBEX = FlowSpec.define("globex.orders", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));

    private static FlowSpec parentOf(String name, String child) {
        return FlowSpec.define(name, 1, Map.class, Steps.class,
                (f, s) -> f.thenSubFlow("child", child, Map.class).thenApply(s::work));
    }

    private static WiggleClient.TokenInfo token(WiggleClient c, String instanceId, String taskId) {
        return c.instanceDetail(instanceId).tokens().stream().filter(t -> t.id().equals(taskId)).findFirst().orElseThrow();
    }

    /** A tenant's key, everything scoped to its prefix as the portal grants it. */
    private static String tenantKey(Accounts accounts, String slug) {
        String p = slug + ".*";
        accounts.putRole("ops", "wc-" + slug, List.of("read:" + p, "task.poll:" + p, "workflow.register:" + p,
                "instance.start:" + p, "schedule.write:" + p), true);
        return accounts.createApiKey("ops", slug, "wc-" + slug, null);
    }

    @Test @DisplayName("two tenants on prefix scopes: listings are filtered, and neither can touch the other's work")
    void prefixTenants() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.ApiAuth.ENFORCE)).start()) {
            Accounts accounts = server.accounts();
            String adminKey = accounts.createApiKey("ops", "root", "admin", null);
            try (WiggleClient admin = client(server, adminKey);
                 WiggleClient acme = client(server, tenantKey(accounts, "acme"));
                 WiggleClient globex = client(server, tenantKey(accounts, "globex"))) {
                acme.register(ACME);
                globex.register(GLOBEX);
                assertEquals(403, status(() -> acme.register(GLOBEX)));
                String mine = acme.start("acme.orders", Map.of(), null, "shared");
                String theirs = globex.start("globex.orders", Map.of(), null, "shared");
                acme.createSchedule("acme.orders", Duration.ofHours(1), Map.of());
                globex.createSchedule("globex.orders", Duration.ofHours(1), Map.of());

                assertEquals(List.of("acme.orders"), acme.workflowNames());
                assertEquals(List.of("acme.orders"), acme.schedules().stream().map(WiggleClient.ScheduleInfo::workflow).toList());
                assertEquals(List.of(mine), acme.findByCorrelation("shared").stream()
                        .map(com.wiggle.core.InstanceView::id).toList(), "a correlation id two tenants share");
                assertEquals(List.of("acme.orders"), acme.backlogCoverage(100).stream()
                        .map(WiggleClient.BacklogSlice::workflow).distinct().toList());
                assertEquals(2, admin.findByCorrelation("shared").size(), "unscoped read sees both");

                assertEquals(403, status(() -> acme.register(parentOf("acme.parent", "globex.orders"))),
                        "a child workflow the key may not start");
                assertFalse(admin.workflowNames().contains("acme.parent"), "a refused definition is not stored");
                acme.register(parentOf("acme.parent2", "acme.orders"));
                assertEquals(403, status(() -> acme.register(FlowSpec.define("acme.hijack", 1, Map.class, Steps.class,
                        (f, s) -> f.apply(s::work, "globex.orders")))), "a step on another tenant's queue");
                assertFalse(admin.workflowNames().contains("acme.hijack"));
                acme.register(FlowSpec.define("acme.gpu", 1, Map.class, Steps.class, (f, s) -> f.apply(s::work, "acme.gpu-q")));

                com.wiggle.core.TaskActivation task = globex.poll("g1", List.of("globex.orders"), 1, 60_000, 5_000)
                        .tasks().getFirst();
                assertEquals(theirs, task.instanceId());
                String owner = task.leaseOwner();
                WiggleClient.TokenInfo before = token(admin, theirs, task.taskId());
                assertEquals(404, status(() -> acme.heartbeat(task.taskId(), owner, 60_000)));
                assertEquals(404, status(() -> acme.fail(task.taskId(), owner, "no", false)));
                WiggleClient.RunOutcome reported = acme.reportSteps(List.of(new WiggleClient.RunSubmission(task.taskId(),
                        owner, List.of(new WiggleClient.StepReport(task.nodeId(), Map.of("x", 1), null)), false)))
                        .get(task.taskId());
                assertEquals(404, reported.errorStatus());
                assertEquals("RUNNING", admin.instance(theirs).status(), "the other tenant's instance is untouched");
                WiggleClient.TokenInfo after = token(admin, theirs, task.taskId());
                assertEquals(List.of(before.status(), before.attempt(), before.leaseOwner()),
                        List.of(after.status(), after.attempt(), after.leaseOwner()), "and so is its task");
                globex.heartbeat(task.taskId(), owner, 60_000);
            }
        }
    }

    @Test @DisplayName("a branch's sub-flow needs the start permission on its workflow, as a direct start does")
    void createdSubFlowsAreScoped() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.ApiAuth.ENFORCE)).start()) {
            Accounts accounts = server.accounts();
            try (WiggleClient acme = client(server, tenantKey(accounts, "acme"));
                 WiggleClient globex = client(server, tenantKey(accounts, "globex"))) {
                FlowSpec spawning = FlowSpec.define("acme.spawn", 1, Map.class, SpawnSteps.class,
                        (f, s) -> f.thenApply(s::work).combine(s::merge));
                acme.register(spawning);
                acme.register(ACME);
                globex.register(GLOBEX);
                String id = acme.start("acme.spawn", Map.of(), null, null);
                com.wiggle.core.TaskActivation task = acme.poll("a1", List.of("acme.spawn"), 1, 60_000, 5_000)
                        .tasks().getFirst();
                java.util.function.Function<String, List<WiggleClient.RunSubmission>> creating = child -> List.of(
                        new WiggleClient.RunSubmission(task.taskId(), task.leaseOwner(), List.of(
                                new WiggleClient.StepReport(task.nodeId(), Map.of(), null, null, null, List.of(),
                                        List.of(new com.wiggle.core.CreatedBranch(Map.of(), null, List.of(
                                                com.wiggle.core.CreatedBranch.BranchStep.subFlow("child", child)))))),
                                true));

                assertEquals(403, status(() -> acme.reportSteps(creating.apply("globex.orders"))),
                        "another tenant's workflow, started through a created branch");
                assertEquals("RUNNING", acme.instance(id).status());
                WiggleClient.RunOutcome ok = acme.reportSteps(creating.apply("acme.orders")).get(task.taskId());
                assertTrue(ok.ok(), String.valueOf(ok.error()));
            }
        }
    }

    @Test @DisplayName("a worker with a key runs its steps, and stops being let in once the key is deleted")
    void workerAndRevocation() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.ApiAuth.ENFORCE)).start()) {
            Accounts accounts = server.accounts();
            accounts.putRole("ops", "worker", List.of("task.poll", "read:orders"), true);
            String adminKey = accounts.createApiKey("ops", "root", "admin", null);
            String workerKey = accounts.createApiKey("ops", "w1", "worker", null);
            try (WiggleClient admin = client(server, adminKey); WiggleClient wc = client(server, workerKey)) {
                admin.register(ORDERS);
                Worker w = new Worker(wc, "w1").registerHandler(new OrderSteps()).start();
                String id = admin.start(ORDERS, Map.of());
                assertEquals("COMPLETED", admin.awaitCompletion(id, Duration.ofSeconds(20)).status());

                w.close();
                accounts.deleteCredential("ops", "w1");
                awaitRefused(() -> wc.poll("w", List.of(), 1, 1_000, 0), 401);
            }
        }
    }

    @Test @DisplayName("an expired key is refused")
    void expiry() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.ApiAuth.ENFORCE)).start()) {
            String key = server.accounts().createApiKey("ops", "old", "admin", System.currentTimeMillis() - 1);
            try (WiggleClient c = client(server, key)) {
                assertEquals(401, status(c::workflowNames));
            }
        }
    }

    @Test @DisplayName("log mode serves every call, and off mode reads no credential at all")
    void logAndOff() throws Exception {
        for (ServerConfig.ApiAuth mode : List.of(ServerConfig.ApiAuth.LOG, ServerConfig.ApiAuth.OFF)) {
            try (WiggleServer server = new WiggleServer(config(mode)).start();
                 WiggleClient anonymous = client(server, null)) {
                anonymous.register(ORDERS);
                assertTrue(anonymous.workflowNames().contains("orders"), mode.name());
                assertFalse(anonymous.start(ORDERS, Map.of()).isEmpty(), mode.name());
            }
        }
    }

    /**
     * Every RPC but the health check refuses a caller whose role grants nothing it could use, before
     * it reads anything: a new RPC added without a check fails here.
     */
    @Test @DisplayName("every RPC except the health check is refused to a caller without its permission")
    void everyRpcIsChecked() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.ApiAuth.ENFORCE)).start()) {
            server.accounts().putRole("ops", "nothing", List.of("user.manage"), true);   // no RPC needs it
            String key = server.accounts().createApiKey("ops", "nobody", "nothing", null);
            List<String> unchecked = new ArrayList<>();
            for (ControlPlane.Method m : ControlPlane.METHODS) {
                if (m == ControlPlane.HEALTH_CHECK) continue;
                RawApi.Answer answer = RawApi.call(server.baseUrl(), m, Msg.create(), key);
                if (answer.status() != 403) unchecked.add(m.name() + " -> " + answer.status() + " " + answer.code());
            }
            assertEquals(List.of(), unchecked);
        }
    }

    @Test @DisplayName("the mode's old name is refused, so renaming it cannot leave authorization off")
    void oldModeNameIsRefused() {
        System.setProperty("wiggle.grpc.auth", "enforce");
        try {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, ServerConfig.Auth::fromEnvironment);
            assertTrue(e.getMessage().contains("WIGGLE_API_AUTH"), e.getMessage());
        } finally {
            System.clearProperty("wiggle.grpc.auth");
        }
        System.setProperty("wiggle.api.auth", "enforce");
        try {
            assertEquals(ServerConfig.ApiAuth.ENFORCE, ServerConfig.Auth.fromEnvironment().api());
        } finally {
            System.clearProperty("wiggle.api.auth");
        }
    }
}
