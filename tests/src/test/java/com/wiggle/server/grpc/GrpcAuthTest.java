package com.wiggle.server.grpc;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleClient.WiggleApiException;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.Tls;
import com.wiggle.proto.WiggleControlPlaneGrpc;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import com.wiggle.server.auth.Accounts;
import com.wiggle.tests.TestPorts;
import io.grpc.CallOptions;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;
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
class GrpcAuthTest {

    interface Steps { Map<String, Object> work(Map<String, Object> ctx); }

    @ForFlow("orders")
    public static final class OrderSteps {
        public Map<String, Object> work(Map<String, Object> ctx) { return ctx; }
    }

    private static final FlowSpec ORDERS = FlowSpec.define("orders", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));
    private static final FlowSpec BILLING = FlowSpec.define("billing", 1, Map.class, Steps.class, (f, s) -> f.thenApply(s::work));

    private static ServerConfig config(ServerConfig.GrpcAuth mode) {
        return new ServerConfig(TestPorts.free(), "authz-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10))
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
        try (WiggleServer server = new WiggleServer(config(ServerConfig.GrpcAuth.ENFORCE)).start();
             WiggleClient anonymous = client(server, null);
             WiggleClient forged = client(server, Accounts.KEY_PREFIX + "not-a-key")) {
            assertEquals(401, status(anonymous::workflowNames));
            assertEquals(401, status(forged::workflowNames));
            ManagedChannel channel = Grpc.newChannelBuilder(server.baseUrl(), InsecureChannelCredentials.create()).build();
            try {
                assertEquals("UP", WiggleControlPlaneGrpc.newBlockingStub(channel)
                        .healthCheck(com.wiggle.proto.Empty.getDefaultInstance()).getStatus(),
                        "the health check stays open for probes");
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test @DisplayName("a key's role decides what it may do, scoped to the workflows and queues it names")
    void scopedRoles() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.GrpcAuth.ENFORCE)).start()) {
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

                admin.createTrigger("orders", "billing", List.of("wf.completed"), false);
                admin.createTrigger("billing", "orders", List.of("wf.completed"), false);
                assertEquals(List.of("orders"), app.triggers().stream().map(WiggleClient.TriggerInfo::workflow).toList(),
                        "only the triggers of workflows it may read");
            }
        }
    }

    @Test @DisplayName("a worker with a key runs its steps, and stops being let in once the key is deleted")
    void workerAndRevocation() throws Exception {
        try (WiggleServer server = new WiggleServer(config(ServerConfig.GrpcAuth.ENFORCE)).start()) {
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
        try (WiggleServer server = new WiggleServer(config(ServerConfig.GrpcAuth.ENFORCE)).start()) {
            String key = server.accounts().createApiKey("ops", "old", "admin", System.currentTimeMillis() - 1);
            try (WiggleClient c = client(server, key)) {
                assertEquals(401, status(c::workflowNames));
            }
        }
    }

    @Test @DisplayName("log mode serves every call, and off mode reads no credential at all")
    void logAndOff() throws Exception {
        for (ServerConfig.GrpcAuth mode : List.of(ServerConfig.GrpcAuth.LOG, ServerConfig.GrpcAuth.OFF)) {
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
        try (WiggleServer server = new WiggleServer(config(ServerConfig.GrpcAuth.ENFORCE)).start()) {
            server.accounts().putRole("ops", "nothing", List.of("user.manage"), true);   // no RPC needs it
            String key = server.accounts().createApiKey("ops", "nobody", "nothing", null);
            ManagedChannel channel = Grpc.newChannelBuilder(server.baseUrl(), InsecureChannelCredentials.create()).build();
            try {
                Metadata headers = new Metadata();
                headers.put(Authorizer.AUTHORIZATION, "Bearer " + key);
                io.grpc.Channel authed = io.grpc.ClientInterceptors.intercept(channel,
                        MetadataUtils.newAttachHeadersInterceptor(headers));
                List<String> unchecked = new ArrayList<>();
                for (io.grpc.MethodDescriptor<?, ?> m : WiggleControlPlaneGrpc.getServiceDescriptor().getMethods()) {
                    if (m.getBareMethodName().equals("HealthCheck")) continue;
                    Status.Code code = call(authed, m);
                    if (code != Status.Code.PERMISSION_DENIED) unchecked.add(m.getBareMethodName() + " -> " + code);
                }
                assertEquals(List.of(), unchecked);
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <Q, R> Status.Code call(io.grpc.Channel channel, MethodDescriptor<?, ?> m) {
        MethodDescriptor<Q, R> method = (MethodDescriptor<Q, R>) m;
        Q request = (Q) ((MethodDescriptor.PrototypeMarshaller<?>) method.getRequestMarshaller()).getMessagePrototype();
        try {
            ClientCalls.blockingUnaryCall(channel, method, CallOptions.DEFAULT.withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS), request);
            return Status.Code.OK;
        } catch (StatusRuntimeException e) {
            return e.getStatus().getCode();
        }
    }
}
