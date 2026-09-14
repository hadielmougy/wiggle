package com.wiggle.tests;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.ForFlow;
import com.wiggle.client.worker.Worker;
import com.wiggle.client.worker.WorkerOptions;
import com.wiggle.core.Ids;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Json;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Allocating a version to a worker: {@code handlers(obj, version)} binds one version's steps and
 * claims only that version's tasks.
 *
 * <p>The point is handing a capability from one service to another. Service A serves v1; v2 is
 * published and service B serves it; A drains its in-flight v1 instances and retires -- no shared
 * deploy, no cutover. Without the scoping A keeps claiming v2's tasks and running them with v1's
 * code, because the activity a handler binds is {@code workflow#step} and carries no version, so the
 * names match and nothing notices. That is the failure these tests pin.
 *
 * <p>Unversioned registration stays the default and serves every version, which is almost always
 * right: step names are stable across versions, so one implementation covers them all.
 */
class VersionScopedWorkerTest {

    /** The step this spec names; a worker binds it by name. */
    interface OneStep {
        Map<String, Object> handle(Map<String, Object> ctx);
        Map<String, Object> extra(Map<String, Object> ctx);
    }

    private static Map<String, Object> put(Map<String, Object> ctx, String k, Object v) {
        Map<String, Object> n = new LinkedHashMap<>(ctx);
        n.put(k, v);
        return n;
    }

    private static ServerConfig config() {
        return new ServerConfig(TestPorts.free(), "vs-node", null, null, null, 4,
                Duration.ofMillis(100), Duration.ofMillis(500), 3, Duration.ofSeconds(20),
                Duration.ofMillis(500), Duration.ofHours(1), 100, 0,
                Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    /** v1 and v2 of one workflow: same step name, different topology, so different content hashes. */
    private static FlowSpec v1() {
        return FlowSpec.define("vs-order", Map.class, OneStep.class, (f, s) -> f.thenApply(s::handle));
    }

    private static FlowSpec v2() {
        return FlowSpec.define("vs-order", Map.class, OneStep.class, (f, s) -> f
                .thenApply(s::handle)
                .thenApply(s::extra));
    }

    /** Service A's code. Tags the context with which service ran it. */
    @ForFlow("vs-order")
    public static final class ServiceA {
        final AtomicInteger runs;
        ServiceA(AtomicInteger runs) { this.runs = runs; }
        public Map<String, Object> handle(Map<String, Object> c) {
            runs.incrementAndGet();
            return put(c, "servedBy", "A");
        }
    }

    /** Service B's code -- the capability, moved. It also implements v2's new step. */
    @ForFlow("vs-order")
    public static final class ServiceB {
        final AtomicInteger runs;
        ServiceB(AtomicInteger runs) { this.runs = runs; }
        public Map<String, Object> handle(Map<String, Object> c) {
            runs.incrementAndGet();
            return put(c, "servedBy", "B");
        }
        public Map<String, Object> extra(Map<String, Object> c) {
            runs.incrementAndGet();
            return put(c, "extra", true);
        }
    }

    @Test
    @DisplayName("a version-scoped worker serves its own version and never claims the other's")
    void versionsAreAllocatedToWorkers() throws Exception {
        AtomicInteger aRuns = new AtomicInteger();
        AtomicInteger bRuns = new AtomicInteger();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            FlowSpec one = v1();
            FlowSpec two = v2();
            client.register(one);
            client.register(two);
            assertTrue(one.version() != two.version(), "the two topologies must differ");

            try (Worker a = new Worker(client, "service-a-" + Ids.next("x"),
                         WorkerOptions.defaults().withConcurrency(2))
                         .registerHandler(new ServiceA(aRuns), one.version());
                 Worker b = new Worker(client, "service-b-" + Ids.next("x"),
                         WorkerOptions.defaults().withConcurrency(2))
                         .registerHandler(new ServiceB(bRuns), two.version())) {
                a.start();
                b.start();

                // an in-flight v1 instance drains on the old service
                InstanceView v1Run = client.awaitCompletion(
                        client.start(one, Map.of()), Duration.ofSeconds(20));
                assertEquals("COMPLETED", v1Run.status());
                assertEquals("A", Json.asObject(v1Run.context()).get("servedBy"),
                        "v1 must be served by the service allocated v1");

                // and new work runs on the new one
                InstanceView v2Run = client.awaitCompletion(
                        client.start(two, Map.of()), Duration.ofSeconds(20));
                assertEquals("COMPLETED", v2Run.status());
                Map<String, Object> ctx = Json.asObject(v2Run.context());
                assertEquals("B", ctx.get("servedBy"), "v2 must be served by the service allocated v2");
                assertEquals(true, ctx.get("extra"), "including v2's new step");

                assertEquals(1, aRuns.get(), "A ran only its own version's task");
                assertEquals(2, bRuns.get(), "B ran v2's two steps and nothing of v1's");
            }
        }
    }

    @Test
    @DisplayName("an unversioned worker still serves every version -- the default is unchanged")
    void unversionedWorkerServesEveryVersion() throws Exception {
        AtomicInteger runs = new AtomicInteger();

        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            FlowSpec one = v1();
            FlowSpec two = v2();
            client.register(one);
            client.register(two);

            try (Worker w = new Worker(client, "any-" + Ids.next("x"),
                    WorkerOptions.defaults().withConcurrency(2))
                    .registerHandler(new ServiceB(runs))) {       // no version: serves both
                w.start();

                assertEquals("COMPLETED",
                        client.awaitCompletion(client.start(one, Map.of()), Duration.ofSeconds(20)).status());
                assertEquals("COMPLETED",
                        client.awaitCompletion(client.start(two, Map.of()), Duration.ofSeconds(20)).status());
                assertEquals(3, runs.get(), "one v1 step plus v2's two");
            }
        }
    }

    @Test
    @DisplayName("binding a version that is not registered fails at startup, not at the first task")
    void bindingAnUnknownVersionFailsFast() throws Exception {
        try (WiggleServer server = new WiggleServer(config()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            FlowSpec one = v1();
            client.register(one);

            try (Worker w = new Worker(client, "wrong-" + Ids.next("x"))
                    .registerHandler(new ServiceA(new AtomicInteger()), one.version() + 1)) {
                // the whole value of naming a version: the mismatch surfaces here, where a deploy can
                // fail, rather than as a decode error per task later
                assertThrows(RuntimeException.class, w::start);
            }
        }
    }
}