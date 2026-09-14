package com.wiggle.cookbook;

import com.wiggle.client.WiggleClient;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Runs every {@link Cookbook} recipe to completion in one embedded JVM, printing the resulting
 * context. The mirror of {@link CookbookDemo} — same operators, same outcomes, defined through
 * {@link com.wiggle.client.flow.Wiggle#define} instead of {@code graph}.
 *
 * <pre>./gradlew :example:runCookbook</pre>
 *
 * <p>Each recipe is one object here, registered and bound in the same breath: {@code register(r.spec())}
 * takes the topology it defined, {@code handlers(r)} takes the very same instance as its handlers.
 * That is the whole point of the typed mode — the two halves cannot drift, because they are one class.
 *
 * <p>Two recipes ({@code tcb-approval-escalation}, {@code tcb-kitchen-sink}) wait on a signal this
 * demo deliberately never sends, so you can watch the escalation branch fire instead.
 */
public final class CookbookDemo {

    public static void main(String[] args) throws Exception {
        Cookbook.LinearWithGate linearGate = new Cookbook.LinearWithGate();
        Cookbook.ChooseThenFork chooseFork = new Cookbook.ChooseThenFork();
        Cookbook.ForEachAcrossQueues forEachQueues = new Cookbook.ForEachAcrossQueues();
        Cookbook.PollUntilReady pollLoop = new Cookbook.PollUntilReady();
        Cookbook.ApprovalWithEscalation approval = new Cookbook.ApprovalWithEscalation();
        Cookbook.ChildCheckThenFork parentChild = new Cookbook.ChildCheckThenFork();
        Cookbook.BatchedLoopWithCheckpoint batchedLoop = new Cookbook.BatchedLoopWithCheckpoint();
        Cookbook.KitchenSink kitchenSink = new Cookbook.KitchenSink();

        // tcb-linear-gate is also the child workflow of tcb-parent, so it must be registered before
        // that instance starts.
        FlowSpec linearGateSpec = linearGate.spec();

        try (WiggleServer server = new WiggleServer(ServerConfig.fromEnvironment()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            // The author publishes every topology; each recipe object then only implements its steps.
            for (FlowSpec spec : List.of(linearGateSpec, chooseFork.spec(), forEachQueues.spec(),
                    pollLoop.spec(), approval.spec(), parentChild.spec(), batchedLoop.spec(),
                    kitchenSink.spec())) {
                client.register(spec);
            }

            try (Worker worker = new Worker(client, "typed-cookbook-worker")
                    .registerHandler(linearGate)
                    .registerHandler(chooseFork)
                    .registerHandler(forEachQueues)
                    .registerHandler(pollLoop)
                    .registerHandler(approval)
                    .registerHandler(parentChild)
                    .registerHandler(batchedLoop)
                    .registerHandler(kitchenSink)) {
                worker.start();

                run(client, "1. step + effect + gate (and a context type change)", linearGate.spec(),
                        Map.of("email", "  HADI@Wiggle.dev  "));

                run(client, "2. oneOf + allOf + retry", chooseFork.spec(),
                        Map.of("amount", 5000));

                run(client, "3. forEach + per-step queue", forEachQueues.spec(),
                        Map.of("items", List.of(Map.of("sku", "A"), Map.of("sku", "BB"),
                                Map.of("sku", "CCC"))));

                run(client, "4. repeatWhile + gate", pollLoop.spec(), Map.of("cancelled", false));

                run(client, "5. thenAwait(escalation) + oneOf (nobody signals -> escalates)",
                        approval.spec(), Map.of());

                run(client, "6. thenSubFlow + gate + allOf", parentChild.spec(),
                        Map.of("email", "hadi@wiggle.dev"));

                run(client, "7. execution(LOCAL_ASYNC) + checkpoint + repeatWhile",
                        batchedLoop.spec(), Map.of());

                run(client, "8. kitchen sink -- almost every operator in one graph",
                        kitchenSink.spec(),
                        Map.of("items", List.of(Map.of("sku", "A"), Map.of("sku", "B"),
                                Map.of("sku", "C"))));
            }
        }
    }

    private static void run(WiggleClient client, String label, FlowSpec spec,
                            Map<String, Object> context) throws Exception {
        System.out.println("\n--- " + label + " ---");
        String id = client.start(spec, context);
        InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));
        System.out.println("   status:  " + v.status()
                + (v.terminationReason() == null ? "" : " (" + v.terminationReason() + ")"));
        if (v.error() != null) System.out.println("   error:   " + v.error());
        System.out.println("   context: " + v.context());
    }
}