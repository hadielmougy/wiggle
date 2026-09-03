package com.wiggle.cookbook;

import com.wiggle.client.dsl.Blueprint;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.worker.Worker;
import com.wiggle.core.InstanceView;
import com.wiggle.server.ServerConfig;
import com.wiggle.server.WiggleServer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Runs every {@link Cookbook} blueprint to completion in one embedded JVM, printing the
 * resulting context so you can see each operator combination's actual effect. Two of the
 * blueprints ({@code cb-approval-escalation}, {@code cb-kitchen-sink}) wait on a signal that
 * this demo deliberately never sends, so you can watch the escalation branch fire instead.
 *
 * <pre>./gradlew :example:runCookbook</pre>
 */
public final class CookbookDemo {

    public static void main(String[] args) throws Exception {
        try (WiggleServer server = new WiggleServer(ServerConfig.fromEnvironment()).start();
             WiggleClient client = new WiggleClient(server.baseUrl())) {

            // cb-linear-gate is also used as a child workflow by cb-parent and cb-kitchen-sink,
            // so it must be registered before either of those instances starts.
            Blueprint linearGate = Cookbook.linearWithGate();
            Blueprint chooseFork = Cookbook.chooseThenFork();
            Blueprint forkEachQueues = Cookbook.forkEachAcrossQueues();
            Blueprint pollLoop = Cookbook.pollUntilReady();
            Blueprint approval = Cookbook.approvalWithEscalation();
            Blueprint parentChild = Cookbook.childCheckThenFork();
            Blueprint batchedLoop = Cookbook.batchedLoopWithCheckpoint();
            Blueprint kitchenSink = Cookbook.kitchenSink();

            try (Worker worker = new Worker(client, "cookbook-worker")
                    .register(linearGate).register(chooseFork).register(forkEachQueues)
                    .register(pollLoop).register(approval).register(parentChild)
                    .register(batchedLoop).register(kitchenSink)
                    .handlers(new CookbookHandlers.LinearGate())
                    .handlers(new CookbookHandlers.ChooseFork())
                    .handlers(new CookbookHandlers.ForeachQueues())
                    .handlers(new CookbookHandlers.PollUntilReady())
                    .handlers(new CookbookHandlers.ApprovalEscalation())
                    .handlers(new CookbookHandlers.Parent())
                    .handlers(new CookbookHandlers.BatchedLoop())
                    .handlers(new CookbookHandlers.KitchenSink())) {
                worker.start();

                run(client, "1. step + then + effect + gate", linearGate,
                        Map.of("email", "HADI@Wiggle.dev"));

                run(client, "2. choose + fork + retry", chooseFork,
                        Map.of("amount", 5000));

                run(client, "3. forkEach + per-step queue", forkEachQueues,
                        Map.of("items", List.of(Map.of("sku", "A"), Map.of("sku", "B"), Map.of("sku", "C"))));

                run(client, "4. doWhile + gate", pollLoop, Map.of("cancelled", false));

                run(client, "5. awaitSignal(escalation) + choose (nobody signals -> escalates)",
                        approval, Map.of());

                run(client, "6. subWorkflow + gate + fork", parentChild,
                        Map.of("email", "hadi@wiggle.dev"));

                run(client, "7. execution(LOCAL_ASYNC) + checkpoint + doWhile", batchedLoop, Map.of());

                run(client, "8. kitchen sink -- almost every operator in one graph", kitchenSink,
                        Map.of("email", "hadi@wiggle.dev",
                                "items", List.of(Map.of("sku", "A"), Map.of("sku", "B"))));
            }
        }
    }

    private static void run(WiggleClient client, String label, Blueprint bp,
                             Map<String, Object> context) throws Exception {
        System.out.println("\n--- " + label + " ---");
        String id = client.start(bp, context);
        InstanceView v = client.awaitCompletion(id, Duration.ofSeconds(30));
        System.out.println("   status:  " + v.status()
                + (v.terminationReason() == null ? "" : " (" + v.terminationReason() + ")"));
        if (v.error() != null) System.out.println("   error:   " + v.error());
        System.out.println("   context: " + v.context());
    }
}
